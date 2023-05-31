/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/core/reactor.hh>

#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "db/virtual_table.hh"
#include "db/virtual_tables.hh"
#include "db/size_estimates_virtual_reader.hh"
#include "db/view/build_progress_virtual_reader.hh"
#include "index/built_indexes_virtual_reader.hh"
#include "gms/gossiper.hh"
#include "partition_slice_builder.hh"
#include "protocol_server.hh"
#include "release.hh"
#include "replica/database.hh"
#include "schema/schema_builder.hh"
#include "schema/schema_registry.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/storage_service.hh"
#include "types/list.hh"
#include "types/types.hh"
#include "utils/build_id.hh"
#include "log.hh"

namespace db {

namespace {

logging::logger vtlog("virtual_tables");

class cluster_status_table : public memtable_filling_virtual_table {
private:
    service::storage_service& _ss;
    gms::gossiper& _gossiper;
public:
    cluster_status_table(service::storage_service& ss, gms::gossiper& g)
            : memtable_filling_virtual_table(build_schema())
            , _ss(ss), _gossiper(g) {}

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "cluster_status");
        return schema_builder(system_keyspace::NAME, "cluster_status", std::make_optional(id))
            .with_column("peer", inet_addr_type, column_kind::partition_key)
            .with_column("dc", utf8_type)
            .with_column("up", boolean_type)
            .with_column("status", utf8_type)
            .with_column("load", utf8_type)
            .with_column("tokens", int32_type)
            .with_column("owns", float_type)
            .with_column("host_id", uuid_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    future<> execute(std::function<void(mutation)> mutation_sink) override {
        return _ss.get_ownership().then([&, mutation_sink] (std::map<gms::inet_address, float> ownership) {
            const locator::token_metadata& tm = _ss.get_token_metadata();

            for (auto&& e : _gossiper.get_endpoint_states()) {
                auto endpoint = e.first;

                mutation m(schema(), partition_key::from_single_value(*schema(), data_value(endpoint).serialize_nonnull()));
                row& cr = m.partition().clustered_row(*schema(), clustering_key::make_empty()).cells();

                set_cell(cr, "up", _gossiper.is_alive(endpoint));
                set_cell(cr, "status", _gossiper.get_gossip_status(endpoint));
                set_cell(cr, "load", _gossiper.get_application_state_value(endpoint, gms::application_state::LOAD));

                auto hostid = tm.get_host_id_if_known(endpoint);
                if (hostid) {
                    set_cell(cr, "host_id", hostid->uuid());
                }

                if (tm.is_normal_token_owner(endpoint)) {
                    sstring dc = tm.get_topology().get_location(endpoint).dc;
                    set_cell(cr, "dc", dc);
                }

                if (ownership.contains(endpoint)) {
                    set_cell(cr, "owns", ownership[endpoint]);
                }

                set_cell(cr, "tokens", int32_t(tm.get_tokens(endpoint).size()));

                mutation_sink(std::move(m));
            }
        });
    }
};

class token_ring_table : public streaming_virtual_table {
private:
    replica::database& _db;
    service::storage_service& _ss;
public:
    token_ring_table(replica::database& db, service::storage_service& ss)
            : streaming_virtual_table(build_schema())
            , _db(db)
            , _ss(ss)
    {
        _shard_aware = true;
    }

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "token_ring");
        return schema_builder(system_keyspace::NAME, "token_ring", std::make_optional(id))
            .with_column("keyspace_name", utf8_type, column_kind::partition_key)
            .with_column("start_token", utf8_type, column_kind::clustering_key)
            .with_column("endpoint", inet_addr_type, column_kind::clustering_key)
            .with_column("end_token", utf8_type)
            .with_column("dc", utf8_type)
            .with_column("rack", utf8_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    dht::decorated_key make_partition_key(const sstring& name) {
        return dht::decorate_key(*_s, partition_key::from_single_value(*_s, data_value(name).serialize_nonnull()));
    }

    clustering_key make_clustering_key(sstring start_token, gms::inet_address host) {
        return clustering_key::from_exploded(*_s, {
            data_value(start_token).serialize_nonnull(),
            data_value(host).serialize_nonnull()
        });
    }

    struct endpoint_details_cmp {
        bool operator()(const dht::endpoint_details& l, const dht::endpoint_details& r) const {
            return inet_addr_type->less(
                data_value(l._host).serialize_nonnull(),
                data_value(r._host).serialize_nonnull());
        }
    };

    future<> execute(reader_permit permit, result_collector& result, const query_restrictions& qr) override {
        struct decorated_keyspace_name {
            sstring name;
            dht::decorated_key key;
        };

        auto keyspace_names = boost::copy_range<std::vector<decorated_keyspace_name>>(
                _db.get_keyspaces() | boost::adaptors::transformed([this] (auto&& e) {
                    return decorated_keyspace_name{e.first, make_partition_key(e.first)};
        }));

        boost::sort(keyspace_names, [less = dht::ring_position_less_comparator(*_s)]
                (const decorated_keyspace_name& l, const decorated_keyspace_name& r) {
            return less(l.key, r.key);
        });

        for (const decorated_keyspace_name& e : keyspace_names) {
            auto&& dk = e.key;
            if (!this_shard_owns(dk) || !contains_key(qr.partition_range(), dk) || !_db.has_keyspace(e.name)) {
                continue;
            }

            std::vector<dht::token_range_endpoints> ranges = co_await _ss.describe_ring(e.name);

            co_await result.emit_partition_start(dk);
            boost::sort(ranges, [] (const dht::token_range_endpoints& l, const dht::token_range_endpoints& r) {
                return l._start_token < r._start_token;
            });

            for (dht::token_range_endpoints& range : ranges) {
                boost::sort(range._endpoint_details, endpoint_details_cmp());

                for (const dht::endpoint_details& detail : range._endpoint_details) {
                    clustering_row cr(make_clustering_key(range._start_token, detail._host));
                    set_cell(cr.cells(), "end_token", sstring(range._end_token));
                    set_cell(cr.cells(), "dc", sstring(detail._datacenter));
                    set_cell(cr.cells(), "rack", sstring(detail._rack));
                    co_await result.emit_row(std::move(cr));
                }
            }

            co_await result.emit_partition_end();
        }
    }
};

class snapshots_table : public streaming_virtual_table {
    distributed<replica::database>& _db;
public:
    explicit snapshots_table(distributed<replica::database>& db)
            : streaming_virtual_table(build_schema())
            , _db(db)
    {
        _shard_aware = true;
    }

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "snapshots");
        return schema_builder(system_keyspace::NAME, "snapshots", std::make_optional(id))
            .with_column("keyspace_name", utf8_type, column_kind::partition_key)
            .with_column("table_name", utf8_type, column_kind::clustering_key)
            .with_column("snapshot_name", utf8_type, column_kind::clustering_key)
            .with_column("live", long_type)
            .with_column("total", long_type)
            .set_comment("Lists all the snapshots along with their size, dropped tables are not part of the listing.")
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    dht::decorated_key make_partition_key(const sstring& name) {
        return dht::decorate_key(*_s, partition_key::from_single_value(*_s, data_value(name).serialize_nonnull()));
    }

    clustering_key make_clustering_key(sstring table_name, sstring snapshot_name) {
        return clustering_key::from_exploded(*_s, {
            data_value(std::move(table_name)).serialize_nonnull(),
            data_value(std::move(snapshot_name)).serialize_nonnull()
        });
    }

    future<> execute(reader_permit permit, result_collector& result, const query_restrictions& qr) override {
        struct decorated_keyspace_name {
            sstring name;
            dht::decorated_key key;
        };
        std::vector<decorated_keyspace_name> keyspace_names;

        for (const auto& [name, _] : _db.local().get_keyspaces()) {
            auto dk = make_partition_key(name);
            if (!this_shard_owns(dk) || !contains_key(qr.partition_range(), dk)) {
                continue;
            }
            keyspace_names.push_back({std::move(name), std::move(dk)});
        }

        boost::sort(keyspace_names, [less = dht::ring_position_less_comparator(*_s)]
                (const decorated_keyspace_name& l, const decorated_keyspace_name& r) {
            return less(l.key, r.key);
        });

        using snapshots_by_tables_map = std::map<sstring, std::map<sstring, replica::table::snapshot_details>>;

        class snapshot_reducer {
        private:
            snapshots_by_tables_map _result;
        public:
            future<> operator()(const snapshots_by_tables_map& value) {
                for (auto& [table_name, snapshots] : value) {
                    if (auto [_, added] = _result.try_emplace(table_name, std::move(snapshots)); added) {
                        continue;
                    }
                    auto& rp = _result.at(table_name);
                    for (auto&& [snapshot_name, snapshot_detail]: snapshots) {
                        if (auto [_, added] = rp.try_emplace(snapshot_name, std::move(snapshot_detail)); added) {
                            continue;
                        }
                        auto& detail = rp.at(snapshot_name);
                        detail.live += snapshot_detail.live;
                        detail.total += snapshot_detail.total;
                    }
                }
                return make_ready_future<>();
            }
            snapshots_by_tables_map get() && {
                return std::move(_result);
            }
        };

        for (auto& ks_data : keyspace_names) {
            co_await result.emit_partition_start(ks_data.key);

            const auto snapshots_by_tables = co_await _db.map_reduce(snapshot_reducer(), [ks_name_ = ks_data.name] (replica::database& db) mutable -> future<snapshots_by_tables_map> {
                auto ks_name = std::move(ks_name_);
                snapshots_by_tables_map snapshots_by_tables;
                for (auto& [_, table] : db.get_column_families()) {
                    if (table->schema()->ks_name() != ks_name) {
                        continue;
                    }
                    const auto unordered_snapshots = co_await table->get_snapshot_details();
                    snapshots_by_tables.emplace(table->schema()->cf_name(), std::map<sstring, replica::table::snapshot_details>(unordered_snapshots.begin(), unordered_snapshots.end()));
                }
                co_return snapshots_by_tables;
            });

            for (const auto& [table_name, snapshots] : snapshots_by_tables) {
                for (auto& [snapshot_name, details] : snapshots) {
                    clustering_row cr(make_clustering_key(table_name, snapshot_name));
                    set_cell(cr.cells(), "live", details.live);
                    set_cell(cr.cells(), "total", details.total);
                    co_await result.emit_row(std::move(cr));
                }

            }

            co_await result.emit_partition_end();
        }
    }
};

class protocol_servers_table : public memtable_filling_virtual_table {
private:
    service::storage_service& _ss;

    struct protocol_server_info {
        sstring name;
        sstring protocol;
        sstring protocol_version;
        std::vector<sstring> listen_addresses;

        explicit protocol_server_info(protocol_server& s)
            : name(s.name())
            , protocol(s.protocol())
            , protocol_version(s.protocol_version()) {
            for (const auto& addr : s.listen_addresses()) {
                listen_addresses.push_back(format("{}:{}", addr.addr(), addr.port()));
            }
        }
    };
public:
    explicit protocol_servers_table(service::storage_service& ss)
        : memtable_filling_virtual_table(build_schema())
        , _ss(ss) {
        _shard_aware = true;
    }

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "protocol_servers");
        return schema_builder(system_keyspace::NAME, "protocol_servers", std::make_optional(id))
            .with_column("name", utf8_type, column_kind::partition_key)
            .with_column("protocol", utf8_type)
            .with_column("protocol_version", utf8_type)
            .with_column("listen_addresses", list_type_impl::get_instance(utf8_type, false))
            .set_comment("Lists all client protocol servers and their status.")
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    future<> execute(std::function<void(mutation)> mutation_sink) override {
        // Servers are registered on shard 0 only
        const auto server_infos = co_await smp::submit_to(0ul, [&ss = _ss.container()] {
            return boost::copy_range<std::vector<protocol_server_info>>(ss.local().protocol_servers()
                    | boost::adaptors::transformed([] (protocol_server* s) { return protocol_server_info(*s); }));
        });
        for (auto server : server_infos) {
            auto dk = dht::decorate_key(*_s, partition_key::from_single_value(*schema(), data_value(server.name).serialize_nonnull()));
            if (!this_shard_owns(dk)) {
                continue;
            }
            mutation m(schema(), std::move(dk));
            row& cr = m.partition().clustered_row(*schema(), clustering_key::make_empty()).cells();
            set_cell(cr, "protocol", server.protocol);
            set_cell(cr, "protocol_version", server.protocol_version);
            std::vector<data_value> addresses(server.listen_addresses.begin(), server.listen_addresses.end());
            set_cell(cr, "listen_addresses", make_list_value(schema()->get_column_definition("listen_addresses")->type, std::move(addresses)));
            mutation_sink(std::move(m));
        }
    }
};

class runtime_info_table : public memtable_filling_virtual_table {
private:
    distributed<replica::database>& _db;
    service::storage_service& _ss;
    std::optional<dht::decorated_key> _generic_key;

private:
    std::optional<dht::decorated_key> maybe_make_key(sstring key) {
        auto dk = dht::decorate_key(*_s, partition_key::from_single_value(*schema(), data_value(std::move(key)).serialize_nonnull()));
        if (this_shard_owns(dk)) {
            return dk;
        }
        return std::nullopt;
    }

    void do_add_partition(std::function<void(mutation)>& mutation_sink, dht::decorated_key key, std::vector<std::pair<sstring, sstring>> rows) {
        mutation m(schema(), std::move(key));
        for (auto&& [ckey, cvalue] : rows) {
            row& cr = m.partition().clustered_row(*schema(), clustering_key::from_single_value(*schema(), data_value(std::move(ckey)).serialize_nonnull())).cells();
            set_cell(cr, "value", std::move(cvalue));
        }
        mutation_sink(std::move(m));
    }

    void add_partition(std::function<void(mutation)>& mutation_sink, sstring key, sstring value) {
        if (_generic_key) {
            do_add_partition(mutation_sink, *_generic_key, {{key, std::move(value)}});
        }
    }

    void add_partition(std::function<void(mutation)>& mutation_sink, sstring key, std::initializer_list<std::pair<sstring, sstring>> rows) {
        auto dk = maybe_make_key(std::move(key));
        if (dk) {
            do_add_partition(mutation_sink, std::move(*dk), std::move(rows));
        }
    }

    future<> add_partition(std::function<void(mutation)>& mutation_sink, sstring key, std::function<future<sstring>()> value_producer) {
        if (_generic_key) {
            do_add_partition(mutation_sink, *_generic_key, {{key, co_await value_producer()}});
        }
    }

    future<> add_partition(std::function<void(mutation)>& mutation_sink, sstring key, std::function<future<std::vector<std::pair<sstring, sstring>>>()> value_producer) {
        auto dk = maybe_make_key(std::move(key));
        if (dk) {
            do_add_partition(mutation_sink, std::move(*dk), co_await value_producer());
        }
    }

    template <typename T>
    future<T> map_reduce_tables(std::function<T(replica::table&)> map, std::function<T(T, T)> reduce = std::plus<T>{}) {
        class shard_reducer {
            T _v{};
            std::function<T(T, T)> _reduce;
        public:
            shard_reducer(std::function<T(T, T)> reduce) : _reduce(std::move(reduce)) { }
            future<> operator()(T v) {
                v = _reduce(_v, v);
                return make_ready_future<>();
            }
            T get() && { return std::move(_v); }
        };
        co_return co_await _db.map_reduce(shard_reducer(reduce), [map, reduce] (replica::database& db) {
            T val = {};
            for (auto& [_, table] : db.get_column_families()) {
               val = reduce(val, map(*table));
            }
            return val;
        });
    }
    template <typename T>
    future<T> map_reduce_shards(std::function<T()> map, std::function<T(T, T)> reduce = std::plus<T>{}, T initial = {}) {
        co_return co_await map_reduce(
                boost::irange(0u, smp::count),
                [map] (shard_id shard) {
                    return smp::submit_to(shard, [map] {
                        return map();
                    });
                },
                initial,
                reduce);
    }

public:
    explicit runtime_info_table(distributed<replica::database>& db, service::storage_service& ss)
        : memtable_filling_virtual_table(build_schema())
        , _db(db)
        , _ss(ss) {
        _shard_aware = true;
        _generic_key = maybe_make_key("generic");
    }

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "runtime_info");
        return schema_builder(system_keyspace::NAME, "runtime_info", std::make_optional(id))
            .with_column("group", utf8_type, column_kind::partition_key)
            .with_column("item", utf8_type, column_kind::clustering_key)
            .with_column("value", utf8_type)
            .set_comment("Runtime system information.")
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    future<> execute(std::function<void(mutation)> mutation_sink) override {
        co_await add_partition(mutation_sink, "gossip_active", [this] () -> future<sstring> {
            return _ss.is_gossip_running().then([] (bool running){
                return format("{}", running);
            });
        });
        co_await add_partition(mutation_sink, "load", [this] () -> future<sstring> {
            return map_reduce_tables<int64_t>([] (replica::table& tbl) {
                return tbl.get_stats().live_disk_space_used;
            }).then([] (int64_t load) {
                return format("{}", load);
            });
        });
        add_partition(mutation_sink, "uptime", format("{} seconds", std::chrono::duration_cast<std::chrono::seconds>(engine().uptime()).count()));
        add_partition(mutation_sink, "trace_probability", format("{:.2}", tracing::tracing::get_local_tracing_instance().get_trace_probability()));
        co_await add_partition(mutation_sink, "memory", [this] () {
            struct stats {
                // take the pre-reserved memory into account, as seastar only returns
                // the stats of memory managed by the seastar allocator, but we instruct
                // it to reserve addition memory for non-seastar allocator on per-shard
                // basis.
                uint64_t total = 0;
                uint64_t free = 0;
                static stats reduce(stats a, stats b) {
                    return stats{
                        a.total + b.total + db::config::wasm_udf_reserved_memory,
                        a.free + b.free};
                    };
            };
            return map_reduce_shards<stats>([] () {
                const auto& s = memory::stats();
                return stats{s.total_memory(), s.free_memory()};
            }, stats::reduce, stats{}).then([] (stats s) {
                return std::vector<std::pair<sstring, sstring>>{
                        {"total", format("{}", s.total)},
                        {"used", format("{}", s.total - s.free)},
                        {"free", format("{}", s.free)}};
            });
        });
        co_await add_partition(mutation_sink, "memtable", [this] () {
            struct stats {
                uint64_t total = 0;
                uint64_t free = 0;
                uint64_t entries = 0;
                static stats reduce(stats a, stats b) { return stats{a.total + b.total, a.free + b.free, a.entries + b.entries}; }
            };
            return map_reduce_tables<stats>([] (replica::table& t) {
                logalloc::occupancy_stats s;
                uint64_t partition_count = 0;
                for (replica::memtable* active_memtable : t.active_memtables()) {
                    s += active_memtable->region().occupancy();
                    partition_count += active_memtable->partition_count();
                }
                return stats{s.total_space(), s.free_space(), partition_count};
            }, stats::reduce).then([] (stats s) {
                return std::vector<std::pair<sstring, sstring>>{
                        {"memory_total", format("{}", s.total)},
                        {"memory_used", format("{}", s.total - s.free)},
                        {"memory_free", format("{}", s.free)},
                        {"entries", format("{}", s.entries)}};
            });
        });
        co_await add_partition(mutation_sink, "cache", [this] () {
            struct stats {
                uint64_t total = 0;
                uint64_t free = 0;
                uint64_t entries = 0;
                uint64_t hits = 0;
                uint64_t misses = 0;
                utils::rate_moving_average hits_moving_average;
                utils::rate_moving_average requests_moving_average;
                static stats reduce(stats a, stats b) {
                    return stats{
                        a.total + b.total,
                        a.free + b.free,
                        a.entries + b.entries,
                        a.hits + b.hits,
                        a.misses + b.misses,
                        a.hits_moving_average + b.hits_moving_average,
                        a.requests_moving_average + b.requests_moving_average};
                }
            };
            return _db.map_reduce0([] (replica::database& db) {
                stats res{};
                auto occupancy = db.row_cache_tracker().region().occupancy();
                res.total = occupancy.total_space();
                res.free = occupancy.free_space();
                res.entries = db.row_cache_tracker().partitions();
                for (const auto& [_, t] : db.get_column_families()) {
                    auto& cache_stats = t->get_row_cache().stats();
                    res.hits += cache_stats.hits.count();
                    res.misses += cache_stats.misses.count();
                    res.hits_moving_average += cache_stats.hits.rate();
                    res.requests_moving_average += (cache_stats.hits.rate() + cache_stats.misses.rate());
                }
                return res;
            }, stats{}, stats::reduce).then([] (stats s) {
                return std::vector<std::pair<sstring, sstring>>{
                        {"memory_total", format("{}", s.total)},
                        {"memory_used", format("{}", s.total - s.free)},
                        {"memory_free", format("{}", s.free)},
                        {"entries", format("{}", s.entries)},
                        {"hits", format("{}", s.hits)},
                        {"misses", format("{}", s.misses)},
                        {"hit_rate_total", format("{:.2}", static_cast<double>(s.hits) / static_cast<double>(s.hits + s.misses))},
                        {"hit_rate_recent", format("{:.2}", s.hits_moving_average.mean_rate)},
                        {"requests_total", format("{}", s.hits + s.misses)},
                        {"requests_recent", format("{}", static_cast<uint64_t>(s.requests_moving_average.mean_rate))}};
            });
        });
        co_await add_partition(mutation_sink, "incremental_backup_enabled", [this] () {
            return _db.map_reduce0([] (replica::database& db) {
                return boost::algorithm::any_of(db.get_keyspaces(), [] (const auto& id_and_ks) {
                    return id_and_ks.second.incremental_backups_enabled();
                });
            }, false, std::logical_or{}).then([] (bool res) -> sstring {
                return res ? "true" : "false";
            });
        });
    }
};

class versions_table : public memtable_filling_virtual_table {
public:
    explicit versions_table()
        : memtable_filling_virtual_table(build_schema()) {
        _shard_aware = false;
    }

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "versions");
        return schema_builder(system_keyspace::NAME, "versions", std::make_optional(id))
            .with_column("key", utf8_type, column_kind::partition_key)
            .with_column("version", utf8_type)
            .with_column("build_mode", utf8_type)
            .with_column("build_id", utf8_type)
            .set_comment("Version information.")
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    future<> execute(std::function<void(mutation)> mutation_sink) override {
        mutation m(schema(), partition_key::from_single_value(*schema(), data_value("local").serialize_nonnull()));
        row& cr = m.partition().clustered_row(*schema(), clustering_key::make_empty()).cells();
        set_cell(cr, "version", scylla_version());
        set_cell(cr, "build_mode", scylla_build_mode());
        set_cell(cr, "build_id", get_build_id());
        mutation_sink(std::move(m));
        return make_ready_future<>();
    }
};

class db_config_table final : public streaming_virtual_table {
    db::config& _cfg;

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "config");
        return schema_builder(system_keyspace::NAME, "config", std::make_optional(id))
            .with_column("name", utf8_type, column_kind::partition_key)
            .with_column("type", utf8_type)
            .with_column("source", utf8_type)
            .with_column("value", utf8_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    future<> execute(reader_permit permit, result_collector& result, const query_restrictions& qr) override {
        struct config_entry {
            dht::decorated_key key;
            sstring_view type;
            sstring source;
            sstring value;
        };

        std::vector<config_entry> cfg;
        for (auto&& cfg_ref : _cfg.values()) {
            auto&& c = cfg_ref.get();
            dht::decorated_key dk = dht::decorate_key(*_s, partition_key::from_single_value(*_s, data_value(c.name()).serialize_nonnull()));
            if (this_shard_owns(dk)) {
                cfg.emplace_back(config_entry{ std::move(dk), c.type_name(), c.source_name(), c.value_as_json()._res });
            }
        }

        boost::sort(cfg, [less = dht::ring_position_less_comparator(*_s)]
                (const config_entry& l, const config_entry& r) {
            return less(l.key, r.key);
        });

        for (auto&& c : cfg) {
            co_await result.emit_partition_start(c.key);
            mutation m(schema(), c.key);
            clustering_row cr(clustering_key::make_empty());
            set_cell(cr.cells(), "type", c.type);
            set_cell(cr.cells(), "source", c.source);
            set_cell(cr.cells(), "value", c.value);
            co_await result.emit_row(std::move(cr));
            co_await result.emit_partition_end();
        }
    }

    virtual future<> apply(const frozen_mutation& fm) override {
        const mutation m = fm.unfreeze(_s);
        query::result_set rs(m);
        auto name = rs.row(0).get<sstring>("name");
        auto value = rs.row(0).get<sstring>("value");

        if (!_cfg.enable_cql_config_updates()) {
            return virtual_table::apply(fm); // will return back exceptional future
        }

        if (!name) {
            return make_exception_future<>(virtual_table_update_exception("option name is required"));
        }

        if (!value) {
            return make_exception_future<>(virtual_table_update_exception("option value is required"));
        }

        if (rs.row(0).cells().contains("type")) {
            return make_exception_future<>(virtual_table_update_exception("option type is immutable"));
        }

        if (rs.row(0).cells().contains("source")) {
            return make_exception_future<>(virtual_table_update_exception("option source is not updateable"));
        }

        return smp::submit_to(0, [&cfg = _cfg, name = std::move(*name), value = std::move(*value)] () mutable -> future<> {
            for (auto& c_ref : cfg.values()) {
                auto& c = c_ref.get();
                if (c.name() == name) {
                    std::exception_ptr ex;
                    try {
                        if (co_await c.set_value_on_all_shards(value, utils::config_file::config_source::CQL)) {
                            co_return;
                        } else {
                            ex = std::make_exception_ptr(virtual_table_update_exception("option is not live-updateable"));
                        }
                    } catch (boost::bad_lexical_cast&) {
                        ex = std::make_exception_ptr(virtual_table_update_exception("cannot parse option value"));
                    }
                    co_await coroutine::return_exception_ptr(std::move(ex));
                }
            }

            co_await coroutine::return_exception(virtual_table_update_exception("no such option"));
        });
    }

public:
    explicit db_config_table(db::config& cfg)
            : streaming_virtual_table(build_schema())
            , _cfg(cfg)
    {
        _shard_aware = true;
    }
};

class clients_table : public streaming_virtual_table {
    service::storage_service& _ss;

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "clients");
        return schema_builder(system_keyspace::NAME, "clients", std::make_optional(id))
            .with_column("address", inet_addr_type, column_kind::partition_key)
            .with_column("port", int32_type, column_kind::clustering_key)
            .with_column("client_type", utf8_type, column_kind::clustering_key)
            .with_column("shard_id", int32_type)
            .with_column("connection_stage", utf8_type)
            .with_column("driver_name", utf8_type)
            .with_column("driver_version", utf8_type)
            .with_column("hostname", utf8_type)
            .with_column("protocol_version", int32_type)
            .with_column("ssl_cipher_suite", utf8_type)
            .with_column("ssl_enabled", boolean_type)
            .with_column("ssl_protocol", utf8_type)
            .with_column("username", utf8_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    dht::decorated_key make_partition_key(net::inet_address ip) {
        return dht::decorate_key(*_s, partition_key::from_single_value(*_s, data_value(ip).serialize_nonnull()));
    }

    clustering_key make_clustering_key(int32_t port, sstring clt) {
        return clustering_key::from_exploded(*_s, {
            data_value(port).serialize_nonnull(),
            data_value(clt).serialize_nonnull()
        });
    }

    future<> execute(reader_permit permit, result_collector& result, const query_restrictions& qr) override {
        // Collect
        using client_data_vec = utils::chunked_vector<client_data>;
        using shard_client_data = std::vector<client_data_vec>;
        std::vector<foreign_ptr<std::unique_ptr<shard_client_data>>> cd_vec;
        cd_vec.resize(smp::count);

        auto servers = co_await _ss.container().invoke_on(0, [] (auto& ss) { return ss.protocol_servers(); });
        co_await smp::invoke_on_all([&cd_vec_ = cd_vec, &servers_ = servers] () -> future<> {
            auto& cd_vec = cd_vec_;
            auto& servers = servers_;

            auto scd = std::make_unique<shard_client_data>();
            for (const auto& ps : servers) {
                client_data_vec cds = co_await ps->get_client_data();
                if (cds.size() != 0) {
                    scd->emplace_back(std::move(cds));
                }
            }
            cd_vec[this_shard_id()] = make_foreign(std::move(scd));
        });

        // Partition
        struct decorated_ip {
            dht::decorated_key key;
            net::inet_address ip;

            struct compare {
                dht::ring_position_less_comparator less;
                explicit compare(const class schema& s) : less(s) {}
                bool operator()(const decorated_ip& a, const decorated_ip& b) const {
                    return less(a.key, b.key);
                }
            };
        };

        decorated_ip::compare cmp(*_s);
        std::set<decorated_ip, decorated_ip::compare> ips(cmp);
        std::unordered_map<net::inet_address, client_data_vec> cd_map;
        for (int i = 0; i < smp::count; i++) {
            for (auto&& ps_cdc : *cd_vec[i]) {
                for (auto&& cd : ps_cdc) {
                    if (cd_map.contains(cd.ip)) {
                        cd_map[cd.ip].emplace_back(std::move(cd));
                    } else {
                        dht::decorated_key key = make_partition_key(cd.ip);
                        if (this_shard_owns(key) && contains_key(qr.partition_range(), key)) {
                            ips.insert(decorated_ip{std::move(key), cd.ip});
                            cd_map[cd.ip].emplace_back(std::move(cd));
                        }
                    }
                    co_await coroutine::maybe_yield();
                }
            }
        }

        // Emit
        for (const auto& dip : ips) {
            co_await result.emit_partition_start(dip.key);
            auto& clients = cd_map[dip.ip];

            boost::sort(clients, [] (const client_data& a, const client_data& b) {
                return a.port < b.port || a.client_type_str() < b.client_type_str();
            });

            for (const auto& cd : clients) {
                clustering_row cr(make_clustering_key(cd.port, cd.client_type_str()));
                set_cell(cr.cells(), "shard_id", cd.shard_id);
                set_cell(cr.cells(), "connection_stage", cd.stage_str());
                if (cd.driver_name) {
                    set_cell(cr.cells(), "driver_name", *cd.driver_name);
                }
                if (cd.driver_version) {
                    set_cell(cr.cells(), "driver_version", *cd.driver_version);
                }
                if (cd.hostname) {
                    set_cell(cr.cells(), "hostname", *cd.hostname);
                }
                if (cd.protocol_version) {
                    set_cell(cr.cells(), "protocol_version", *cd.protocol_version);
                }
                if (cd.ssl_cipher_suite) {
                    set_cell(cr.cells(), "ssl_cipher_suite", *cd.ssl_cipher_suite);
                }
                if (cd.ssl_enabled) {
                    set_cell(cr.cells(), "ssl_enabled", *cd.ssl_enabled);
                }
                if (cd.ssl_protocol) {
                    set_cell(cr.cells(), "ssl_protocol", *cd.ssl_protocol);
                }
                set_cell(cr.cells(), "username", cd.username ? *cd.username : sstring("anonymous"));
                co_await result.emit_row(std::move(cr));
            }
            co_await result.emit_partition_end();
        }
    }

public:
    clients_table(service::storage_service& ss)
            : streaming_virtual_table(build_schema())
            , _ss(ss)
    {
        _shard_aware = true;
    }
};

// Shows the current state of each Raft group.
// Currently it shows only the configuration.
// In the future we plan to add additional columns with more information.
class raft_state_table : public streaming_virtual_table {
private:
    sharded<service::raft_group_registry>& _raft_gr;

public:
    raft_state_table(sharded<service::raft_group_registry>& raft_gr)
        : streaming_virtual_table(build_schema())
        , _raft_gr(raft_gr) {
    }

    future<> execute(reader_permit permit, result_collector& result, const query_restrictions& qr) override {
        struct decorated_gid {
            raft::group_id gid;
            dht::decorated_key key;
            unsigned shard;
        };

        auto groups_and_shards = co_await _raft_gr.map([] (service::raft_group_registry& raft_gr) {
            return std::pair{raft_gr.all_groups(), this_shard_id()};
        });

        std::vector<decorated_gid> decorated_gids;
        for (auto& [groups, shard]: groups_and_shards) {
            for (auto& gid: groups) {
                decorated_gids.push_back(decorated_gid{gid, make_partition_key(gid), shard});
            }
        }

        // Must return partitions in token order.
        std::sort(decorated_gids.begin(), decorated_gids.end(), [less = dht::ring_position_less_comparator(*_s)]
                (const decorated_gid& l, const decorated_gid& r) { return less(l.key, r.key); });

        for (auto& [gid, dk, shard]: decorated_gids) {
            if (!contains_key(qr.partition_range(), dk)) {
                continue;
            }

            auto cfg_opt = co_await _raft_gr.invoke_on(shard,
                    [gid=gid] (service::raft_group_registry& raft_gr) -> std::optional<raft::configuration> {
                // Be ready for a group to disappear while we're querying.
                auto* srv = raft_gr.find_server(gid);
                if (!srv) {
                    return std::nullopt;
                }
                // FIXME: the configuration returned here is obtained from raft::fsm, it may not be
                // persisted yet, so this is not 100% correct. It may happen that we crash after
                // a config entry is appended in-memory in fsm but before it's persisted. It would be
                // incorrect to return the configuration observed during this window - after restart
                // the configuration would revert to the previous one.  Perhaps this is unlikely to
                // happen in practice, but for correctness we should add a way of querying the
                // latest persisted configuration.
                return srv->get_configuration();
            });

            if (!cfg_opt) {
                continue;
            }

            co_await result.emit_partition_start(dk);

            // List current config first, because 'C' < 'P' and the disposition
            // (ascii_type, 'CURRENT' vs 'PREVIOUS') is the first column in the clustering key.
            co_await emit_member_set(result, "CURRENT", cfg_opt->current);
            co_await emit_member_set(result, "PREVIOUS", cfg_opt->previous);

            co_await result.emit_partition_end();
        }
    }

private:
    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "raft_state");
        return schema_builder(system_keyspace::NAME, "raft_state", std::make_optional(id))
            .with_column("group_id", timeuuid_type, column_kind::partition_key)
            .with_column("disposition", ascii_type, column_kind::clustering_key) // can be 'CURRENT` or `PREVIOUS'
            .with_column("server_id", uuid_type, column_kind::clustering_key)
            .with_column("can_vote", boolean_type)
            .set_comment("Currently operating RAFT configuration")
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    dht::decorated_key make_partition_key(raft::group_id gid) {
        // Make sure to use timeuuid_native_type so comparisons are done correctly
        // (we must emit partitions in the correct token order).
        return dht::decorate_key(*_s, partition_key::from_single_value(
                *_s, data_value(timeuuid_native_type{gid.uuid()}).serialize_nonnull()));
    }

    clustering_key make_clustering_key(std::string_view disposition, raft::server_id id) {
        return clustering_key::from_exploded(*_s, {
            data_value(disposition).serialize_nonnull(),
            data_value(id.uuid()).serialize_nonnull()
        });
    }

    future<> emit_member_set(result_collector& result, std::string_view disposition,
                             const raft::config_member_set& set) {
        // Must sort servers in clustering order (i.e. according to their IDs).
        // This is how `config_member::operator<` works so no need for custom comparator.
        std::vector<raft::config_member> members{set.begin(), set.end()};
        std::sort(members.begin(), members.end());
        for (auto& member: members) {
            clustering_row cr{make_clustering_key(disposition, member.addr.id)};
            set_cell(cr.cells(), "can_vote", member.can_vote);
            co_await result.emit_row(std::move(cr));
        }
    }
};

class mutation_dump_table : public virtual_table {
    distributed<replica::database>& _db;

private:
    class reader : public flat_mutation_reader_v2::impl {
        struct remote_state {
            schema_ptr output_schema;
            schema_ptr underlying_schema;
            reader_permit permit;
            tracing::trace_state_ptr ts;
            flat_mutation_reader_v2::tracked_buffer buf;
            flat_mutation_reader_v2_opt reader;
            // has to be sorted, because it is part of the clustering key
            std::map<sstring, mutation_source> mutation_sources;

            remote_state(schema_ptr output_schema, schema_ptr underlying_schema, reader_permit permit, tracing::trace_state_ptr ts)
                : output_schema(std::move(output_schema)), underlying_schema(std::move(underlying_schema)), permit(permit), ts(std::move(ts)), buf(permit)
            { }
        };

    private:
        distributed<replica::database>& _db;
        dht::decorated_key _dk;
        query::partition_slice _ps;
        tracing::trace_state_ptr _ts;
        std::vector<position_range> _pos_ranges;
        schema_ptr _underlying_schema;
        dht::partition_range _underlying_pr;
        query::partition_slice _underlying_ps;
        unsigned _shard;
        foreign_ptr<std::unique_ptr<remote_state>> _remote_state;
        bool _partition_start_emitted = false;
        bool _partition_end_emitted = false;

    private:
        schema_ptr extract_underlying_schema() {
            auto exploded_pk = _dk.key().explode(*_schema);
            const auto keyspace_name = value_cast<sstring>(utf8_type->deserialize_value(exploded_pk[0]));
            const auto table_name = value_cast<sstring>(utf8_type->deserialize_value(exploded_pk[1]));
            return _db.local().find_schema(keyspace_name, table_name);
        }

        std::vector<bytes>
        unserialize_underlying_key(bytes_view serialized_key, const std::vector<data_type>& underlying_key_types, allow_prefixes allow_prefix) {
            const auto underlying_key_vals = value_cast<std::vector<data_value>>(get_underlying_key_type()->deserialize_value(serialized_key));

            if (allow_prefix == allow_prefixes::yes) {
                if (underlying_key_types.size() < underlying_key_vals.size()) {
                    throw std::runtime_error(fmt::format("underlying table has at most {} clustering key components, but got {}",
                                underlying_key_types.size(), underlying_key_vals.size()));
                }
            } else {
                if (underlying_key_types.size() != underlying_key_vals.size()) {
                    throw std::runtime_error(fmt::format("underlying table has {} partition key components, but got {}",
                                underlying_key_types.size(), underlying_key_vals.size()));
                }
            }

            std::vector<bytes> underlying_key_raw;
            underlying_key_raw.reserve(underlying_key_types.size());
            for (unsigned i = 0; i < underlying_key_vals.size(); ++i) {
                const auto& type = underlying_key_types.at(i);
                const auto& value = underlying_key_vals.at(i);
                const auto component_str = value_cast<sstring>(value);
                underlying_key_raw.push_back(type->from_string(component_str));
            }

            return underlying_key_raw;
        }

        dht::partition_range extract_underlying_pr() {
            const auto& pk_types = _underlying_schema->partition_key_type()->types();
            auto underlying_pk = partition_key::from_exploded(unserialize_underlying_key(_dk.key().explode(*_schema).at(2), pk_types, allow_prefixes::no));
            auto underlying_dk = dht::decorate_key(*_underlying_schema, underlying_pk);
            return dht::partition_range::make_singular(std::move(underlying_dk));
        }

        query::partition_slice extract_underlying_ps() {
            auto builder = partition_slice_builder(*_underlying_schema);
            const auto& ranges = _ps.row_ranges(*_schema, _dk.key());
            const auto& ck_types = _underlying_schema->clustering_key_type()->types();
            for (const auto& range : ranges) {
                if ((!range.start() || range.start()->value().explode(*_schema).size() < 3) &&
                        (!range.end() || range.end()->value().explode(*_schema).size() < 3)) {
                    continue; // doesn't contain any restriction to underlying
                }
                builder.with_range(range.transform([&] (const clustering_key& ck) {
                    auto exploded_ck = ck.explode(*_schema);
                    if (exploded_ck.size() < 3) {
                        return clustering_key::make_empty();
                    }
                    return clustering_key::from_exploded(unserialize_underlying_key(exploded_ck.at(2), ck_types, allow_prefixes::yes));
                }));
            }
            auto ret = builder.build();
            return ret;
        }

        // Runs on _shard
        static bool
        is_source_included(const char* source, const ::schema& output_schema, const dht::decorated_key& dk, const query::partition_slice& ps) {
            const auto& ranges = ps.row_ranges(output_schema, dk.key());
            std::vector<data_value> ck_exploded;
            ck_exploded.push_back(data_value(source));
            auto ck = clustering_key::from_deeply_exploded(output_schema, ck_exploded);
            auto cmp = clustering_key::prefix_equal_tri_compare(output_schema);
            const auto res = std::ranges::any_of(ranges, [&] (const query::clustering_range& cr) {
                return cr.contains(ck, cmp);
            });
            return res;
        }

        // Runs on _shard
        static foreign_ptr<std::unique_ptr<remote_state>>
        create_remote_state(replica::database& db, schema_ptr output_schema, schema_ptr underlying_schema, reader_permit permit,
                const dht::decorated_key& dk, const query::partition_slice& ps, tracing::trace_state_ptr ts) {
            auto& tbl = db.find_column_family(underlying_schema);
            auto rs = std::make_unique<remote_state>(output_schema, std::move(underlying_schema), std::move(permit), std::move(ts));

            if (is_source_included("memtable", *output_schema, dk, ps)) {
                rs->mutation_sources.emplace("memtable", mutation_source([&tbl] (
                        schema_ptr schema,
                        reader_permit permit,
                        const dht::partition_range& pr,
                        const query::partition_slice& ps,
                        tracing::trace_state_ptr ts,
                        streamed_mutation::forwarding,
                        mutation_reader::forwarding) {
                    return tbl.make_memtable_reader(std::move(schema), std::move(permit), pr, ps, ts);
                }));
            }
            if (is_source_included("row-cache", *output_schema, dk, ps)) {
                rs->mutation_sources.emplace("row-cache", mutation_source([&tbl] (
                        schema_ptr schema,
                        reader_permit permit,
                        const dht::partition_range& pr,
                        const query::partition_slice& ps,
                        tracing::trace_state_ptr ts,
                        streamed_mutation::forwarding,
                        mutation_reader::forwarding) {
                    return tbl.make_cache_reader(std::move(schema), std::move(permit), pr, ps, ts);
                }));
            }
            if (is_source_included("sstable", *output_schema, dk, ps)) {
                rs->mutation_sources.emplace("sstable", mutation_source([&tbl] (
                        schema_ptr schema,
                        reader_permit permit,
                        const dht::partition_range& pr,
                        const query::partition_slice& ps,
                        tracing::trace_state_ptr ts,
                        streamed_mutation::forwarding fwd_sm,
                        mutation_reader::forwarding fwd) {
                    return tbl.make_sstable_reader(std::move(schema), std::move(permit), pr, ps, std::move(ts), fwd_sm, fwd);
                }));
            }
            return make_foreign(std::move(rs));
        }

        static clustering_key
        transform_clustering_key(position_in_partition_view pos, const sstring& data_source_name, const ::schema& output_schema, const ::schema& underlying_schema) {
            const auto& underlying_ck_types = underlying_schema.clustering_key_type()->types();
            const auto underlying_ck_raw_values = pos.has_key() ? pos.key().explode(underlying_schema) : std::vector<bytes>{};
            std::vector<data_value> underlying_ck_data_values;
            underlying_ck_data_values.reserve(underlying_ck_raw_values.size());
            for (unsigned i = 0; i < underlying_ck_raw_values.size(); ++i) {
                const auto ck_component_str = underlying_ck_types[i]->to_string(underlying_ck_raw_values[i]);
                underlying_ck_data_values.emplace_back(ck_component_str);
            }

            std::vector<bytes> output_ck_raw_values;
            const auto& output_ck_types = output_schema.clustering_key_type()->types();
            output_ck_raw_values.push_back(data_value(data_source_name).serialize_nonnull());
            output_ck_raw_values.push_back(data_value(static_cast<int8_t>(pos.region())).serialize_nonnull());
            output_ck_raw_values.push_back(make_list_value(output_ck_types[2], underlying_ck_data_values).serialize_nonnull());
            output_ck_raw_values.push_back(data_value(static_cast<int8_t>(pos.get_bound_weight())).serialize_nonnull());

            return clustering_key::from_exploded(output_schema, output_ck_raw_values);
        }

        // Runs on _shard
        static mutation_fragment_v2
        transform_mutation_fragment(mutation_fragment_v2&& mf, const sstring& data_source_name, schema_ptr output_schema, schema_ptr underlying_schema, reader_permit permit) {
            auto ck = transform_clustering_key(mf.position(), data_source_name, *output_schema, *underlying_schema);
            auto cr = clustering_row(ck);
            mutation_dump_table::set_cell(*output_schema, cr.cells(), "kind", fmt::to_string(mf.mutation_fragment_kind()));

            switch (mf.mutation_fragment_kind()) {
                case mutation_fragment_v2::kind::partition_start:
                    if (auto tomb = mf.as_partition_start().partition_tombstone()) {
                        mutation_dump_table::set_cell(*output_schema, cr.cells(), "value", fmt::to_string(tomb));
                    }
                    break;
                case mutation_fragment_v2::kind::static_row:
                    mutation_dump_table::set_cell(*output_schema, cr.cells(), "value", fmt::to_string(mutation_fragment_v2::printer(*underlying_schema, mf)));
                    break;
                case mutation_fragment_v2::kind::clustering_row:
                     mutation_dump_table::set_cell(*output_schema, cr.cells(), "value", fmt::to_string(mutation_fragment_v2::printer(*underlying_schema, mf)));
                    break;
                case mutation_fragment_v2::kind::range_tombstone_change:
                    mutation_dump_table::set_cell(*output_schema, cr.cells(), "value", fmt::to_string(mutation_fragment_v2::printer(*underlying_schema, mf)));
                    break;
                case mutation_fragment_v2::kind::partition_end:
                    // No value set.
                    break;
            }

            return mutation_fragment_v2(*output_schema, permit, std::move(cr));
        }

        // Runs on _shard
        static future<> fill_remote_buffer(remote_state& rs, size_t max_buffer_size_in_bytes, const dht::partition_range& pr, const query::partition_slice& ps) {
            rs.buf.clear();
            size_t size = 0;
            while (size < max_buffer_size_in_bytes && !rs.mutation_sources.empty()) {
                if (!rs.reader) {
                    rs.reader = rs.mutation_sources.begin()->second.make_reader_v2(rs.underlying_schema, rs.permit, pr, ps,
                            rs.ts, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
                }
                auto mf_opt = co_await (*rs.reader)();
                if (!mf_opt) {
                    co_await rs.reader->close();
                    rs.reader = {};
                    rs.mutation_sources.erase(rs.mutation_sources.begin());
                    continue;
                }
                auto trasnformed_mf = transform_mutation_fragment(std::move(*mf_opt), rs.mutation_sources.begin()->first, rs.output_schema, rs.underlying_schema, rs.permit);
                rs.buf.push_back(std::move(trasnformed_mf));
                size += rs.buf.back().memory_usage();
            }
        }

    public:
        reader(schema_ptr schema, reader_permit permit, distributed<replica::database>& db, const dht::decorated_key& dk, const query::partition_slice& ps, tracing::trace_state_ptr ts)
            : impl(std::move(schema), std::move(permit))
            , _db(db)
            , _dk(dk)
            , _ps(ps)
            , _ts(std::move(ts))
            , _underlying_schema(extract_underlying_schema())
            , _underlying_pr(extract_underlying_pr())
            , _underlying_ps(extract_underlying_ps())
            , _shard(dht::shard_of(*_underlying_schema, _underlying_pr.start()->value().token()))
        { }

        virtual future<> fill_buffer() override {
            if (!is_buffer_empty()) {
                co_return;
            }
            if (!_remote_state) {
                if (_shard == this_shard_id()) {
                    _remote_state = create_remote_state(_db.local(), _schema, _underlying_schema, _permit, _dk, _ps, _ts);
                } else {
                    auto gs = global_schema_ptr(_schema);
                    auto gus = global_schema_ptr(_underlying_schema);
                    auto gts = tracing::global_trace_state_ptr(_ts);
                    auto timeout = _permit.timeout();
                    _remote_state = co_await _db.invoke_on(_shard, [gs = std::move(gs), gus = std::move(gus), gts = std::move(gts), &dk = _dk, &ps = _ps, timeout] (replica::database& db) {
                        auto output_schema = gs.get();
                        auto underlying_schema = gus.get();
                        auto ts = gts.get();
                        return db.obtain_reader_permit(underlying_schema, "data-source-remote-read", timeout, {}).then(
                                [&db, output_schema = std::move(output_schema), underlying_schema, ts = std::move(ts), &dk, &ps] (reader_permit permit) mutable {
                            return create_remote_state(db, std::move(output_schema), std::move(underlying_schema), std::move(permit), dk, ps, std::move(ts));
                        });
                    });
                }
            }
            if (!_partition_start_emitted) {
                push_mutation_fragment(*_schema, _permit, partition_start(_dk, {}));
                _partition_start_emitted = true;
            }
            co_await _db.invoke_on(_shard, [this] (replica::database&) {
                return fill_remote_buffer(*_remote_state, max_buffer_size_in_bytes, _underlying_pr, _underlying_ps);
            });
            auto cmp = clustering_key::prefix_equal_tri_compare(*_schema);
            for (const auto& remote_mf : _remote_state->buf) {
                if (!remote_mf.is_clustering_row()) {
                    push_mutation_fragment(*_schema, _permit, remote_mf);
                } else if (std::ranges::any_of(_ps.row_ranges(*_schema, _dk.key()), [&] (const query::clustering_range& cr) { return cr.contains(remote_mf.position().key(), cmp); })) {
                    // Translating the row ranges is best-effort, so we have a post-filtering stage here to drop any excess rows.
                    push_mutation_fragment(*_schema, _permit, remote_mf);
                }
            }
            _end_of_stream = _remote_state->mutation_sources.empty();
            if (_end_of_stream && !_partition_end_emitted) {
                push_mutation_fragment(*_schema, _permit, partition_end{});
                _partition_end_emitted = true;
            }
        }

        virtual future<> next_partition() override { throw std::bad_function_call(); }
        virtual future<> fast_forward_to(const dht::partition_range&) override { throw std::bad_function_call(); }
        virtual future<> fast_forward_to(position_range) override { throw std::bad_function_call(); }
        virtual future<> close() noexcept override {
            if (_remote_state) {
                return smp::submit_to(_shard, [rs = std::exchange(_remote_state, {})] () mutable -> future<> {
                    if (rs->reader) {
                        return rs->reader->close();
                    }
                    rs.release();
                    return make_ready_future<>();
                });
            }
            return make_ready_future<>();
        }
    };

private:
    static data_type get_underlying_key_type() {
        return list_type_impl::get_instance(utf8_type, false);
    }

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "mutation_dump");
        return schema_builder(system_keyspace::NAME, "mutation_dump", std::make_optional(id))
            .with_column("keyspace_name", utf8_type, column_kind::partition_key)
            .with_column("table_name", utf8_type, column_kind::partition_key)
            .with_column("partition_key", get_underlying_key_type(), column_kind::partition_key)
            .with_column("source", utf8_type, column_kind::clustering_key)
            .with_column("partition_region", byte_type, column_kind::clustering_key)
            .with_column("clustering_key", get_underlying_key_type(), column_kind::clustering_key)
            .with_column("position_weight", byte_type, column_kind::clustering_key)
            .with_column("kind", utf8_type)
            .with_column("value", utf8_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    mutation_source as_mutation_source() override {
        return mutation_source([this] (
                    schema_ptr schema,
                    reader_permit permit,
                    const dht::partition_range& pr,
                    const query::partition_slice& slice,
                    tracing::trace_state_ptr ts,
                    streamed_mutation::forwarding,
                    mutation_reader::forwarding) {
            if (!pr.is_singular()) {
                throw std::runtime_error("only single partition queries are supported");
            }
            if (slice.is_reversed()) {
                throw std::runtime_error("reverse reads are not supported");
            }

            const auto& rp = pr.start()->value();
            if (dht::shard_of(*schema, rp.token()) != this_shard_id()) {
                return make_empty_flat_reader_v2(std::move(schema), std::move(permit));
            }

            return make_flat_mutation_reader_v2<reader>(std::move(schema), std::move(permit), _db, rp.as_decorated_key(), slice, std::move(ts));
        });
    }

public:
    mutation_dump_table(distributed<replica::database>& db)
            : virtual_table(build_schema())
            , _db(db)
    {
        _shard_aware = true;
    }
};

}

// Map from table's schema ID to table itself. Helps avoiding accidental duplication.
static thread_local std::map<table_id, std::unique_ptr<virtual_table>> virtual_tables;

// Precondition: `register_virtual_tables` has finished.
std::vector<schema_ptr> all_virtual_tables() {
    std::vector<schema_ptr> r;
    r.reserve(virtual_tables.size());
    for (const auto& [_, v] : virtual_tables) {
        r.push_back(v->schema());
    }
    return r;
}

void register_virtual_tables(distributed<replica::database>& dist_db, distributed<service::storage_service>& dist_ss, sharded<gms::gossiper>& dist_gossiper, sharded<service::raft_group_registry>& dist_raft_gr, db::config& cfg) {
    auto add_table = [] (std::unique_ptr<virtual_table>&& tbl) {
        virtual_tables[tbl->schema()->id()] = std::move(tbl);
    };

    auto& db = dist_db.local();
    auto& ss = dist_ss.local();
    auto& gossiper = dist_gossiper.local();

    // Add built-in virtual tables here.
    add_table(std::make_unique<cluster_status_table>(ss, gossiper));
    add_table(std::make_unique<token_ring_table>(db, ss));
    add_table(std::make_unique<snapshots_table>(dist_db));
    add_table(std::make_unique<protocol_servers_table>(ss));
    add_table(std::make_unique<runtime_info_table>(dist_db, ss));
    add_table(std::make_unique<versions_table>());
    add_table(std::make_unique<db_config_table>(cfg));
    add_table(std::make_unique<clients_table>(ss));
    add_table(std::make_unique<raft_state_table>(dist_raft_gr));
    add_table(std::make_unique<mutation_dump_table>(dist_db));
}

void install_virtual_readers(db::system_keyspace& sys_ks, replica::database& db) {
    db.find_column_family(system_keyspace::size_estimates()).set_virtual_reader(mutation_source(db::size_estimates::virtual_reader(db, sys_ks)));
    db.find_column_family(system_keyspace::v3::views_builds_in_progress()).set_virtual_reader(mutation_source(db::view::build_progress_virtual_reader(db)));
    db.find_column_family(system_keyspace::built_indexes()).set_virtual_reader(mutation_source(db::index::built_indexes_virtual_reader(db)));

    for (auto&& [id, vt] : virtual_tables) {
        auto&& cf = db.find_column_family(vt->schema());
        cf.set_virtual_reader(vt->as_mutation_source());
        cf.set_virtual_writer([&vt = *vt] (const frozen_mutation& m) { return vt.apply(m); });
    }
}

} // namespace db
