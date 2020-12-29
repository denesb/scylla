/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#include <unordered_map>
#include <stdexcept>
#include <regex>
#include <tuple>

#include <boost/filesystem.hpp>

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <seastar/core/semaphore.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

#include "replicated_key_provider.hh"
#include "encryption.hh"
#include "local_file_provider.hh"
#include "symmetric_key.hh"
#include "database.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"
#include "utils/hash.hh"
#include "service/storage_service.hh"
#include "service/migration_manager.hh"
#include "sstables/compaction_manager.hh"
#include "distributed_loader.hh"
#include "schema_builder.hh"
#include "db/system_keyspace.hh"


namespace encryption {

static auto constexpr KSNAME = "system_replicated_keys";
static auto constexpr TABLENAME = "encrypted_keys";

static logger log("replicated_key_provider");

using utils::UUID;

class replicated_key_provider : public key_provider {
public:
    static constexpr int8_t version = 0;
    /**
     * Header:
     * 1 byte version
     * 16 bytes UUID of key
     * 16 bytes MD5 of UUID
     */
    static const size_t header_size = 33;

    struct key_id {
        key_info info;
        opt_bytes id;

        key_id(key_info k, opt_bytes b = {})
            : info(std::move(k))
            , id(std::move(b))
        {}
        bool operator==(const key_id& v) const {
            return info == v.info && id == v.id;
        }
    };

    struct key_id_hash {
        size_t operator()(const key_id& id) const {
            return utils::tuple_hash()(std::tie(id.info.alg, id.info.len, id.id));
        }
    };

    replicated_key_provider(encryption_context& ctxt, shared_ptr<system_key> system_key, shared_ptr<key_provider> local_provider)
        : _ctxt(ctxt)
        , _system_key(std::move(system_key))
        , _local_provider(std::move(local_provider))
    {}


    future<std::tuple<key_ptr, opt_bytes>> key(const key_info&, opt_bytes = {}) override;
    future<> validate() const override;
    future<> maybe_initialize_tables();

    bool should_delay_read(const opt_bytes& id) const override {
        if (!id || _initialized) {
            return false;
        }
        if (!_initialized) {
            return true;
        }
        auto& qp = _ctxt.get_query_processor();
        return !qp.local_is_initialized() || _ctxt.get_database().local().get_compaction_manager().enabled();
    }

    void print(std::ostream& os) const override {
        os << "system_key=" << _system_key->name() << ", local=" << *_local_provider;
    }

private:
    void store_key(const key_id&, const UUID&, key_ptr);
    opt_bytes decode_id(const opt_bytes&) const;

    future<std::tuple<UUID, key_ptr>> get_key(const key_info&, opt_bytes = {});

    future<key_ptr> load_or_create(const key_info&);
    future<key_ptr> load_or_create_local(const key_info&);
    future<> read_key_file();
    future<> write_key_file();

    template<typename... Args>
    future<::shared_ptr<cql3::untyped_result_set>> query(sstring, Args&& ...);

    future<> force_blocking_flush();

    encryption_context& _ctxt;
    shared_ptr<system_key> _system_key;
    shared_ptr<key_provider> _local_provider;
    std::unordered_map<key_id, std::pair<UUID, key_ptr>, key_id_hash> _keys;

    bool _initialized = false;
};

using namespace std::chrono_literals;

static const timeout_config rkp_db_timeout_config {
    5s, 5s, 5s, 5s, 5s, 5s, 5s,
};

template<typename... Args>
future<::shared_ptr<cql3::untyped_result_set>> replicated_key_provider::query(sstring q, Args&& ...params) {
    return _ctxt.get_storage_service().local().is_starting().then([this, t = std::make_tuple<sstring, Args...>(std::move(q), std::forward<Args>(params)...)](bool starting) {
        auto query_internal = [this](const sstring& q, auto&& ...params) {
            return _ctxt.get_query_processor().local().execute_internal(q, { (params)...});
        };
        auto query_normal = [this](const sstring& q, auto&& ...params) {
            return _ctxt.get_query_processor().local().execute_internal(q, db::consistency_level::ONE, rkp_db_timeout_config, { (params)...}, false);
        };
        return starting ? std::apply(query_internal, t) : std::apply(query_normal, t);
    });
}

future<> replicated_key_provider::force_blocking_flush() {
    return _ctxt.get_database().invoke_on_all([](database& db) {
        // if (!Boolean.getBoolean("cassandra.unsafesystem"))
        column_family& cf = db.find_column_family(KSNAME, TABLENAME);
        return cf.flush();
    });
}

void replicated_key_provider::store_key(const key_id& id, const UUID& uuid, key_ptr k) {
    _keys[id] = std::make_pair(uuid, k);
    if (!id.id) {
        _keys[key_id(id.info, uuid.serialize())] = std::make_pair(uuid, k);
    }
}

opt_bytes replicated_key_provider::decode_id(const opt_bytes& b) const {
    if (b) {
        auto i = b->begin();
        auto v = *i++;
        if (v == version && b->size() == 33) {
            bytes id(i + 1, i + 1 + 16);
            bytes md(i + 1 + 16, b->end());
            if (calculate_md5(id) == md) {
                return id;
            }
        }
    }
    return std::nullopt;
}

future<std::tuple<key_ptr, opt_bytes>> replicated_key_provider::key(const key_info& info, opt_bytes input) {
    opt_bytes id;

    if (input) { //reading header?
        auto v = *input;
        if (v[0] == version) {
            bytes bid(v.begin() + 1, v.begin() + 1 + 16);
            bytes md(v.begin() + 1 + 16, v.begin() + 1 + 32);
            if (calculate_md5(bid) == md) {
                id = bid;
            }
        }
    }

    auto gen_id = !input;

    return get_key(info, std::move(id)).then([gen_id](std::tuple<UUID, key_ptr> uuid_k) {
        auto&& [uuid, k] = uuid_k;
        opt_bytes id;
        if (gen_id) { // write case. need to give key id back
            bytes b{bytes::initialized_later(), header_size};
            auto i = b.begin();
            *i++ = version;
            uuid.serialize(i);
            auto md = calculate_md5(b, 1, 16);
            std::copy(md.begin(), md.end(), i);
            id = std::move(b);
        }
        return make_ready_future<std::tuple<key_ptr, opt_bytes>>(std::tuple(k, std::move(id)));
    }).handle_exception([this, info, input = std::move(input)](std::exception_ptr ep) {
        log.warn("Exception looking up key {}: {}", info, ep);
        if (_local_provider) {
            try {
                std::rethrow_exception(ep);
            } catch (no_such_keyspace&) {
            } catch (exceptions::invalid_request_exception&) {
            } catch (...) {
                throw;
            }
            log.warn("Falling back to local key {}", info);
            return _local_provider->key(info, input);
        }
        return make_exception_future<std::tuple<key_ptr, opt_bytes>>(ep);
    });
}

future<std::tuple<UUID, key_ptr>> replicated_key_provider::get_key(const key_info& info, opt_bytes opt_id) {
    key_id id(info, std::move(opt_id));
    auto i = _keys.find(id);
    if (i != _keys.end()) {
        return make_ready_future<std::tuple<UUID, key_ptr>>(std::tuple(i->second.first, i->second.second));
    }

    if (!_initialized) {
        return maybe_initialize_tables().then([this, id = std::move(id)] {
            return get_key(id.info, id.id);
        });
    }

    // TODO: origin does non-cql acquire of all available keys from
    // replicas in the "host_ids" table iff we get here during boot.
    // For now, ignore this and assume that if we have a sstable with
    // key X, we should have a local replica of X as well, given
    // the "everywhere strategy of the keys table.

    auto cipher = info.alg.substr(0, info.alg.find('/')); // e.g. "AES"


    UUID uuid;

    auto f = [&] {
        if (id.id) {
            uuid = utils::UUID_gen::get_UUID(*id.id);
            log.debug("Finding key {} ({})", uuid, info);
            auto s = sprint("SELECT * FROM %s.%s WHERE key_file=? AND cipher=? AND strength=? AND key_id=?;", KSNAME, TABLENAME);
            return query(std::move(s), _system_key->name(), cipher, int32_t(id.info.len), uuid);
        } else {
            log.debug("Finding key ({})", info);
            auto s = sprint("SELECT * FROM %s.%s WHERE key_file=? AND cipher=? AND strength=? LIMIT 1;", KSNAME, TABLENAME);
            return query(std::move(s), _system_key->name(), cipher, int32_t(id.info.len));
        }
    }();

    return f.then([this, id = std::move(id), cipher = std::move(cipher), uuid](shared_ptr<cql3::untyped_result_set> res) mutable {
        // if we find nothing, and we actually queried a specific key (by uuid), we've failed.
        if (res->empty() && id.id) {
            log.debug("Could not find key {}", id.id);
            return make_exception_future<std::tuple<UUID, key_ptr>>(std::runtime_error(sprint("Unable to find key for cipher=%s strength=%s id=%s", cipher, id.info.len, uuid)));
        }
        // otoh, if we don't need a specific key, we can just create a new one (writing a sstable)
        if (res->empty()) {
            uuid = utils::UUID_gen::get_time_UUID();

            log.debug("No key found. Generating {}", uuid);

            auto k = make_shared<symmetric_key>(id.info);
            store_key(id, uuid, k);
            return _system_key->encrypt(k->key()).then([this, id = std::move(id), cipher = std::move(cipher), uuid](bytes b) {
                auto ks = base64_encode(b);
                return query(sprint("INSERT INTO %s.%s (key_file, cipher, strength, key_id, key) VALUES (?, ?, ?, ?, ?)", KSNAME, TABLENAME)
                                , _system_key->name(), cipher, int32_t(id.info.len), uuid, ks
                ).then([this](auto&&) {
                    return force_blocking_flush();
                });
            }).then([k, uuid]() {
                return make_ready_future<std::tuple<UUID, key_ptr>>(std::tuple(uuid, k));
            });
        }
        // found it
        auto& row = res->one();
        uuid = row.get_as<UUID>("key_id");
        auto ks = row.get_as<sstring>("key");
        auto kb = base64_decode(ks);
        return _system_key->decrypt(kb).then([this, uuid, id = std::move(id)](bytes b) {
            auto k = make_shared<symmetric_key>(id.info, b);
            store_key(id, uuid, k);
            return make_ready_future<std::tuple<UUID, key_ptr>>(std::tuple(uuid, k));
        });
    });
}

future<> replicated_key_provider::validate() const {
    auto f = _system_key->validate().handle_exception([this](auto ep) {
        std::throw_with_nested(std::invalid_argument(sprint("Could not validate system key: %s (%s)", _system_key->name(), ep)));
    });
    if (_local_provider){
        f = f.then([this] {
            return _local_provider->validate();
        });
    }
    return f;
}

schema_ptr encrypted_keys_table() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(KSNAME, TABLENAME);
        return schema_builder(KSNAME, TABLENAME, std::make_optional(id))
                .with_column("key_file", utf8_type, column_kind::partition_key)
                .with_column("cipher", utf8_type, column_kind::partition_key)
                .with_column("strength", int32_type, column_kind::clustering_key)
                .with_column("key_id", timeuuid_type, column_kind::clustering_key)
                .with_column("key", utf8_type)
                .with_version(::db::system_keyspace::generate_schema_version(id))
                .build();
    }();
    return schema;
}

future<> replicated_key_provider::maybe_initialize_tables() {
    auto& db = _ctxt.get_database().local();

    if (db.has_schema(KSNAME, TABLENAME)) {
        return db.find_keyspace(KSNAME).ensure_populated().then([this] {
            _initialized = true;
        });
    }

    auto f = make_ready_future();
    auto& mm = _ctxt.get_migration_manager().local();
    static auto ignore_existing = [] (seastar::noncopyable_function<future<>()> func) {
        return futurize_invoke(std::move(func)).handle_exception_type([] (exceptions::already_exists_exception& ignored) { });
    };
    log.debug("Creating keyspace and table");
    if (!db.has_keyspace(KSNAME)) {

        f = ignore_existing([this, &mm] { // Create the keyspace if not exists
            auto ksm = keyspace_metadata::new_keyspace(
                    KSNAME,
                    "org.apache.cassandra.locator.EverywhereStrategy",
                    {},
                    true);
            return mm.announce_new_keyspace(ksm, api::min_timestamp);
        });

    }
    f = f.then([this, &mm] { // Then create the table
        return ignore_existing([this, &mm] {
            return mm.announce_new_column_family(encrypted_keys_table(), api::min_timestamp);
        });
    }).then([&] { // Then do some final stuff and mark this object as initialized
        auto& ks = db.find_keyspace(KSNAME);
        auto& rs = ks.get_replication_strategy();
        // should perhaps check name also..
        if (rs.get_type() != locator::replication_strategy_type::everywhere_topology) {
            // TODO: reset to everywhere + repair.
        }
        _initialized = true;
    });

    return f;
}

const size_t replicated_key_provider::header_size;

replicated_key_provider_factory::replicated_key_provider_factory()
{}

replicated_key_provider_factory::~replicated_key_provider_factory()
{}

namespace bfs = std::filesystem;

shared_ptr<key_provider> replicated_key_provider_factory::get_provider(encryption_context& ctxt, const options& map) {
    opt_wrapper opts(map);
    auto system_key_name = opts("system_key_file").value_or("system_key");
    if (system_key_name.find('/') != sstring::npos) {
        throw std::invalid_argument("system_key cannot contain '/'");
    }

    auto system_key = ctxt.get_system_key(system_key_name);
    auto local_key_file = bfs::absolute(bfs::path(opts("secret_key_file").value_or(default_key_file_path)));

    if (system_key->is_local() && bfs::absolute(bfs::path(system_key->name())) == local_key_file) {
        throw std::invalid_argument("system key and local key cannot be the same");
    }

    auto name = system_key->name() + ":" + local_key_file.string();
    auto p = ctxt.get_cached_provider(name);
    if (!p) {
        p = seastar::make_shared<replicated_key_provider>(ctxt, std::move(system_key), local_file_provider_factory::find(ctxt, local_key_file.string()));
        ctxt.cache_provider(name, p);
    }

    return p;
}

void replicated_key_provider_factory::init() {
    distributed_loader::mark_keyspace_as_load_prio(KSNAME);
}

}
