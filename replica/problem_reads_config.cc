/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/problem_reads_config.hh"

#include <algorithm>

#include "db/cluster_config_registry.hh"
#include "replica/database.hh"

namespace replica {

namespace {

namespace registry = db::cluster_config_registry;

const registry::option& find_option(std::string_view name) {
    const auto* opt = registry::find(name);
    if (!opt) {
        on_internal_error(dblog, fmt::format("cluster config option '{}' is not registered", name));
    }
    return *opt;
}

// Negative values are accepted by the registry's integer type but make no sense here; they
// read as 0, which disables the setting, like the configured 0 does.
uint64_t non_negative(int64_t v) {
    return uint64_t(std::max<int64_t>(v, 0));
}

// The symptoms switched by a threshold, and where their threshold goes; the rest are switched by
// a boolean.
void apply_threshold(problem_read_thresholds& th, problem_read_symptom symptom, uint64_t v) {
    switch (symptom) {
    case problem_read_symptom::slow: th.slow_floor = std::chrono::milliseconds(v); break;
    case problem_read_symptom::admission_wait: th.admission_wait_floor = std::chrono::milliseconds(v); break;
    case problem_read_symptom::tombstones: th.tombstones = v; break;
    case problem_read_symptom::read_amplification: th.read_amplification_sstables = v; break;
    case problem_read_symptom::memory_peak: th.memory_peak_percent = v; break;
    case problem_read_symptom::memory_amplification: th.memory_amplification_ratio = v; break;
    default: on_internal_error(dblog, fmt::format("problem read symptom {} has no threshold", to_string(symptom)));
    }
}

// Sets the symptom switches and thresholds of `th` from problem_reads_thresholds; leaves
// th.enabled, which is problem_reads_enabled, alone.
void apply_thresholds(problem_read_thresholds& th, const registry::option& opt, const registry::map_value& value) {
    const auto keys = std::get<std::span<const registry::map_key>>(opt.default_value);
    for (size_t i = 0; i < problem_read_symptom_count; ++i) {
        const auto symptom = problem_read_symptom(i);
        const auto name = to_string(symptom);
        const auto key = std::ranges::find(keys, name, &registry::map_key::name);
        if (key == keys.end()) {
            on_internal_error(dblog, fmt::format("problem_reads_thresholds has no key for symptom {}", name));
        }
        if (key->type() == registry::value_type::boolean) {
            th.symptoms[i] = value.get_boolean(name);
        } else {
            const auto v = non_negative(value.get_integer(name));
            th.symptoms[i] = v != 0;
            apply_threshold(th, symptom, v);
        }
    }
    th.memory_amplification_min_bytes = non_negative(value.get_integer(registry::problem_reads_thresholds_keys::memory_amplification_min_mb)) * 1024 * 1024;
}

// Calls `update` with the thresholds of the table of `ctx`, then sets them on the table.
template <typename Func>
void update_table(database& db, const db::cluster_config_manager::lookup_context& ctx, Func update) {
    if (!ctx.keyspace_name || !ctx.table_name) {
        return;
    }
    try {
        auto& t = db.find_column_family(*ctx.keyspace_name, *ctx.table_name);
        auto th = t.get_problem_read_thresholds();
        update(th);
        t.set_problem_read_thresholds(th);
    } catch (const data_dictionary::no_such_column_family&) {
        // Dropped since; nothing to configure.
    }
}

// Calls `update` with the shard's problem_reads config, then sets it.
template <typename Func>
void update_sampling(database& db, Func update) {
    auto cfg = db.get_problem_reads().get_config();
    update(cfg);
    db.get_problem_reads().set_config(cfg);
}

}

problem_reads_config::problem_reads_config(sharded<database>& sharded_db, sharded<db::cluster_config_manager>& mgr) {
    auto& db = sharded_db.local();
    auto reg = [&] (std::string_view name, auto apply) {
        _registrations.push_back(mgr.local().register_config_callback(sstring(name),
                [&db, &opt = find_option(name), apply] (const db::cluster_config_manager::lookup_context& ctx, std::optional<sstring> value) {
            apply(db, ctx, opt, value);
            return make_ready_future<>();
        }));
    };
    // Each option is resolved on its own, so each callback sets only the fields of its option.
    reg(registry::option_name::problem_reads_enabled, [] (database& db, const auto& ctx, const auto& opt, const auto& value) {
        update_table(db, ctx, [&] (problem_read_thresholds& th) { th.enabled = registry::to_boolean(opt, value); });
    });
    reg(registry::option_name::problem_reads_thresholds, [] (database& db, const auto& ctx, const auto& opt, const auto& value) {
        update_table(db, ctx, [&] (problem_read_thresholds& th) { apply_thresholds(th, opt, registry::to_map(opt, value)); });
    });
    reg(registry::option_name::problem_reads_capacity, [] (database& db, const auto&, const auto& opt, const auto& value) {
        update_sampling(db, [&] (problem_reads::config& cfg) { cfg.capacity = non_negative(registry::to_integer(opt, value)); });
    });
    reg(registry::option_name::problem_reads_sampling, [] (database& db, const auto&, const auto& opt, const auto& value) {
        update_sampling(db, [&] (problem_reads::config& cfg) {
            namespace keys = registry::problem_reads_sampling_keys;
            const auto map = registry::to_map(opt, value);
            cfg.sample_period = std::chrono::milliseconds(non_negative(map.get_integer(keys::sample_period_ms)));
            cfg.ttl = std::chrono::seconds(non_negative(map.get_integer(keys::ttl_seconds)));
        });
    });
}

future<> problem_reads_config::stop() {
    for (auto& r : _registrations) {
        r.unregister();
    }
    return make_ready_future<>();
}

}
