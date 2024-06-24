/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/irange.hpp>
#include <fmt/ranges.h>
#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>

#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "replica/database.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/perf/perf.hh"

logging::logger plclog("perf_large_collection");

namespace bpo = boost::program_options;

dht::decorated_key make_pkey(const schema& s, int32_t pk) {
    auto key = partition_key::from_single_value(s, serialized(pk));
    return dht::decorate_key(s, key);
}

cql3::raw_value make_raw(int32_t v) {
    return cql3::raw_value::make_value(serialized(v));
}

struct prepared_partition_key {
    int32_t value;
    dht::decorated_key key;
    cql3::raw_value prepared_key;
    int32_t last_map_key = 0;

    prepared_partition_key(const schema& s, int32_t pk)
        : value(pk)
        , key(make_pkey(s, pk))
        , prepared_key(make_raw(pk))
    { }

    cql3::raw_value next_map_key() {
        return make_raw(last_map_key++);
    }
};

void print_stats(const char* prefix, uint64_t ops, const utils::estimated_histogram& latencies_hist) {
    plclog.info("{}{} ops | latencies mean {:>3.2f}, min: {:>3.2f}, max: {:>3.2f}, 50%: {:>3.2f}, 90%: {:>3.2f}, 99%: {:>3.2f}, 99.9%: {:>3.2f} [ms]",
        prefix,
        ops,
        latencies_hist.mean() / 1000.0,
        latencies_hist.min() / 1000.0,
        latencies_hist.max() / 1000.0,
        latencies_hist.percentile(0.5) / 1000.0,
        latencies_hist.percentile(0.9) / 1000.0,
        latencies_hist.percentile(0.99) / 1000.0,
        latencies_hist.percentile(0.999) / 1000.0);
}

std::vector<prepared_partition_key> write_data(const bpo::variables_map& app_cfg, cql_test_env& env, schema_ptr s, std::mt19937& engine) {
    std::vector<prepared_partition_key> partitions;

    // Check if we have a previous data dir and load it instead if not empty.
    if (app_cfg.count("data-dir")) {
        //TODO
    }

    const auto partition_count = app_cfg["partition-count"].as<size_t>();
    const auto write_ops = app_cfg["write-ops"].as<size_t>();
    const auto live_dead_cell_ratio = app_cfg["live-dead-cell-ratio"].as<double>();
    const auto geometric_distribution_param = app_cfg["geometric-distribution-param"].as<double>();

    auto update_id = env.prepare("UPDATE ks.tbl SET v[?] = ? WHERE pk = ?").get();
    auto delete_id = env.prepare("DELETE v[?] FROM ks.tbl WHERE pk = ?").get();

    partitions.reserve(partition_count);
    for (size_t i = 0; i < partition_count; ++i) {
        auto& partition = partitions.emplace_back(*s, tests::random::get_int<int32_t>(engine));

        // Make sure all partitions have at least one cell.
        env.execute_prepared(update_id, {partition.prepared_key, partition.next_map_key(), make_raw(0)}).get();
    }

    // We want a distribution which will heavily favor a subset of
    // the keys, producing a few huge partitions, leaving most of
    // them small.
    std::geometric_distribution<size_t> pk_index_dist{geometric_distribution_param};

    const auto live_threshold = size_t(100* live_dead_cell_ratio);
    std::uniform_int_distribution<size_t> live_dead_distribution{0, 100};
    size_t i = 0;

    time_parallel([&] () -> future<> {
        ++i;
        const auto pk_index = std::clamp(pk_index_dist(engine), size_t(0), partitions.size() - 1);
        const auto is_live = live_dead_distribution(engine) <= live_threshold;

        auto& partition = partitions.at(pk_index);

        if (is_live) {
            return env.execute_prepared(update_id, {partition.next_map_key(), make_raw(i), partition.prepared_key}).discard_result();
        } else {
            return env.execute_prepared(delete_id, {partition.next_map_key(), partition.prepared_key}).discard_result();
        }
    }, 100, 0, write_ops, true);

    utils::estimated_histogram cells_hist;
    for (const auto& p : partitions) {
        cells_hist.add(p.last_map_key);
    }
    std::vector<sstring> lines;
    lines.push_back("quantile      cells");
    for (float p = 0.1; p <= 1.0; p += 0.1) {
        lines.push_back(fmt::format("{:>6.3f} {:>10d}", p, cells_hist.percentile(p)));
    }
    lines.push_back(fmt::format("{:>6.3f} {:>10d}", 0.99, cells_hist.percentile(0.99)));
    lines.push_back(fmt::format("{:>6.3f} {:>10d}", 0.999, cells_hist.percentile(0.999)));
    plclog.info("cell count histogram:\n{}\n\nChange by tweaking --partition-count, --write-ops and --geometric-distribution-param.", fmt::join(lines, "\n"));

    return partitions;
}

int main(int argc, char** argv) {
    app_template app;
    app.add_options()
        ("workdir", bpo::value<sstring>(), "Work directory to use, can be the one from a previous run; write-phase only runs if the workdir is empty")
        ("concurrency", bpo::value<unsigned>()->default_value(100), "Read concurrency")
        ("duration", bpo::value<unsigned>()->default_value(60), "Duration [s] after which the test terminates with a success")
        ("operations-per-shard", bpo::value<unsigned>(), "Operations to execute after which the test terminates with a success (overrides duration)")
        ("stop-on-error", "Stop test on first error?")
        ("random-seed", bpo::value<uint32_t>(), "Random seed for the random number generator")

        ("partition-count", bpo::value<size_t>()->default_value(1000), "Number of partitions in the data-set")
        ("write-ops", bpo::value<size_t>()->default_value(10000), "Number of write operations to perform (either update or delete, see live-dead-cell-ratio)")
        ("live-dead-cell-ratio", bpo::value<double>()->default_value(0.5), "Percentage of the total cells that is live, expressed as a number between [0.0, 1.0]")
        ("geometric-distribution-param", bpo::value<double>()->default_value(0.5), "The control param of the geometric distribution, which selects the partition-key to update, "
                "see https://en.cppreference.com/w/cpp/numeric/random/geometric_distribution")

        ("reader-concurrency-semaphore-cpu-concurrency", bpo::value<uint32_t>()->default_value(1), "Admit new reads while there are less than this number of requests that need CPU")
        ;

    return app.run(argc, argv, [&app] {
        const auto& app_cfg = app.configuration();

        auto test_cfg = cql_test_config{};
        auto& cfg = *test_cfg.db_config;

        if (app_cfg.count("workdir")) {
            test_cfg.workdir_path = std::filesystem::path(app_cfg["workdir"].as<sstring>());
        }

        cfg.reader_concurrency_semaphore_cpu_concurrency.set(uint32_t(app_cfg["reader-concurrency-semaphore-cpu-concurrency"].as<uint32_t>()));
        cfg.enable_commitlog(false);
        cfg.enable_cache(true);

        return do_with_cql_env_thread([&app_cfg] (cql_test_env& env) {
            const auto concurrency = app_cfg["concurrency"].as<unsigned>();
            const auto duration_in_seconds = app_cfg["duration"].as<unsigned>();
            const auto operations_per_shard = app_cfg.count("operations-per-shard") ? app_cfg["operations-per-shard"].as<unsigned>() : 0u;
            const auto stop_on_error = bool(app_cfg.count("stop-on-error"));

            env.execute_cql("CREATE TABLE ks.tbl (pk int PRIMARY KEY, v map<int, int>) WITH compaction = {'class': 'NullCompactionStrategy'}").get();
            auto s = env.local_db().find_schema("ks", "tbl");

            uint32_t seed = std::random_device()();
            if (app_cfg.count("random-seed")) {
                seed = app_cfg["random-seed"].as<uint32_t>();
            }
            plclog.info("random_seed: {}", seed);
            std::mt19937 engine(seed);

            const auto partitions = write_data(app_cfg, env, s, engine);

            uint64_t reads = 0;
            utils::estimated_histogram reads_hist;

            uint64_t total_reads = 0;
            utils::estimated_histogram total_reads_hist;

            timer<> stats_printer;
            stats_printer.set_callback([&] {
                print_stats("", reads, reads_hist);
                reads_hist.clear();
                reads = 0;
            });
            stats_printer.arm_periodic(1s);

            // read workload
            {
                using clock = std::chrono::steady_clock;

                std::uniform_int_distribution<size_t> pk_index_distribution{0, partitions.size() - 1};

                auto read_id = env.prepare("select * from ks.tbl where pk = ?;").get();

                time_parallel([&] () -> future<> {
                    const auto pk_index = pk_index_distribution(engine);
                    const auto& partition = partitions.at(pk_index);

                    auto t0 = clock::now();
                    co_await env.execute_prepared(read_id, {partition.prepared_key});
                    const auto latency = std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - t0).count();
                    reads_hist.add(latency);
                    total_reads_hist.add(latency);

                    ++total_reads;
                    ++reads;
                }, concurrency, duration_in_seconds, operations_per_shard, stop_on_error);
            }

            print_stats("Final stats: ", total_reads, total_reads_hist);

            plclog.info("semaphore diagnostics: {}", env.local_db().get_reader_concurrency_semaphore().dump_diagnostics());
        }, std::move(test_cfg));
    });
}
