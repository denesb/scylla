/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <map>
#include <utility>
#include <vector>

#include <fmt/ranges.h>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "db/cluster_config_manager.hh"
#include "exceptions/exceptions.hh"
#include "replica/database.hh"
#include "replica/problem_reads.hh"
#include "test/lib/cql_test_env.hh"
#include "transport/messages/result_message.hh"
#include "types/map.hh"
#include "types/types.hh"

BOOST_AUTO_TEST_SUITE(problem_reads_test)

using namespace std::chrono_literals;
using replica::problem_read_sample;
using replica::problem_read_symptom;
using replica::problem_reads;

namespace {

const auto t0 = problem_reads::clock_type::time_point(std::chrono::hours(1));

problem_read_sample make_sample(table_id table, problem_read_symptom symptom, sstring service_level = "default", uint64_t row_limit = 0) {
    problem_read_sample sample;
    sample.table = table;
    sample.symptom = symptom;
    sample.service_level = std::move(service_level);
    sample.row_limit = row_limit;
    return sample;
}

}

// At most one sample is recorded per symptom per period; each symptom has its own period.
SEASTAR_THREAD_TEST_CASE(test_problem_reads_rate_limits_per_symptom) {
    problem_reads pr;
    pr.set_config({.capacity = 8, .sample_period = 1s, .ttl = 1h});
    const auto t = table_id::create_random_id();

    BOOST_REQUIRE(pr.should_sample(problem_read_symptom::slow, t0));
    pr.record(make_sample(t, problem_read_symptom::slow), t0);
    BOOST_REQUIRE(!pr.should_sample(problem_read_symptom::slow, t0));
    BOOST_REQUIRE(!pr.should_sample(problem_read_symptom::slow, t0 + 999ms));
    BOOST_REQUIRE(pr.should_sample(problem_read_symptom::slow, t0 + 1s));
    BOOST_REQUIRE(pr.should_sample(problem_read_symptom::tombstones, t0));
}

// A sample for a key already in the list replaces it and moves it to the front; the key is
// (table, symptom, service level).
SEASTAR_THREAD_TEST_CASE(test_problem_reads_latest_sample_wins) {
    problem_reads pr;
    pr.set_config({.capacity = 8, .sample_period = 1s, .ttl = 1h});
    const auto t1 = table_id::create_random_id();
    const auto t2 = table_id::create_random_id();

    pr.record(make_sample(t1, problem_read_symptom::slow, "default", 1), t0);
    pr.record(make_sample(t2, problem_read_symptom::slow, "default", 2), t0 + 1s);
    pr.record(make_sample(t1, problem_read_symptom::slow, "sl2", 3), t0 + 2s);
    pr.record(make_sample(t1, problem_read_symptom::tombstones, "default", 4), t0 + 3s);
    BOOST_REQUIRE_EQUAL(pr.size(), 4);

    pr.record(make_sample(t1, problem_read_symptom::slow, "default", 5), t0 + 4s);
    BOOST_REQUIRE_EQUAL(pr.size(), 4);
    const auto samples = pr.samples();
    BOOST_REQUIRE_EQUAL(samples.front().row_limit, 5);
    BOOST_REQUIRE_EQUAL(samples[1].row_limit, 4);
    BOOST_REQUIRE_EQUAL(samples[2].row_limit, 3);
    BOOST_REQUIRE_EQUAL(samples[3].row_limit, 2);
}

// When the list is full, a new key evicts the least recently recorded sample.
SEASTAR_THREAD_TEST_CASE(test_problem_reads_evicts_least_recently_recorded) {
    problem_reads pr;
    pr.set_config({.capacity = 2, .sample_period = 1s, .ttl = 1h});
    const auto t1 = table_id::create_random_id();
    const auto t2 = table_id::create_random_id();
    const auto t3 = table_id::create_random_id();

    pr.record(make_sample(t1, problem_read_symptom::slow), t0);
    pr.record(make_sample(t2, problem_read_symptom::slow), t0 + 1s);
    // Refreshes t1, so t2 becomes the least recently recorded.
    pr.record(make_sample(t1, problem_read_symptom::slow), t0 + 2s);
    pr.record(make_sample(t3, problem_read_symptom::slow), t0 + 3s);

    const auto samples = pr.samples();
    BOOST_REQUIRE_EQUAL(samples.size(), 2);
    BOOST_REQUIRE(samples[0].table == t3);
    BOOST_REQUIRE(samples[1].table == t1);

    // Shrinking the capacity drops the oldest samples.
    pr.set_config({.capacity = 1, .sample_period = 1s, .ttl = 1h});
    BOOST_REQUIRE_EQUAL(pr.size(), 1);
    BOOST_REQUIRE(pr.samples()[0].table == t3);
}

SEASTAR_THREAD_TEST_CASE(test_problem_reads_expires_samples) {
    problem_reads pr;
    pr.set_config({.capacity = 8, .sample_period = 1s, .ttl = 10s});
    const auto t1 = table_id::create_random_id();
    const auto t2 = table_id::create_random_id();

    pr.record(make_sample(t1, problem_read_symptom::slow), t0);
    pr.record(make_sample(t2, problem_read_symptom::slow), t0 + 5s);

    pr.expire(t0 + 9s);
    BOOST_REQUIRE_EQUAL(pr.size(), 2);
    pr.expire(t0 + 10s);
    BOOST_REQUIRE_EQUAL(pr.size(), 1);
    BOOST_REQUIRE(pr.samples()[0].table == t2);
    pr.expire(t0 + 15s);
    BOOST_REQUIRE_EQUAL(pr.size(), 0);

    // A TTL of 0 disables expiry.
    pr.set_config({.capacity = 8, .sample_period = 1s, .ttl = 0s});
    pr.record(make_sample(t1, problem_read_symptom::slow), t0);
    pr.expire(t0 + 24h);
    BOOST_REQUIRE_EQUAL(pr.size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_problem_reads_zero_capacity_disables_sampling) {
    problem_reads pr;
    pr.set_config({.capacity = 0, .sample_period = 1s, .ttl = 1h});
    BOOST_REQUIRE(!pr.should_sample(problem_read_symptom::slow, t0));
}

namespace {

// The rows of system.problem_reads for ks.tbl, as (symptom, column -> value) pairs, with the
// values of the requested columns rendered as text.
std::vector<std::pair<sstring, std::map<sstring, sstring>>> problem_read_rows(cql_test_env& e, std::string_view ks, std::string_view tbl,
        const std::vector<sstring>& columns) {
    auto msg = e.execute_cql(fmt::format("SELECT symptom, {} FROM system.problem_reads WHERE keyspace_name = '{}' AND table_name = '{}'",
            fmt::join(columns, ", "), ks, tbl)).get();
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    BOOST_REQUIRE(rows);
    const auto& metadata = rows->rs().get_metadata().get_names();
    std::vector<std::pair<sstring, std::map<sstring, sstring>>> result;
    for (const auto& row : rows->rs().result_set().rows()) {
        // A partition with no samples has only its static row, the counters.
        if (!row[0]) {
            continue;
        }
        auto symptom = value_cast<sstring>(utf8_type->deserialize(*row[0]));
        std::map<sstring, sstring> values;
        for (size_t i = 1; i < row.size(); ++i) {
            values.emplace(columns[i - 1], row[i] ? metadata[i]->type->to_string(to_bytes(*row[i])) : sstring("null"));
        }
        result.emplace_back(std::move(symptom), std::move(values));
    }
    return result;
}

const std::map<sstring, sstring>* find_symptom(const std::vector<std::pair<sstring, std::map<sstring, sstring>>>& rows, std::string_view symptom) {
    auto it = std::ranges::find(rows, symptom, &std::pair<sstring, std::map<sstring, sstring>>::first);
    return it == rows.end() ? nullptr : &it->second;
}

// Runs a read the way a user read runs: in the statement scheduling group. Reads in other
// groups are internal and are not sampled.
void user_read(cql_test_env& e, std::string_view query) {
    with_scheduling_group(get_scheduling_groups().get().statement_scheduling_group, [&] {
        return e.execute_cql(query).discard_result();
    }).get();
}

void set_thresholds(cql_test_env& e, std::string_view ks, std::string_view tbl, replica::problem_read_thresholds thresholds) {
    e.db().invoke_on_all([&] (replica::database& db) {
        db.find_column_family(ks, tbl).set_problem_read_thresholds(thresholds);
    }).get();
}

}

// An unpaged read that may return more than one row is sampled, and shows up in the virtual
// table with the details of the read. A single-row unpaged read is fine and is not sampled.
SEASTAR_TEST_CASE(test_problem_reads_samples_unpaged_read) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))").get();
        e.execute_cql("INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 0, 0)").get();
        e.execute_cql("INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 1, 1)").get();

        // cql_test_env reads are unpaged.
        user_read(e, "SELECT * FROM ks.tbl WHERE pk = 0 AND ck = 0");
        BOOST_REQUIRE(!find_symptom(problem_read_rows(e, "ks", "tbl", {"paged"}), "unpaged"));

        // A partition of a table without clustering columns has a single row.
        e.execute_cql("CREATE TABLE ks.single_row (pk int PRIMARY KEY, v int)").get();
        e.execute_cql("INSERT INTO ks.single_row (pk, v) VALUES (0, 0)").get();
        user_read(e, "SELECT * FROM ks.single_row WHERE pk = 0");
        BOOST_REQUIRE(!find_symptom(problem_read_rows(e, "ks", "single_row", {"paged"}), "unpaged"));

        user_read(e, "SELECT * FROM ks.tbl WHERE pk = 0");
        const auto rows = problem_read_rows(e, "ks", "tbl", {"paged", "read_kind", "partition_range_count", "result_rows", "columns", "live_rows"});
        const auto* row = find_symptom(rows, "unpaged");
        BOOST_REQUIRE(row);
        BOOST_REQUIRE_EQUAL(row->at("paged"), "false");
        BOOST_REQUIRE_EQUAL(row->at("read_kind"), "data");
        BOOST_REQUIRE_EQUAL(row->at("partition_range_count"), "1");
        BOOST_REQUIRE_EQUAL(row->at("result_rows"), "2");
        BOOST_REQUIRE_EQUAL(row->at("live_rows"), "2");
        BOOST_REQUIRE_EQUAL(row->at("columns"), "v");
    });
}

// A read that goes through many tombstones is sampled with the counts of what it read and
// the threshold it was compared to.
SEASTAR_TEST_CASE(test_problem_reads_samples_tombstone_heavy_read) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))").get();
        set_thresholds(e, "ks", "tbl", replica::problem_read_thresholds{.tombstones = 5});
        for (int ck = 0; ck < 10; ++ck) {
            e.execute_cql(fmt::format("DELETE FROM ks.tbl WHERE pk = 0 AND ck = {}", ck)).get();
        }
        e.execute_cql("INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 100, 0)").get();

        user_read(e, "SELECT * FROM ks.tbl WHERE pk = 0");
        const auto rows = problem_read_rows(e, "ks", "tbl", {"dead_rows", "live_rows", "threshold", "threshold_unit"});
        const auto* row = find_symptom(rows, "tombstones");
        BOOST_REQUIRE(row);
        BOOST_REQUIRE_EQUAL(row->at("dead_rows"), "10");
        BOOST_REQUIRE_EQUAL(row->at("live_rows"), "1");
        BOOST_REQUIRE_EQUAL(row->at("threshold"), "5");
        BOOST_REQUIRE_EQUAL(row->at("threshold_unit"), "tombstones");
    });
}

// Disabling a table's sampling, or a symptom, stops samples from being recorded for it.
// The per-table counters count every read and every occurrence of a symptom, also the ones
// the rate limit does not sample.
SEASTAR_TEST_CASE(test_problem_reads_counts_every_occurrence) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))").get();
        e.execute_cql("INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 0, 0)").get();
        e.execute_cql("INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 1, 1)").get();

        user_read(e, "SELECT * FROM ks.tbl WHERE pk = 0 AND ck = 0");
        for (int i = 0; i < 3; ++i) {
            user_read(e, "SELECT * FROM ks.tbl WHERE pk = 0");
        }

        auto msg = e.execute_cql("SELECT reads, symptom_counts, counted_since FROM system.problem_reads WHERE keyspace_name = 'ks' AND table_name = 'tbl'").get();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);
        BOOST_REQUIRE(!rows->rs().result_set().rows().empty());
        const auto& row = rows->rs().result_set().rows().front();
        BOOST_REQUIRE_EQUAL(value_cast<int64_t>(long_type->deserialize(*row[0])), 4);
        const auto map_type = map_type_impl::get_instance(utf8_type, long_type, false);
        const auto counts = value_cast<map_type_impl::native_type>(map_type->deserialize(*row[1]));
        BOOST_REQUIRE_EQUAL(counts.size(), 1);
        BOOST_REQUIRE_EQUAL(value_cast<sstring>(counts[0].first), "unpaged");
        BOOST_REQUIRE_EQUAL(value_cast<int64_t>(counts[0].second), 3);
        BOOST_REQUIRE(row[2]);

        // Only one of the three was sampled.
        const auto samples = problem_read_rows(e, "ks", "tbl", {"paged"});
        BOOST_REQUIRE_EQUAL(std::ranges::count(samples, sstring("unpaged"), &std::pair<sstring, std::map<sstring, sstring>>::first), 1);
    });
}

SEASTAR_TEST_CASE(test_problem_reads_respects_disabled_symptoms) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))").get();
        e.execute_cql("INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 0, 0)").get();
        e.execute_cql("INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 1, 1)").get();

        replica::problem_read_thresholds thresholds;
        thresholds.symptoms[size_t(replica::problem_read_symptom::unpaged)] = false;
        set_thresholds(e, "ks", "tbl", thresholds);
        user_read(e, "SELECT * FROM ks.tbl WHERE pk = 0");
        BOOST_REQUIRE(!find_symptom(problem_read_rows(e, "ks", "tbl", {"paged"}), "unpaged"));
    });
}

// The problem_reads option sets a table's thresholds, problem_reads_sampling the node's sample
// list; both are applied live.
SEASTAR_TEST_CASE(test_problem_reads_cluster_config) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))").get();

        auto thresholds = [&] {
            return e.db().local().find_column_family("ks", "tbl").get_problem_read_thresholds();
        };
        BOOST_REQUIRE_EQUAL(thresholds().tombstones, 1000);
        // The per-symptom switch, regardless of problem_reads_enabled.
        auto switched_on = [&] (replica::problem_read_symptom symptom) {
            return thresholds().symptoms[size_t(symptom)];
        };

        e.execute_cql("ALTER KEYSPACE ks WITH problem_reads_thresholds = {'tombstones': '20', 'slow': '50'} AND problem_reads_enabled = false").get();
        e.execute_cql("ALTER TABLE ks.tbl WITH problem_reads_thresholds = {'tombstones': '5', 'slow': '0', 'unpaged': 'false'}").get();
        e.cluster_config_manager().local().refresh().get();
        BOOST_REQUIRE_EQUAL(thresholds().tombstones, 5);
        BOOST_REQUIRE(switched_on(replica::problem_read_symptom::tombstones));
        // A threshold of 0 switches its symptom off; a symptom without a threshold has a switch.
        BOOST_REQUIRE(!switched_on(replica::problem_read_symptom::slow));
        BOOST_REQUIRE(!switched_on(replica::problem_read_symptom::unpaged));
        // The table's map replaces the keyspace's as a whole: keys it does not set take their defaults.
        BOOST_REQUIRE_EQUAL(thresholds().read_amplification_sstables, 10);
        // Options are resolved independently: the table inherits the keyspace's problem_reads_enabled.
        BOOST_REQUIRE(!thresholds().enabled);

        e.execute_cql("ALTER TABLE ks.tbl WITH problem_reads_thresholds = NULL").get();
        e.cluster_config_manager().local().refresh().get();
        BOOST_REQUIRE_EQUAL(thresholds().tombstones, 20);
        BOOST_REQUIRE(thresholds().slow_floor == std::chrono::milliseconds(50));
        BOOST_REQUIRE(switched_on(replica::problem_read_symptom::slow));
        BOOST_REQUIRE(switched_on(replica::problem_read_symptom::unpaged));

        BOOST_REQUIRE_THROW(e.execute_cql("ALTER TABLE ks.tbl WITH problem_reads_thresholds = {'no_such_symptom': '1'}").get(), exceptions::configuration_exception);

        e.execute_cql("ALTER KEYSPACE ks WITH problem_reads_thresholds = NULL AND problem_reads_enabled = NULL").get();
        e.cluster_config_manager().local().refresh().get();
        BOOST_REQUIRE_EQUAL(thresholds().tombstones, 1000);
        BOOST_REQUIRE(thresholds().slow_floor == std::chrono::milliseconds(10));
        BOOST_REQUIRE(thresholds().enabled);

        e.execute_cql("ALTER CLUSTER WITH problem_reads_sampling = {'sample_period_ms': '500', 'ttl_seconds': '0'}").get();
        e.cluster_config_manager().local().refresh().get();
        e.db().invoke_on_all([] (replica::database& db) {
            BOOST_REQUIRE(db.get_problem_reads().get_config().sample_period == std::chrono::milliseconds(500));
            BOOST_REQUIRE(db.get_problem_reads().get_config().ttl == std::chrono::seconds(0));
        }).get();
        e.execute_cql("ALTER CLUSTER WITH problem_reads_sampling = NULL").get();

        e.execute_cql("ALTER CLUSTER WITH problem_reads_capacity = 0").get();
        e.cluster_config_manager().local().refresh().get();
        e.db().invoke_on_all([] (replica::database& db) {
            BOOST_REQUIRE_EQUAL(db.get_problem_reads().get_config().capacity, 0);
            BOOST_REQUIRE(!db.get_problem_reads().can_sample_any());
        }).get();

        e.execute_cql("ALTER CLUSTER WITH problem_reads_capacity = NULL").get();
        e.cluster_config_manager().local().refresh().get();
        e.db().invoke_on_all([] (replica::database& db) {
            BOOST_REQUIRE_EQUAL(db.get_problem_reads().get_config().capacity, 32);
        }).get();
    });
}

BOOST_AUTO_TEST_SUITE_END()
