/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include <chrono>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/thread_test_case.hh>

#include "replica/problem_reads.hh"

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

BOOST_AUTO_TEST_SUITE_END()
