/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <list>
#include <optional>
#include <string_view>
#include <vector>

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

#include "db_clock.hh"
#include "query/query_id.hh"
#include "reader_permit.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

namespace replica {

// A known problem a read can exhibit. A read is checked against each symptom on the replica,
// and one that exhibits it may be sampled into problem_reads.
enum class problem_read_symptom : uint8_t {
    // Unpaged read whose result exceeded the unpaged-query soft memory limit, or that failed
    // on the hard limit. Checked before `unpaged`.
    unpaged_large,
    // Any other unpaged read that may need paging.
    unpaged,
    slow,
    // Timed out after admission.
    timed_out,
    admission_wait,
    tombstones,
    // The page was cut short by the tombstone limit.
    tombstone_limit_page,
    // Single-partition read that touched too many sstables.
    read_amplification,
    memory_peak,
    memory_amplification,
};

constexpr size_t problem_read_symptom_count = size_t(problem_read_symptom::memory_amplification) + 1;

std::string_view to_string(problem_read_symptom symptom);

// A bound of a partition or clustering range, as recorded in a sample. `value` holds the
// components of the key, each rendered as a CQL literal. For a partition range, `token` is
// always set and `value` is absent for a bound that has a token but no key. For a clustering
// range, `token` is absent and `value` holds the components of the clustering prefix, so it
// may be shorter than the clustering key.
struct problem_read_range_bound {
    std::optional<std::vector<sstring>> value;
    std::optional<int64_t> token;
    bool inclusive = false;
};

// A disengaged bound is unbounded.
struct problem_read_range {
    std::optional<problem_read_range_bound> start;
    std::optional<problem_read_range_bound> end;
};

// One sampled read, with every detail collected about it, whichever symptom triggered the
// sample. A detail the read did not produce is disengaged.
struct problem_read_sample {
    enum class read_kind : uint8_t { data, digest, mutation };
    enum class failure_kind : uint8_t { timeout, unpaged_hard_limit };
    enum class threshold_unit : uint8_t { us, tombstones, sstables, bytes, ratio };

    using read_counts = reader_permit::page_report::read_counts;

    // Key.
    table_id table;
    sstring keyspace_name;
    sstring table_name;
    problem_read_symptom symptom;
    sstring service_level;

    db_clock::time_point sampled_at;

    // The read.
    read_kind kind = read_kind::data;
    bool paged = false;
    query_id query_uuid;
    bool is_first_page = false;
    // At most max_ranges each; the total count of each is kept when the list was truncated.
    std::vector<problem_read_range> partition_ranges;
    uint64_t partition_range_count = 0;
    std::vector<problem_read_range> clustering_ranges;
    uint64_t clustering_range_count = 0;
    std::vector<sstring> columns;
    std::vector<sstring> slice_options;
    uint64_t row_limit = 0;
    uint64_t partition_limit = 0;
    uint64_t per_partition_limit = 0;

    // Outcome.
    std::optional<failure_kind> failure;
    bool tombstone_limit_reached = false;

    // Timing.
    std::optional<std::chrono::microseconds> latency;
    std::optional<std::chrono::microseconds> queue_wait;
    std::optional<std::chrono::microseconds> timeout;
    std::optional<std::chrono::microseconds> table_p50_latency;
    std::optional<std::chrono::microseconds> table_p99_latency;

    // Admission, if the read was queued.
    std::optional<reader_permit::wait_stats> admission;

    // Result.
    std::optional<uint64_t> result_partitions;
    std::optional<uint64_t> result_rows;
    std::optional<uint64_t> result_bytes;

    // What the page read.
    std::optional<read_counts> counts;

    // Resources.
    std::optional<uint64_t> sstables_read;
    std::optional<uint64_t> memory_peak;
    std::optional<uint64_t> memory_total;

    // What triggered the sample.
    std::optional<int64_t> threshold;
    std::optional<threshold_unit> unit;

    static constexpr size_t max_ranges = 16;
};

std::string_view to_string(problem_read_sample::read_kind);
std::string_view to_string(problem_read_sample::failure_kind);
std::string_view to_string(problem_read_sample::threshold_unit);
std::string_view to_string(reader_permit::wait_reason);

// A small, fixed-capacity, per-shard list of sample reads that exhibited a known problem.
//
// A sample is keyed by (table, symptom, service level). Recording a read for a key already in
// the list replaces the previous sample (latest wins); otherwise the least recently recorded
// sample is evicted when the list is full. Samples also expire after a TTL.
//
// The list holds samples, not statistics: rate limiting drops most occurrences of a symptom on
// purpose. At most one sample is recorded per symptom per sample period. Callers check
// should_sample() first, before evaluating the symptom at all, so a workload where every read
// is problematic costs a clock read and a comparison per read and symptom.
class problem_reads {
public:
    using clock_type = seastar::lowres_clock;

    struct config {
        // 0 disables sampling.
        size_t capacity = 32;
        std::chrono::milliseconds sample_period = std::chrono::seconds(1);
        // 0 disables expiry.
        std::chrono::seconds ttl = std::chrono::hours(1);
    };

private:
    struct entry {
        problem_read_sample sample;
        clock_type::time_point recorded_at;
    };

    config _config;
    // Most recently recorded first.
    std::list<entry> _entries;
    std::array<clock_type::time_point, problem_read_symptom_count> _next_sample_at{};
    timer<clock_type> _expiry_timer;

private:
    void arm_expiry_timer();

public:
    problem_reads();

    void set_config(config cfg);
    const config& get_config() const noexcept {
        return _config;
    }

    bool should_sample(problem_read_symptom symptom, clock_type::time_point now = clock_type::now()) const noexcept {
        return _config.capacity && now >= _next_sample_at[size_t(symptom)];
    }

    // Records a sample of a read that exhibited `sample.symptom`, and starts a new sample
    // period for the symptom. Call only after should_sample() returned true.
    void record(problem_read_sample sample, clock_type::time_point now = clock_type::now());

    // Removes the samples older than the TTL.
    void expire(clock_type::time_point now = clock_type::now());

    // The samples, most recently recorded first.
    std::vector<problem_read_sample> samples() const;

    size_t size() const noexcept {
        return _entries.size();
    }
};

}
