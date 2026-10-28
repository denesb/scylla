/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/problem_reads.hh"

#include <algorithm>

#include <seastar/core/scheduling.hh>
#include <seastar/core/timed_out_error.hh>

#include "dht/i_partitioner.hh"
#include "query/query-request.hh"
#include "query/query-result.hh"
#include "reader_concurrency_semaphore.hh"
#include "utils/exceptions.hh"
#include "utils/small_vector.hh"
#include "schema/schema.hh"

namespace replica {

std::string_view to_string(problem_read_symptom symptom) {
    switch (symptom) {
    case problem_read_symptom::unpaged_large: return "unpaged_large";
    case problem_read_symptom::unpaged: return "unpaged";
    case problem_read_symptom::slow: return "slow";
    case problem_read_symptom::timed_out: return "timed_out";
    case problem_read_symptom::admission_wait: return "admission_wait";
    case problem_read_symptom::tombstones: return "tombstones";
    case problem_read_symptom::tombstone_limit_page: return "tombstone_limit_page";
    case problem_read_symptom::read_amplification: return "read_amplification";
    case problem_read_symptom::memory_peak: return "memory_peak";
    case problem_read_symptom::memory_amplification: return "memory_amplification";
    }
    __builtin_unreachable();
}

problem_read_sample::read_kind to_read_kind(query::result_request request) noexcept {
    return request == query::result_request::only_digest ? problem_read_sample::read_kind::digest : problem_read_sample::read_kind::data;
}

std::string_view to_string(problem_read_sample::read_kind kind) {
    switch (kind) {
    case problem_read_sample::read_kind::data: return "data";
    case problem_read_sample::read_kind::digest: return "digest";
    case problem_read_sample::read_kind::mutation: return "mutation";
    }
    __builtin_unreachable();
}

std::string_view to_string(problem_read_sample::failure_kind kind) {
    switch (kind) {
    case problem_read_sample::failure_kind::timeout: return "timeout";
    case problem_read_sample::failure_kind::unpaged_hard_limit: return "unpaged_hard_limit";
    }
    __builtin_unreachable();
}

std::string_view to_string(problem_read_sample::threshold_unit unit) {
    switch (unit) {
    case problem_read_sample::threshold_unit::us: return "us";
    case problem_read_sample::threshold_unit::tombstones: return "tombstones";
    case problem_read_sample::threshold_unit::sstables: return "sstables";
    case problem_read_sample::threshold_unit::bytes: return "bytes";
    case problem_read_sample::threshold_unit::ratio: return "ratio";
    }
    __builtin_unreachable();
}

std::string_view to_string(reader_permit::wait_reason reason) {
    switch (reason) {
    // Admissible, but queued behind earlier waiters.
    case reader_permit::wait_reason::all_ok: return "other_waiters";
    case reader_permit::wait_reason::ready_list: return "ready_list";
    case reader_permit::wait_reason::need_cpu_permits: return "need_cpu";
    case reader_permit::wait_reason::memory_resources: return "memory";
    case reader_permit::wait_reason::count_resources: return "count";
    }
    __builtin_unreachable();
}

namespace {

// Renders each component of a key as a CQL literal, so it can be pasted into a query.
template <typename Columns>
std::vector<sstring> render_key(const std::vector<bytes>& components, const Columns& columns) {
    std::vector<sstring> result;
    result.reserve(components.size());
    auto column = columns.begin();
    for (const auto& component : components) {
        result.push_back(column->type->deserialize(component).to_parsable_string());
        ++column;
    }
    return result;
}

problem_read_range_bound make_partition_bound(const schema& s, const dht::ring_position& pos, bool inclusive) {
    return problem_read_range_bound{
        .value = pos.has_key() ? std::make_optional(render_key(pos.key()->explode(s), s.partition_key_columns())) : std::nullopt,
        .token = dht::token::to_int64(pos.token()),
        .inclusive = inclusive,
    };
}

problem_read_range_bound make_clustering_bound(const schema& s, const clustering_key_prefix& prefix, bool inclusive) {
    return problem_read_range_bound{
        .value = render_key(prefix.explode(s), s.clustering_key_columns()),
        .token = std::nullopt,
        .inclusive = inclusive,
    };
}

template <typename T, typename MakeBound>
problem_read_range make_range(const interval<T>& range, MakeBound make_bound) {
    problem_read_range result;
    if (range.is_singular()) {
        result.start = make_bound(range.start()->value(), true);
        result.end = result.start;
        return result;
    }
    if (range.start()) {
        result.start = make_bound(range.start()->value(), range.start()->is_inclusive());
    }
    if (range.end()) {
        result.end = make_bound(range.end()->value(), range.end()->is_inclusive());
    }
    return result;
}

std::string_view to_string(query::partition_slice::option option) {
    using opt = query::partition_slice::option;
    switch (option) {
    case opt::send_clustering_key: return "send_clustering_key";
    case opt::send_partition_key: return "send_partition_key";
    case opt::send_timestamp: return "send_timestamp";
    case opt::send_expiry: return "send_expiry";
    case opt::reversed: return "reversed";
    case opt::distinct: return "distinct";
    case opt::collections_as_maps: return "collections_as_maps";
    case opt::send_ttl: return "send_ttl";
    case opt::allow_short_read: return "allow_short_read";
    case opt::with_digest: return "with_digest";
    case opt::bypass_cache: return "bypass_cache";
    case opt::always_return_static_content: return "always_return_static_content";
    case opt::range_scan_data_variant: return "range_scan_data_variant";
    case opt::allow_mutation_read_page_without_live_row: return "allow_mutation_read_page_without_live_row";
    case opt::send_collection_timestamps: return "send_collection_timestamps";
    }
    __builtin_unreachable();
}

}

void describe_read(problem_read_sample& sample, const schema& s, const query::read_command& cmd, std::span<const dht::partition_range> ranges) {
    sample.table = s.id();
    sample.keyspace_name = s.ks_name();
    sample.table_name = s.cf_name();
    sample.paged = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
    sample.query_uuid = cmd.query_uuid;
    sample.is_first_page = bool(cmd.is_first_page);

    sample.partition_range_count = ranges.size();
    for (const auto& range : ranges | std::views::take(problem_read_sample::max_ranges)) {
        sample.partition_ranges.push_back(make_range(range, [&s] (const dht::ring_position& pos, bool inclusive) {
            return make_partition_bound(s, pos, inclusive);
        }));
    }
    const auto& row_ranges = cmd.slice.default_row_ranges();
    sample.clustering_range_count = row_ranges.size();
    for (const auto& range : row_ranges | std::views::take(problem_read_sample::max_ranges)) {
        sample.clustering_ranges.push_back(make_range(range, [&s] (const clustering_key_prefix& prefix, bool inclusive) {
            return make_clustering_bound(s, prefix, inclusive);
        }));
    }

    for (const auto id : cmd.slice.static_columns) {
        sample.columns.push_back(s.static_column_at(id).name_as_text());
    }
    for (const auto id : cmd.slice.regular_columns) {
        sample.columns.push_back(s.regular_column_at(id).name_as_text());
    }
    for (const auto option : cmd.slice.options) {
        sample.slice_options.emplace_back(to_string(option));
    }

    sample.row_limit = cmd.get_row_limit();
    sample.partition_limit = cmd.partition_limit;
    sample.per_partition_limit = cmd.slice.partition_row_limit();
}

// A service level runs its reads in a scheduling group named after it.
sstring current_service_level() {
    static constexpr std::string_view prefix = "sl:";
    const auto name = current_scheduling_group().name();
    std::string_view view = name;
    if (view.starts_with(prefix)) {
        view.remove_prefix(prefix.size());
    }
    return sstring(view);
}

namespace {

std::chrono::microseconds to_us(problem_reads::clock_type::duration d) {
    return std::chrono::duration_cast<std::chrono::microseconds>(d);
}

problem_read_sample make_base_sample(const problem_read_context& ctx) {
    problem_read_sample sample;
    describe_read(sample, ctx.s, ctx.cmd, ctx.ranges);
    sample.service_level = current_service_level();
    sample.sampled_at = db_clock::now();
    sample.kind = ctx.kind;

    // The read started when it was queued for admission, if it was, else when it started
    // executing.
    std::chrono::microseconds queue_wait{0};
    if (ctx.permit && ctx.admitted_this_page) {
        if (const auto admission = ctx.permit->get_wait_stats()) {
            queue_wait = to_us(admission->wait);
        }
    }
    if (ctx.execution_started) {
        const auto started = *ctx.execution_started - queue_wait;
        sample.latency = to_us(ctx.finished - started);
        if (ctx.timeout != db::no_timeout) {
            sample.timeout = to_us(ctx.timeout - started);
        }
    }
    if (ctx.p50_latency_us > 0) {
        sample.table_p50_latency = std::chrono::microseconds(int64_t(ctx.p50_latency_us));
    }
    if (ctx.p99_latency_us > 0) {
        sample.table_p99_latency = std::chrono::microseconds(int64_t(ctx.p99_latency_us));
    }

    if (ctx.permit) {
        if (ctx.admitted_this_page) {
            sample.admission = ctx.permit->get_wait_stats();
            if (sample.admission) {
                sample.queue_wait = to_us(sample.admission->wait);
            }
        }
        const auto stats = ctx.permit->get_read_stats();
        sample.sstables_read = stats.sstables_read;
        sample.memory_total = stats.memory_consumed;
        sample.memory_peak = ctx.permit->peak_memory();
    }

    if (ctx.page) {
        sample.tombstone_limit_reached = ctx.page->tombstone_limit_reached;
        sample.result_partitions = ctx.page->result_partitions;
        sample.result_rows = ctx.page->result_rows;
        sample.result_bytes = ctx.page->result_bytes;
        sample.counts = ctx.page->counts;
    }
    return sample;
}

}

void maybe_sample(problem_reads& pr, const problem_read_context& ctx) {
    using symptom = problem_read_symptom;
    using unit = problem_read_sample::threshold_unit;
    using failure = problem_read_sample::failure_kind;

    const auto now = ctx.finished;
    const auto& th = ctx.thresholds;
    auto ex = ctx.ex;

    struct detected {
        symptom s;
        std::optional<int64_t> threshold;
        std::optional<unit> threshold_unit;
    };
    // Detection runs on every read: it must not allocate.
    utils::small_vector<detected, problem_read_symptom_count> found;
    std::optional<failure> failed;

    auto check = [&] (symptom s) {
        return th.symptom_enabled(s);
    };

    ++ctx.counters.reads;

    if (ex) {
        if (ctx.execution_started && try_catch<seastar::timed_out_error>(ex)) {
            failed = failure::timeout;
        } else if (try_catch<query::unpaged_query_hard_limit_exceeded>(ex)) {
            failed = failure::unpaged_hard_limit;
        } else {
            // Not the read's fault, as far as we can tell.
            return;
        }
    }

    const bool paged = ctx.cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
    const bool single_partition = ctx.ranges.size() == 1 && query::is_single_partition(ctx.ranges.front());

    if (!paged) {
        if (failed == failure::unpaged_hard_limit || (ctx.page && ctx.page->unpaged_soft_limit_exceeded)) {
            if (check(symptom::unpaged_large)) {
                std::optional<int64_t> soft_limit;
                if (ctx.permit) {
                    soft_limit = ctx.permit->max_result_size().soft_limit;
                }
                found.push_back({symptom::unpaged_large, soft_limit, soft_limit ? std::optional(unit::bytes) : std::nullopt});
            }
        } else if (ctx.page && check(symptom::unpaged)
                // A partition of a table without clustering columns has a single row, which
                // the coordinator's may_need_paging() does not know.
                && !(single_partition && ctx.s.clustering_key_size() == 0)
                && query::may_need_paging(ctx.s, ctx.cmd, ctx.ranges)) {
            found.push_back({symptom::unpaged, {}, {}});
        }
    }

    if (failed == failure::timeout && check(symptom::timed_out)) {
        found.push_back({symptom::timed_out, {}, {}});
    }

    if (ctx.page && ctx.execution_started && th.slow_floor.count() && check(symptom::slow)) {
        auto threshold = th.slow_floor;
        if (single_partition && ctx.kind != problem_read_sample::read_kind::mutation) {
            threshold = std::max(threshold, std::chrono::microseconds(int64_t(ctx.p99_latency_us)));
        }
        if (to_us(now - *ctx.execution_started) >= threshold) {
            found.push_back({symptom::slow, threshold.count(), unit::us});
        }
    }

    if (ctx.permit && ctx.admitted_this_page && th.admission_wait_floor.count() && check(symptom::admission_wait)) {
        if (const auto admission = ctx.permit->get_wait_stats()) {
            const auto threshold = std::max(th.admission_wait_floor, std::chrono::microseconds(int64_t(ctx.p50_latency_us)));
            if (to_us(admission->wait) >= threshold) {
                found.push_back({symptom::admission_wait, threshold.count(), unit::us});
            }
        }
    }

    if (ctx.page) {
        const auto& counts = ctx.page->counts;
        if (th.tombstones && check(symptom::tombstones)
                && (counts.dead_rows + counts.range_tombstones >= th.tombstones || counts.dead_cells >= th.tombstones)) {
            found.push_back({symptom::tombstones, int64_t(th.tombstones), unit::tombstones});
        }
        if (ctx.page->tombstone_limit_reached && check(symptom::tombstone_limit_page)) {
            found.push_back({symptom::tombstone_limit_page, int64_t(ctx.cmd.tombstone_limit), unit::tombstones});
        }
    }

    if (ctx.page && ctx.permit) {
        const auto stats = ctx.permit->get_read_stats();
        const auto sstables_read = stats.sstables_read;
        if (single_partition && th.read_amplification_sstables && sstables_read >= th.read_amplification_sstables
                && check(symptom::read_amplification)) {
            found.push_back({symptom::read_amplification, int64_t(th.read_amplification_sstables), unit::sstables});
        }

        const auto semaphore_memory = uint64_t(std::max<ssize_t>(ctx.permit->semaphore().initial_resources().memory, 0));
        const auto peak_threshold = semaphore_memory * th.memory_peak_percent / 100;
        if (peak_threshold && ctx.permit->peak_memory() >= peak_threshold && check(symptom::memory_peak)) {
            found.push_back({symptom::memory_peak, int64_t(peak_threshold), unit::bytes});
        }

        const auto memory_total = stats.memory_consumed;
        if (th.memory_amplification_ratio && memory_total >= th.memory_amplification_min_bytes
                && memory_total >= th.memory_amplification_ratio * std::max<uint64_t>(ctx.page->result_bytes, 1)
                && check(symptom::memory_amplification)) {
            found.push_back({symptom::memory_amplification, int64_t(th.memory_amplification_ratio), unit::ratio});
        }
    }

    for (const auto& d : found) {
        ++ctx.counters.symptoms[size_t(d.s)];
    }
    found.erase(std::remove_if(found.begin(), found.end(), [&] (const detected& d) { return !pr.should_sample(d.s, now); }), found.end());
    if (found.empty()) {
        return;
    }

    auto sample = make_base_sample(ctx);
    sample.failure = failed;
    for (auto& d : found) {
        auto s = sample;
        s.symptom = d.s;
        s.threshold = d.threshold;
        s.unit = d.threshold_unit;
        pr.record(std::move(s), now);
    }
}

problem_reads::problem_reads()
    : _expiry_timer([this] {
        expire();
        arm_expiry_timer();
    }) {
    arm_expiry_timer();
}

// Expired samples need not go the moment they expire, so the timer runs at a coarse fixed
// interval rather than tracking the oldest sample.
void problem_reads::arm_expiry_timer() {
    if (_config.ttl.count()) {
        _expiry_timer.arm(clock_type::now() + std::min<clock_type::duration>(_config.ttl, std::chrono::minutes(1)));
    }
}

void problem_reads::set_config(config cfg) {
    _config = cfg;
    while (_entries.size() > _config.capacity) {
        _entries.pop_back();
    }
    _expiry_timer.cancel();
    arm_expiry_timer();
}

void problem_reads::record(problem_read_sample sample, clock_type::time_point now) {
    _next_sample_at[size_t(sample.symptom)] = now + _config.sample_period;
    if (!_config.capacity) {
        return;
    }
    auto it = std::ranges::find_if(_entries, [&] (const entry& e) {
        return e.sample.symptom == sample.symptom && e.sample.table == sample.table && e.sample.service_level == sample.service_level;
    });
    if (it == _entries.end() && _entries.size() >= _config.capacity) {
        it = std::prev(_entries.end());
    }
    if (it != _entries.end()) {
        it->sample = std::move(sample);
        it->recorded_at = now;
        _entries.splice(_entries.begin(), _entries, it);
    } else {
        _entries.push_front(entry{std::move(sample), now});
    }
}

void problem_reads::expire(clock_type::time_point now) {
    if (!_config.ttl.count()) {
        return;
    }
    while (!_entries.empty() && now - _entries.back().recorded_at >= _config.ttl) {
        _entries.pop_back();
    }
}

std::vector<problem_read_sample> problem_reads::samples() const {
    std::vector<problem_read_sample> result;
    result.reserve(_entries.size());
    for (const auto& e : _entries) {
        result.push_back(e.sample);
    }
    return result;
}

}
