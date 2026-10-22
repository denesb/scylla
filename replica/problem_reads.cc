/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/problem_reads.hh"

#include <algorithm>

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
