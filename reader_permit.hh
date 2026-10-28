/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>

#include <seastar/util/optimized_optional.hh>
#include "seastarx.hh"

#include "db/timeout_clock.hh"
#include "schema/schema_fwd.hh"
#include "tracing/trace_state.hh"

namespace query {

struct max_result_size;

}

namespace seastar {
    class file;
} // namespace seastar

struct reader_resources {
    int count = 0;
    ssize_t memory = 0;

    static reader_resources with_memory(ssize_t memory) { return reader_resources(0, memory); }

    reader_resources() = default;

    reader_resources(int count, ssize_t memory)
        : count(count)
        , memory(memory) {
    }

    reader_resources operator-(const reader_resources& other) const {
        return reader_resources{count - other.count, memory - other.memory};
    }

    reader_resources& operator-=(const reader_resources& other) {
        count -= other.count;
        memory -= other.memory;
        return *this;
    }

    reader_resources operator+(const reader_resources& other) const {
        return reader_resources{count + other.count, memory + other.memory};
    }

    reader_resources& operator+=(const reader_resources& other) {
        count += other.count;
        memory += other.memory;
        return *this;
    }

    bool non_zero() const {
        return count > 0 || memory > 0;
    }
};

inline bool operator==(const reader_resources& a, const reader_resources& b) {
    return a.count == b.count && a.memory == b.memory;
}

class reader_concurrency_semaphore;

/// A permit for a specific read.
///
/// Used to track the read's resource consumption. Use `consume_memory()` to
/// register memory usage, which returns a `resource_units` RAII object that
/// should be held onto while the respective resources are in use.
class reader_permit {
    friend class reader_concurrency_semaphore;
    friend class tracking_allocator_base;

public:
    class resource_units;
    class need_cpu_guard;
    class awaits_guard;

    enum class state {
        waiting_for_admission,
        waiting_for_memory,
        waiting_for_execution,
        active,
        active_need_cpu,
        active_await,
        inactive,
        evicted,
        preemptive_aborted,
    };

    class impl;

private:
    shared_ptr<impl> _impl;

private:
    reader_permit() = default;
    reader_permit(shared_ptr<impl>);
    explicit reader_permit(reader_concurrency_semaphore& semaphore, schema_ptr schema, std::string_view op_name,
            reader_resources base_resources, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr);
    explicit reader_permit(reader_concurrency_semaphore& semaphore, schema_ptr schema, sstring&& op_name,
            reader_resources base_resources, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr);

    reader_permit::impl& operator*() { return *_impl; }
    reader_permit::impl* operator->() { return _impl.get(); }

    void mark_need_cpu() noexcept;

    void mark_not_need_cpu() noexcept;

    void mark_awaits() noexcept;

    void mark_not_awaits() noexcept;

    operator bool() const { return bool(_impl); }

    friend class optimized_optional<reader_permit>;

    void consume(reader_resources res);

    void signal(reader_resources res);

public:
    ~reader_permit();

    reader_permit(const reader_permit&) = default;
    reader_permit(reader_permit&&) = default;

    reader_permit& operator=(const reader_permit&) = default;
    reader_permit& operator=(reader_permit&&) = default;

    bool operator==(const reader_permit& o) const {
        return _impl == o._impl;
    }

    reader_concurrency_semaphore& semaphore();

    const schema_ptr& get_schema() const;
    std::string_view get_op_name() const;
    state get_state() const;

    bool needs_readmission() const;

    // Call only when needs_readmission() = true.
    future<> wait_readmission();

    resource_units consume_memory(size_t memory = 0);

    resource_units consume_resources(reader_resources res);

    future<resource_units> request_memory(size_t memory);

    reader_resources consumed_resources() const;

    reader_resources base_resources() const;

    void release_base_resources() noexcept;

    sstring description() const;

    db::timeout_clock::time_point timeout() const noexcept;

    void set_timeout(db::timeout_clock::time_point timeout) noexcept;

    const tracing::trace_state_ptr& trace_state() const noexcept;

    void set_trace_state(tracing::trace_state_ptr trace_ptr) noexcept;

    // If the permit is aborted, return the exception it was aborted with.
    const std::exception_ptr& get_abort_exception() const noexcept;

    query::max_result_size max_result_size() const;
    void set_max_result_size(query::max_result_size);

    void on_start_sstable_read() noexcept;
    void on_finish_sstable_read() noexcept;

    // Statistics of the work done on behalf of the permit, for diagnosing problematic reads,
    // since the start of the current page (start_page()), or since the permit was created.
    struct read_stats {
        // The number of sstable readers created.
        uint64_t sstables_read = 0;
        // The sum of all memory consumed, including memory that was released since.
        uint64_t memory_consumed = 0;
    };
    read_stats get_read_stats() const noexcept;

    // The most memory the permit held at once since the last reset_peak_memory() or
    // start_page(), or since it was created.
    uint64_t peak_memory() const noexcept;
    // Restart peak tracking from the memory the permit holds now.
    void reset_peak_memory() noexcept;

    // What a page read with the permit produced and went through, reported by the replica
    // when it is asked to (for detecting problematic reads).
    struct page_report {
        struct read_counts {
            uint64_t live_partitions = 0;
            uint64_t dead_partitions = 0;
            uint64_t live_rows = 0;
            uint64_t dead_rows = 0;
            uint64_t range_tombstones = 0;
            uint64_t live_cells = 0;
            uint64_t dead_cells = 0;
        };
        read_counts counts;
        bool tombstone_limit_reached = false;
        bool unpaged_soft_limit_exceeded = false;
        uint64_t result_partitions = 0;
        uint64_t result_rows = 0;
        uint64_t result_bytes = 0;
    };

    // Start a new page: reset the read statistics and restart peak tracking. A paged read
    // keeps its permit across pages, so this state lives in the permit.
    void start_page() noexcept;
    // When the current page started; the epoch if no page was started.
    db::timeout_clock::time_point page_started_at() const noexcept;

    // Everything about the current page the replica checks a read against, in one call.
    struct page_summary {
        db::timeout_clock::time_point started_at;
        // Zero if the permit was not queued for admission.
        db::timeout_clock::duration queue_wait{};
        read_stats stats;
        uint64_t peak_memory = 0;
        uint64_t semaphore_memory = 0;
    };
    page_summary get_page_summary() const noexcept;

    // The outcome of the semaphore's admission check: whether the resources needed to admit a
    // read are available, and if not, what is missing. A read is queued for admission when
    // the check fails, or when it passes but earlier waiters are queued (all_ok).
    enum class wait_reason { all_ok = 0, ready_list, need_cpu_permits, memory_resources, count_resources };
    // The last wait of the permit in the admission queue, with the state of the semaphore at
    // the time the permit was queued, which is what caused the wait.
    struct wait_stats {
        db::timeout_clock::duration wait;
        wait_reason reason;
        // Permits waiting on the semaphore (for admission, memory or execution).
        uint64_t queue_length;
        // Count resources in use.
        int64_t active_count;
        // Memory resources in use.
        int64_t memory_used;
    };
    // Disengaged if the permit was admitted without waiting in the queue.
    std::optional<wait_stats> get_wait_stats() const noexcept;

    uintptr_t id() { return reinterpret_cast<uintptr_t>(_impl.get()); }
};

using reader_permit_opt = optimized_optional<reader_permit>;

class reader_permit::resource_units {
    reader_permit _permit;
    reader_resources _resources;

    friend class reader_permit;
    friend class reader_concurrency_semaphore;
private:
    class already_consumed_tag {};
    resource_units(reader_permit permit, reader_resources res, already_consumed_tag);
    resource_units(reader_permit permit, reader_resources res);
public:
    resource_units(const resource_units&) = delete;
    resource_units(resource_units&&) noexcept;
    ~resource_units();
    resource_units& operator=(const resource_units&) = delete;
    resource_units& operator=(resource_units&&) noexcept;
    void add(resource_units&& o);
    void reset_to(reader_resources res);
    void reset_to_zero() noexcept;
    reader_permit permit() const { return _permit; }
    reader_resources resources() const { return _resources; }
};

/// Mark a permit as needing CPU.
///
/// Conceptually, a permit is considered as needing CPU, when at least one reader
/// associated with it has an ongoing foreground operation initiated by
/// its consumer. E.g. a pending `fill_buffer()` call.
/// This class is an RAII need_cpu marker meant to be used by keeping it alive
/// while the reader is in need of CPU.
class reader_permit::need_cpu_guard {
    reader_permit_opt _permit;
public:
    explicit need_cpu_guard(reader_permit permit) noexcept : _permit(std::move(permit)) {
        _permit->mark_need_cpu();
    }
    need_cpu_guard(need_cpu_guard&&) noexcept = default;
    need_cpu_guard(const need_cpu_guard&) = delete;
    ~need_cpu_guard() {
        if (_permit) {
            _permit->mark_not_need_cpu();
        }
    }
    need_cpu_guard& operator=(need_cpu_guard&&) = delete;
    need_cpu_guard& operator=(const need_cpu_guard&) = delete;
};

/// Mark a permit as awaiting I/O or an operation running on a remote shard.
///
/// Conceptually, a permit is considered awaiting, when at least one reader
/// associated with it is waiting on I/O or a remote shard as part of a
/// foreground operation initiated by its consumer. E.g. an sstable reader
/// waiting on a disk read as part of its `fill_buffer()` call.
/// This class is an RAII awaits marker meant to be used by keeping it alive
/// until said awaited event completes.
class reader_permit::awaits_guard {
    reader_permit_opt _permit;
public:
    explicit awaits_guard(reader_permit permit) noexcept : _permit(std::move(permit)) {
        _permit->mark_awaits();
    }
    awaits_guard(awaits_guard&&) noexcept = default;
    awaits_guard(const awaits_guard&) = delete;
    ~awaits_guard() {
        if (_permit) {
            _permit->mark_not_awaits();
        }
    }
    awaits_guard& operator=(awaits_guard&&) = delete;
    awaits_guard& operator=(const awaits_guard&) = delete;
};

template <typename Char>
temporary_buffer<Char> make_tracked_temporary_buffer(temporary_buffer<Char> buf, reader_permit::resource_units units) {
    return temporary_buffer<Char>(buf.get_write(), buf.size(), make_object_deleter(buf.release(), std::move(units)));
}

inline temporary_buffer<char> make_new_tracked_temporary_buffer(size_t size, reader_permit& permit) {
    auto buf = temporary_buffer<char>(size);
    return temporary_buffer<char>(buf.get_write(), buf.size(), make_object_deleter(buf.release(), permit.consume_memory(size)));
}

file make_tracked_file(file f, reader_permit p);

class tracking_allocator_base {
    reader_permit _permit;
protected:
    tracking_allocator_base(reader_permit permit) noexcept : _permit(std::move(permit)) { }
    void consume(size_t memory) {
        _permit.consume(reader_resources::with_memory(memory));
    }
    void signal(size_t memory) {
        _permit.signal(reader_resources::with_memory(memory));
    }
};

template <typename T>
class tracking_allocator : public tracking_allocator_base {
public:
    using value_type = T;
    using propagate_on_container_move_assignment = std::true_type;
    using is_always_equal = std::false_type;

private:
    std::allocator<T> _alloc;

public:
    tracking_allocator(reader_permit permit) noexcept : tracking_allocator_base(std::move(permit)) { }

    T* allocate(size_t n) {
        auto p = _alloc.allocate(n);
        try {
            consume(n * sizeof(T));
        } catch (...) {
            _alloc.deallocate(p, n);
            throw;
        }
        return p;
    }
    void deallocate(T* p, size_t n) {
        _alloc.deallocate(p, n);
        if (n) {
            signal(n * sizeof(T));
        }
    }

    template <typename U>
    friend bool operator==(const tracking_allocator<U>& a, const tracking_allocator<U>& b);
};

template <typename T>
bool operator==(const tracking_allocator<T>& a, const tracking_allocator<T>& b) {
    return a._semaphore == b._semaphore;
}

template <> struct fmt::formatter<reader_permit::state> : fmt::formatter<string_view> {
    auto format(reader_permit::state, fmt::format_context& ctx) const -> decltype(ctx.out());
};
template <> struct fmt::formatter<reader_resources> : fmt::formatter<string_view> {
    auto format(const reader_resources&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
