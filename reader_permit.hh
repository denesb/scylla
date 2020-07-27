/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/file.hh>
#include "seastarx.hh"

#include "db/timeout_clock.hh"

struct reader_resources {
    int count = 0;
    ssize_t memory = 0;

    reader_resources() = default;

    reader_resources(int count, ssize_t memory)
        : count(count)
        , memory(memory) {
    }

    bool operator>=(const reader_resources& other) const {
        return count >= other.count && memory >= other.memory;
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

    explicit operator bool() const {
        return count > 0 || memory > 0;
    }
};

class reader_concurrency_semaphore;

/// A permit for a specific read.
///
/// Used to track the read's resource consumption and wait for admission to read
/// from the disk.
/// Use `consume_memory()` to register memory usage. Use `wait_admission()` to
/// wait for admission, before reading from the disk. Both methods return a
/// `resource_units` RAII object that should be held onto while the respective
/// resources are in use.
class reader_permit {
    friend class reader_concurrency_semaphore;

public:
    class resource_units {
        reader_concurrency_semaphore* _semaphore;
        reader_resources _resources;

        friend class reader_permit;
        friend class reader_concurrency_semaphore;
    private:
        resource_units(reader_concurrency_semaphore& semaphore, reader_resources res) noexcept;
    public:
        resource_units(const resource_units&) = delete;
        resource_units(resource_units&&) noexcept;
        ~resource_units();
        resource_units& operator=(const resource_units&) = delete;
        resource_units& operator=(resource_units&&) noexcept;
        void add(resource_units&& o);
        void reset(reader_resources res = {});
    };

private:
    reader_concurrency_semaphore* _semaphore;

private:
    explicit reader_permit(reader_concurrency_semaphore& semaphore);

public:
    bool operator==(const reader_permit& o) const {
        return _semaphore == o._semaphore;
    }

    reader_concurrency_semaphore& semaphore() {
        return *_semaphore;
    }

    future<resource_units> wait_admission(size_t memory, db::timeout_clock::time_point timeout);

    resource_units consume_memory(size_t memory = 0);

    resource_units consume_resources(reader_resources res);

    void release();
};

template <typename Char>
temporary_buffer<Char> make_tracked_temporary_buffer(temporary_buffer<Char> buf, reader_permit& permit) {
    return temporary_buffer<Char>(buf.get_write(), buf.size(),
            make_deleter(buf.release(), [units = permit.consume_memory(buf.size())] () mutable { units.reset(); }));
}

file make_tracked_file(file f, reader_permit p);
