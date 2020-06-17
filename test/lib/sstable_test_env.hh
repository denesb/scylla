/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/do_with.hh>

#include "sstables/sstables.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/test_services.hh"

namespace sstables {

class test_env {
    sstables_manager& _mgr;
public:
    explicit test_env() : _mgr(test_sstables_manager) { }
    explicit test_env(sstables_manager& mgr) : _mgr(mgr) { }

    shared_sstable make_sstable(schema_ptr schema, sstring dir, unsigned long generation,
            sstable::version_types v, sstable::format_types f = sstable::format_types::big,
            size_t buffer_size = default_sstable_buffer_size, gc_clock::time_point now = gc_clock::now()) {
        return _mgr.make_sstable(std::move(schema), dir, generation, v, f, now, default_io_error_handler_gen(), buffer_size);
    }

    future<shared_sstable> reusable_sst(schema_ptr schema, sstring dir, unsigned long generation,
            sstable::version_types version = sstable::version_types::la, sstable::format_types f = sstable::format_types::big) {
        auto sst = make_sstable(std::move(schema), dir, generation, version, f);
        return sst->load().then([sst = std::move(sst)] {
            return make_ready_future<shared_sstable>(std::move(sst));
        });
    }

    future<> working_sst(schema_ptr schema, sstring dir, unsigned long generation) {
        return reusable_sst(std::move(schema), dir, generation).then([] (auto ptr) { return make_ready_future<>(); });
    }

    template <typename Func>
    static inline auto do_with(Func&& func) {
        return seastar::do_with(test_env(), [func = std::move(func)] (test_env& env) mutable {
            return func(env);
        });
    }

    template <typename T, typename Func>
    static inline auto do_with(T&& rval, Func&& func) {
        return seastar::do_with(test_env(), std::forward<T>(rval), [func = std::move(func)] (test_env& env, T& val) mutable {
            return func(env, val);
        });
    }

    template <typename Func>
    static inline auto do_with_async(Func&& func) {
        return seastar::async([func = std::move(func)] {
            auto wait_for_background_jobs = defer([] { sstables::await_background_jobs_on_all_shards().get(); });
            test_env env;
            func(env);
        });
    }
};

}   // namespace sstables
