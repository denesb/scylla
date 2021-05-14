/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "mutation_reader.hh"
#include <seastar/core/gate.hh>

class test_reader_lifecycle_policy
        : public reader_lifecycle_policy
        , public enable_shared_from_this<test_reader_lifecycle_policy> {
public:
    class operations_gate {
    public:
        class operation {
            gate* _g = nullptr;

        private:
            void leave() {
                if (_g) {
                    _g->leave();
                }
            }

        public:
            operation() = default;
            explicit operation(gate& g) : _g(&g) { _g->enter(); }
            operation(const operation&) = delete;
            operation(operation&& o) : _g(std::exchange(o._g, nullptr)) { }
            ~operation() { leave(); }
            operation& operator=(const operation&) = delete;
            operation& operator=(operation&& o) {
                leave();
                _g = std::exchange(o._g, nullptr);
                return *this;
            }
        };

    private:
        std::vector<gate> _gates;

    public:
        operations_gate()
            : _gates(smp::count) {
        }

        operation enter() {
            return operation(_gates[this_shard_id()]);
        }

        future<> close() {
            return parallel_for_each(boost::irange(smp::count), [this] (shard_id shard) {
                return smp::submit_to(shard, [this, shard] {
                    return _gates[shard].close();
                });
            });
        }
    };

    class semaphore_registry {
        std::vector< // 1 per shard
            std::list<reader_concurrency_semaphore>> _semaphores;
    public:
        semaphore_registry() : _semaphores(smp::count) { }
        semaphore_registry(semaphore_registry&&) = delete;
        semaphore_registry(const semaphore_registry&) = delete;
        template <typename... Arg>
        reader_concurrency_semaphore& create_semaphore(Arg&&... arg) {
            return _semaphores[this_shard_id()].emplace_back(std::forward<Arg>(arg)...);
        }
    };

private:
    using factory_function = std::function<flat_mutation_reader(
            schema_ptr,
            reader_permit,
            const dht::partition_range&,
            const query::partition_slice&,
            const io_priority_class&,
            tracing::trace_state_ptr,
            mutation_reader::forwarding)>;

    struct reader_context {
        reader_concurrency_semaphore* semaphore = nullptr;
        operations_gate::operation op;
        std::optional<future<reader_permit>> wait_future;
        std::optional<const dht::partition_range> range;
        std::optional<const query::partition_slice> slice;

        reader_context() = default;
        reader_context(dht::partition_range range, query::partition_slice slice) : range(std::move(range)), slice(std::move(slice)) {
        }
    };

    factory_function _factory_function;
    operations_gate& _operation_gate;
    semaphore_registry& _semaphore_registry;
    std::vector<foreign_ptr<std::unique_ptr<reader_context>>> _contexts;
    std::vector<future<>> _destroy_futures;
    bool _evict_paused_readers = false;

public:
    explicit test_reader_lifecycle_policy(factory_function f, operations_gate& g, semaphore_registry& semaphore_registry, bool evict_paused_readers = false)
        : _factory_function(std::move(f))
        , _operation_gate(g)
        , _semaphore_registry(semaphore_registry)
        , _contexts(smp::count)
        , _evict_paused_readers(evict_paused_readers) {
    }
    virtual flat_mutation_reader create_reader(
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr) override {
        const auto shard = this_shard_id();
        if (_contexts[shard]) {
            _contexts[shard]->range.emplace(range);
            _contexts[shard]->slice.emplace(slice);
        } else {
            _contexts[shard] = make_foreign(std::make_unique<reader_context>(range, slice));
        }
        _contexts[shard]->op = _operation_gate.enter();
        return _factory_function(std::move(schema), std::move(permit), *_contexts[shard]->range, *_contexts[shard]->slice, pc, std::move(trace_state), fwd_mr);
    }
    virtual future<> destroy_reader(shard_id shard, future<stopped_reader> reader) noexcept override {
        // waited via _operation_gate
        return reader.then([shard, this] (stopped_reader&& reader) {
            return smp::submit_to(shard, [handle = std::move(reader.handle), ctx = &*_contexts[shard]] () mutable {
                auto reader_opt = ctx->semaphore->unregister_inactive_read(std::move(*handle));
                auto ret = reader_opt ? reader_opt->close() : make_ready_future<>();
                ctx->semaphore->broken();
                if (ctx->wait_future) {
                  ret = ret.then([ctx = std::move(ctx)] () mutable {
                    return ctx->wait_future->then_wrapped([ctx = std::move(ctx)] (future<reader_permit> f) mutable {
                        f.ignore_ready_future();
                    });
                  });
                }
                return std::move(ret);
            });
        }).finally([zis = shared_from_this()] {});
    }
    virtual reader_concurrency_semaphore& semaphore() override {
        const auto shard = this_shard_id();
        if (!_contexts[shard]) {
            _contexts[shard] = make_foreign(std::make_unique<reader_context>());
        } else if (_contexts[shard]->semaphore) {
            return *_contexts[shard]->semaphore;
        }
        if (_evict_paused_readers) {
            // Create with no memory, so all inactive reads are immediately evicted.
            _contexts[shard]->semaphore = &_semaphore_registry.create_semaphore(1, 0, format("reader_concurrency_semaphore @shard_id={}", shard));
        } else {
            _contexts[shard]->semaphore = &_semaphore_registry.create_semaphore(reader_concurrency_semaphore::no_limits{});
        }
        return *_contexts[shard]->semaphore;
    }
    virtual future<reader_permit> obtain_reader_permit(schema_ptr schema, const char* const description, db::timeout_clock::time_point timeout) override {
        return semaphore().obtain_permit_nowait(schema.get(), description, 128 * 1024, timeout);
    }
};

