/*
 * Copyright (C) 2018 ScyllaDB
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

#include <seastar/core/seastar.hh>
#include <seastar/core/print.hh>
#include <seastar/util/lazy.hh>
#include <seastar/util/log.hh>

#include "reader_concurrency_semaphore.hh"
#include "utils/exceptions.hh"
#include "schema.hh"
#include "utils/human_readable.hh"
#include "flat_mutation_reader.hh"

logger rcslog("reader_concurrency_semaphore");

reader_permit::resource_units::resource_units(reader_permit permit, reader_resources res) noexcept
    : _permit(std::move(permit)), _resources(res) {
    _permit.consume(res);
}

reader_permit::resource_units::resource_units(resource_units&& o) noexcept
    : _permit(std::move(o._permit))
    , _resources(std::exchange(o._resources, {})) {
}

reader_permit::resource_units::~resource_units() {
    if (_resources) {
        reset();
    }
}

reader_permit::resource_units& reader_permit::resource_units::operator=(resource_units&& o) noexcept {
    if (&o == this) {
        return *this;
    }
    reset();
    _permit = std::move(o._permit);
    _resources = std::exchange(o._resources, {});
    return *this;
}

void reader_permit::resource_units::add(resource_units&& o) {
    assert(_permit == o._permit);
    _resources += std::exchange(o._resources, {});
}

void reader_permit::resource_units::reset(reader_resources res) {
    _permit.consume(res);
    if (_resources) {
        _permit.signal(_resources);
    }
    _resources = res;
}

class reader_permit::impl
        : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>
        , public enable_shared_from_this<reader_permit::impl> {
    reader_concurrency_semaphore& _semaphore;
    const schema* _schema;
    sstring _op_name;
    std::string_view _op_name_view;
    reader_resources _base_resources;
    reader_resources _resources;
    reader_permit::state _state = reader_permit::state::registered;
    bool _was_admitted = false;
    uint64_t _used_branches = 0;
    uint64_t _blocked_branches = 0;

public:
    enum class ext_state {
        unused,
        active,
        blocked,
    };

    struct value_tag {};

    impl(reader_concurrency_semaphore& semaphore, const schema* const schema, const std::string_view& op_name)
        : _semaphore(semaphore)
        , _schema(schema)
        , _op_name_view(op_name)
    {
        _semaphore.on_permit_created(*this);
    }
    impl(reader_concurrency_semaphore& semaphore, const schema* const schema, sstring&& op_name)
        : _semaphore(semaphore)
        , _schema(schema)
        , _op_name(std::move(op_name))
        , _op_name_view(_op_name)
    {
        _semaphore.on_permit_created(*this);
    }
    ~impl() {
        if (_state != reader_permit::state::evicted) {
            // on_evicted() already signalled _base_resources
            signal(_base_resources);
        }

        if (_resources) {
            on_internal_error_noexcept(rcslog, format("reader_permit::impl::~impl(): permit {}.{}:{} detected a leak of {{count={}, memory={}}} resources",
                        _schema ? _schema->ks_name() : "*",
                        _schema ? _schema->cf_name() : "*",
                        _op_name_view,
                        _resources.count,
                        _resources.memory));
            signal(_resources);
        }

        if (_used_branches) {
            on_internal_error_noexcept(rcslog, format("reader_permit::impl::~impl(): permit {}.{}:{} destroyed with {} used branches",
                        _schema ? _schema->ks_name() : "*",
                        _schema ? _schema->cf_name() : "*",
                        _op_name_view,
                        _used_branches));
            _semaphore.on_permit_unused();
        }

        if (_blocked_branches) {
            on_internal_error_noexcept(rcslog, format("reader_permit::impl::~impl(): permit {}.{}:{} destroyed with {} blocked branches",
                        _schema ? _schema->ks_name() : "*",
                        _schema ? _schema->cf_name() : "*",
                        _op_name_view,
                        _blocked_branches));
            _semaphore.on_permit_unblocked();
        }

        _semaphore.on_permit_destroyed(*this);
    }

    reader_concurrency_semaphore& semaphore() {
        return _semaphore;
    }

    const ::schema* get_schema() const {
        return _schema;
    }

    std::string_view get_op_name() const {
        return _op_name_view;
    }

    reader_permit::state get_state() const {
        return _state;
    }

    void on_waiting() {
        _state = reader_permit::state::waiting;
    }

    void on_admission() {
        _state = reader_permit::state::admitted;
        _was_admitted = true;
        if (_used_branches) {
            _semaphore.on_permit_used();
        }
        if (_blocked_branches) {
            assert(_used_branches);
            _semaphore.on_permit_blocked();
        }
    }

    void on_register_as_inactive() {
        _state = reader_permit::state::inactive;
        if (_blocked_branches && _was_admitted) {
            _semaphore.on_permit_unblocked();
        }
        if (_used_branches && _was_admitted) {
            _semaphore.on_permit_unused();
        }
    }

    void on_unregister_as_inactive() {
        if (_was_admitted) {
            _state = reader_permit::state::admitted;
        } else {
            _state = reader_permit::state::registered;
        }
        if (_used_branches && _was_admitted) {
            _semaphore.on_permit_used();
        }
        if (_blocked_branches && _was_admitted) {
            _semaphore.on_permit_blocked();
        }
    }

    void on_evicted() {
        signal(_base_resources);
        if (_was_admitted) {
            _state = reader_permit::state::evicted;
        } else {
            _state = reader_permit::state::registered;
        }
    }

    void consume(reader_resources res) {
        _resources += res;
        _semaphore.consume(res);
    }

    void signal(reader_resources res) {
        _resources -= res;
        _semaphore.signal(res);
    }

    reader_resources resources() const {
        return _resources;
    }

    void mark_used() noexcept {
        ++_used_branches;
        if (_state == state::admitted && _used_branches == 1) {
            _semaphore.on_permit_used();
        }
    }

    void mark_unused() noexcept {
        assert(_used_branches);
        --_used_branches;
        if (_state == state::admitted && _used_branches == 0) {
            _semaphore.on_permit_unused();
        }
    }

    void mark_blocked() noexcept {
        ++_blocked_branches;
        if (_state == state::admitted && _blocked_branches == 1) {
            assert(_used_branches);
            _semaphore.on_permit_blocked();
        }
    }

    void mark_unblocked() noexcept {
        assert(_blocked_branches);
        --_blocked_branches;
        if (_state == state::admitted && _blocked_branches == 0) {
            _semaphore.on_permit_unblocked();
        }
    }

    ext_state get_ext_state() const {
        if (!_used_branches) {
            return ext_state::unused;
        }
        if (_blocked_branches) {
            return ext_state::blocked;
        }
        return ext_state::active;
    }

    void incorporate(resource_units res) {
        _base_resources = res._resources;
        _resources -= res._resources;
        res._resources = {};
    }

    future<> wait_readmission(db::timeout_clock::time_point timeout) {
        assert(_state == reader_permit::state::evicted);
        return _semaphore.do_wait_admission(shared_from_this(), _base_resources.memory, timeout).then([this] (resource_units units) {
            incorporate(std::move(units));
        });
    }
};

struct reader_concurrency_semaphore::permit_list {
    using list_type = boost::intrusive::list<reader_permit::impl, boost::intrusive::constant_time_size<false>>;

    list_type permits;
    uint64_t total_permits = 0;
    uint64_t used_permits = 0;
    uint64_t blocked_permits = 0;
};

reader_permit::reader_permit(shared_ptr<impl> impl) : _impl(std::move(impl))
{
}

reader_permit::reader_permit(reader_concurrency_semaphore& semaphore, const schema* const schema, std::string_view op_name)
    : _impl(::seastar::make_shared<reader_permit::impl>(semaphore, schema, op_name))
{
}

reader_permit::reader_permit(reader_concurrency_semaphore& semaphore, const schema* const schema, sstring&& op_name)
    : _impl(::seastar::make_shared<reader_permit::impl>(semaphore, schema, std::move(op_name)))
{
}

void reader_permit::on_waiting() {
    _impl->on_waiting();
}

void reader_permit::on_admission() {
    _impl->on_admission();
}

reader_permit::~reader_permit() {
}

reader_concurrency_semaphore& reader_permit::semaphore() {
    return _impl->semaphore();
}

future<reader_permit::resource_units> reader_permit::wait_admission(size_t memory, db::timeout_clock::time_point timeout) {
    return _impl->semaphore().do_wait_admission(*this, memory, timeout);
}

future<> reader_permit::wait_readmission(db::timeout_clock::time_point timeout) {
    return _impl->wait_readmission(timeout);
}

void reader_permit::consume(reader_resources res) {
    _impl->consume(res);
}

void reader_permit::signal(reader_resources res) {
    _impl->signal(res);
}

reader_permit::resource_units reader_permit::consume_memory(size_t memory) {
    return consume_resources(reader_resources{0, ssize_t(memory)});
}

reader_permit::resource_units reader_permit::consume_resources(reader_resources res) {
    return resource_units(*this, res);
}

reader_resources reader_permit::consumed_resources() const {
    return _impl->resources();
}

void reader_permit::mark_used() noexcept {
    _impl->mark_used();
}

void reader_permit::mark_unused() noexcept {
    _impl->mark_unused();
}

void reader_permit::mark_blocked() noexcept {
    _impl->mark_blocked();
}

void reader_permit::mark_unblocked() noexcept {
    _impl->mark_unblocked();
}

void reader_permit::incorporate(resource_units res) {
    _impl->incorporate(std::move(res));
}

std::ostream& operator<<(std::ostream& os, reader_permit::state s) {
    switch (s) {
        case reader_permit::state::registered:
            os << "registered";
            break;
        case reader_permit::state::waiting:
            os << "waiting";
            break;
        case reader_permit::state::admitted:
            os << "admitted";
            break;
        case reader_permit::state::inactive:
            os << "inactive";
            break;
        case reader_permit::state::evicted:
            os << "evicted";
            break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, reader_permit::impl::ext_state s) {
    switch (s) {
        case reader_permit::impl::ext_state::unused:
            os << "unused";
            break;
        case reader_permit::impl::ext_state::active:
            os << "active";
            break;
        case reader_permit::impl::ext_state::blocked:
            os << "blocked";
            break;
    }
    return os;
}

namespace {

using permit_group_key = std::tuple<const schema*, std::string_view>;

struct permit_group_key_hash {
    size_t operator()(const permit_group_key& k) const {
        return std::hash<uintptr_t>()(reinterpret_cast<uintptr_t>(std::get<0>(k)))
            ^ std::hash<std::string_view>()(std::get<1>(k));
    }
};

struct permit_group {
    reader_resources resources;
    std::vector<const reader_permit::impl*> permits;

    void add(const reader_permit::impl& permit) {
        resources += permit.resources();
        permits.push_back(&permit);
    }
};

using permit_groups = std::unordered_map<permit_group_key, permit_group, permit_group_key_hash>;

struct permit_stats {
    uint64_t permits = 0;
    reader_resources resources;

    void add(const reader_permit::impl& permit) {
        ++permits;
        resources += permit.resources();
    }

    void add(const permit_group& group) {
        permits += group.permits.size();
        resources += group.resources;
    }

    permit_stats& operator+=(const permit_stats& o) {
        permits += o.permits;
        resources += o.resources;
        return *this;
    }
};

template <typename Map>
std::vector<const typename Map::value_type*> sorted_by_value_resources(const Map& map) {
    std::vector<const typename Map::value_type*> sorted_values;
    sorted_values.reserve(map.size());

    for (const auto& val : map) {
        sorted_values.push_back(&val);
    }

    std::ranges::sort(sorted_values, [] (const typename Map::value_type* a, const typename Map::value_type* b) {
        return a->second.resources.memory < b->second.resources.memory;
    });

    return sorted_values;
}

static permit_stats do_dump_reader_permit_diagnostics(std::ostream& os, const permit_groups& permit_groups) {
    const auto sorted_permit_groups = sorted_by_value_resources(permit_groups);

    permit_stats total;

    auto print_line = [&os] (auto col1, auto col2, auto col3, auto col4) {
        fmt::print(os, "{}\t{}\t{}\t{}\n", col1, col2, col3, col4);
    };
    auto print_sub_line = [&os] (auto col1, auto col2, auto col3, auto col4) {
        fmt::print(os, "+ {}\t{}\t{}\t{}\n", col1, col2, col3, col4);
    };

    struct permit_state {
        reader_permit::state state;
        reader_permit::impl::ext_state ext_state;

        explicit permit_state(const reader_permit::impl& permit) : state(permit.get_state()), ext_state(permit.get_ext_state())
        { }

        bool operator<(const permit_state& o) const {
            if (state == o.state) {
                return ext_state < o.ext_state;
            }
            return state < o.state;
        }
    };
    using state_stats_type = std::map<permit_state, permit_stats>;

    print_line("permits", "memory", "count", "name");
    for (const auto group_ptr : sorted_permit_groups) {
        auto& [key, group] = *group_ptr;
        auto [group_schema, group_op_name] = key;

        total.add(group);

        print_line(
                group.permits.size(),
                utils::to_hr_size(group.resources.memory),
                group.resources.count,
                fmt::format("{}.{}:{}",
                        group_schema ? group_schema->ks_name() : "*",
                        group_schema ? group_schema->cf_name() : "*",
                        group_op_name));

        state_stats_type state_stats;
        for (const auto permit : group.permits) {
            state_stats[permit_state(*permit)].add(*permit);
        }

        const auto sorted_state_stats = sorted_by_value_resources(state_stats);

        for (const auto state_ptr : sorted_state_stats) {
            auto& [state_key, stats] = *state_ptr;
            print_sub_line(
                    stats.permits,
                    utils::to_hr_size(stats.resources.memory),
                    stats.resources.count,
                    fmt::format("{}/{}", state_key.state, state_key.ext_state));
        }
    }
    fmt::print(os, "\n");
    print_line(total.permits, utils::to_hr_size(total.resources.memory), total.resources.count, "total");
    return total;
}

static void do_dump_reader_permit_diagnostics(std::ostream& os, const reader_concurrency_semaphore& semaphore,
        const reader_concurrency_semaphore::permit_list& list, std::string_view problem) {
    permit_groups permits;

    for (const auto& permit : list.permits) {
        permits[permit_group_key(permit.get_schema(), permit.get_op_name())].add(permit);
    }

    permit_stats total;

    fmt::print(os, "Semaphore {}: {}, dumping permit diagnostics:\n", semaphore.name(), problem);
    total += do_dump_reader_permit_diagnostics(os, permits);
    fmt::print(os, "\n");
    fmt::print(
            os,
            "Total: {} permits ({} used, {} blocked), consuming {} count and {} memory resources\n",
            list.total_permits,
            list.used_permits,
            list.blocked_permits,
            total.resources.count,
            utils::to_hr_size(total.resources.memory));
}

static void maybe_dump_reader_permit_diagnostics(const reader_concurrency_semaphore& semaphore, const reader_concurrency_semaphore::permit_list& list,
        std::string_view problem) {
    static thread_local logger::rate_limit rate_limit(std::chrono::seconds(30));

    rcslog.log(log_level::info, rate_limit, "{}", value_of([&] {
        std::ostringstream os;
        do_dump_reader_permit_diagnostics(os, semaphore, list, problem);
        return os.str();
    }));
}

} // anonymous namespace

void reader_concurrency_semaphore::expiry_handler::operator()(entry& e) noexcept {
    e.pr.set_exception(named_semaphore_timed_out(_semaphore._name));

    maybe_dump_reader_permit_diagnostics(_semaphore, *_semaphore._permit_list, "timed out");
}

reader_concurrency_semaphore::inactive_read::~inactive_read() {
    detach();
}

void reader_concurrency_semaphore::inactive_read::detach() noexcept {
    if (handle) {
        handle->_irp = nullptr;
        handle = nullptr;
    }
}

void reader_concurrency_semaphore::inactive_read_handle::abandon() noexcept {
    delete std::exchange(_irp, nullptr);
}

void reader_concurrency_semaphore::signal(const resources& r) noexcept {
    _resources += r;
    maybe_admit_waiters();
}

reader_concurrency_semaphore::reader_concurrency_semaphore(int count, ssize_t memory, sstring name, size_t max_queue_length,
        std::function<void()> prethrow_action)
    : _initial_resources(count, memory)
    , _resources(count, memory)
    , _wait_list(expiry_handler(*this))
    , _name(std::move(name))
    , _max_queue_length(max_queue_length)
    , _prethrow_action(std::move(prethrow_action))
    , _permit_list(std::make_unique<permit_list>()) {}

reader_concurrency_semaphore::reader_concurrency_semaphore(no_limits, sstring name)
    : reader_concurrency_semaphore(
            std::numeric_limits<int>::max(),
            std::numeric_limits<ssize_t>::max(),
            std::move(name)) {}

reader_concurrency_semaphore::~reader_concurrency_semaphore() {
    broken(std::make_exception_ptr(broken_semaphore{}));
    clear_inactive_reads();
}

reader_concurrency_semaphore::inactive_read_handle reader_concurrency_semaphore::register_inactive_read(flat_mutation_reader reader) noexcept {
    auto& permit_impl = *reader.permit()._impl;
    // Implies _inactive_reads.empty(), we don't queue new readers before
    // evicting all inactive reads.
    // FIXME: #4758, workaround for keeping tabs on un-admitted reads that are
    // still registered as inactive. Without the below check, these can
    // accumulate without limit. The real fix is #4758 -- that is to make all
    // reads pass admission before getting started.
    if (_wait_list.empty() && (permit_impl.get_state() == reader_permit::state::admitted || _resources >= permit_impl.resources())) {
      try {
        auto irp = std::make_unique<inactive_read>(std::move(reader));
        auto& ir = *irp;
        _inactive_reads.push_back(ir);
        ++_stats.inactive_reads;
        permit_impl.on_register_as_inactive();
        return inactive_read_handle(*this, *irp.release());
      } catch (...) {
        // It is okay to swallow the exception since
        // we're allowed to drop the reader upon registration
        // due to lack of resources. Returning an empty
        // i_r_h here rather than throwing simplifies the caller's
        // error handling.
        rcslog.warn("Registering inactive read failed: {}. Ignored as if it was evicted.", std::current_exception());
      }
    } else {
        ++_stats.permit_based_evictions;
    }
    return inactive_read_handle();
}

void reader_concurrency_semaphore::set_notify_handler(inactive_read_handle& irh, eviction_notify_handler&& notify_handler, std::optional<std::chrono::seconds> ttl_opt) {
    auto& ir = *irh._irp;
    ir.notify_handler = std::move(notify_handler);
    if (ttl_opt) {
        ir.ttl_timer.set_callback([this, &ir] {
            evict(ir, evict_reason::time);
        });
        ir.ttl_timer.arm(lowres_clock::now() + *ttl_opt);
    }
}

flat_mutation_reader_opt reader_concurrency_semaphore::unregister_inactive_read(inactive_read_handle irh) {
    if (!irh) {
        return {};
    }
    if (irh._sem != this) {
        on_internal_error(rcslog, fmt::format(
                    "reader_concurrency_semaphore::unregister_inactive_read(): "
                    "attempted to unregister an inactive read with a handle belonging to another semaphore: "
                    "this is {} (0x{:x}) but the handle belongs to {} (0x{:x})",
                    name(),
                    reinterpret_cast<uintptr_t>(this),
                    irh._sem->name(),
                    reinterpret_cast<uintptr_t>(irh._sem)));
    }

    --_stats.inactive_reads;
    std::unique_ptr<inactive_read> irp(irh._irp);
    irp->reader.permit()._impl->on_unregister_as_inactive();
    return std::move(irp->reader);
}

bool reader_concurrency_semaphore::try_evict_one_inactive_read(evict_reason reason) {
    if (_inactive_reads.empty()) {
        return false;
    }
    evict(_inactive_reads.front(), reason);
    return true;
}

void reader_concurrency_semaphore::clear_inactive_reads() {
    while (!_inactive_reads.empty()) {
        // Destroying the read unlinks it too.
        std::unique_ptr<inactive_read> _(&*_inactive_reads.begin());
    }
}

void reader_concurrency_semaphore::evict(inactive_read& ir, evict_reason reason) noexcept {
    ir.detach();
    ir.reader.permit()._impl->on_evicted();
    std::unique_ptr<inactive_read> irp(&ir);
    try {
        if (ir.notify_handler) {
            ir.notify_handler(reason);
        }
    } catch (...) {
        rcslog.error("[semaphore {}] evict(): notify handler failed for inactive read evicted due to {}: {}", _name, reason, std::current_exception());
    }
    switch (reason) {
        case evict_reason::permit:
            ++_stats.permit_based_evictions;
            break;
        case evict_reason::time:
            ++_stats.time_based_evictions;
            break;
        case evict_reason::manual:
            break;
    }
    --_stats.inactive_reads;
}

bool reader_concurrency_semaphore::has_available_units(const resources& r) const {
    // Special case: when there is no active reader (based on count) admit one
    // regardless of availability of memory.
    return (bool(_resources) && _resources >= r) || _resources.count == _initial_resources.count;
}

bool reader_concurrency_semaphore::all_used_permits_are_stalled() const {
    return _permit_list->blocked_permits >= _permit_list->used_permits;
}

future<reader_permit::resource_units> reader_concurrency_semaphore::enqueue_waiter(reader_permit permit, resources r,
        db::timeout_clock::time_point timeout) {
    if (_wait_list.size() >= _max_queue_length) {
        _stats.total_reads_shed_due_to_overload++;
        if (_prethrow_action) {
            _prethrow_action();
        }
        maybe_dump_reader_permit_diagnostics(*this, *_permit_list, "wait queue overloaded");
        return make_exception_future<reader_permit::resource_units>(
                std::make_exception_ptr(std::runtime_error(
                        format("{}: restricted mutation reader queue overload", _name))));
    }

    promise<reader_permit::resource_units> pr;
    auto fut = pr.get_future();
    permit.on_waiting();
    _wait_list.push_back(entry(std::move(pr), std::move(permit), r), timeout);
    return fut;
}

future<reader_permit::resource_units> reader_concurrency_semaphore::do_wait_admission(reader_permit permit, size_t memory,
        db::timeout_clock::time_point timeout) {
    auto r = resources(1, static_cast<ssize_t>(memory));
    if (permit._impl->get_state() == reader_permit::state::admitted) {
        return make_ready_future<reader_permit::resource_units>(reader_permit::resource_units(std::move(permit), r));
    }

    if (!_wait_list.empty()) {
        return enqueue_waiter(std::move(permit), r, timeout);
    }

    if (!all_used_permits_are_stalled()) {
        return enqueue_waiter(std::move(permit), r, timeout);
    }

    while (!has_available_units(r)) {
        if (!try_evict_one_inactive_read(evict_reason::permit)) {
            return enqueue_waiter(std::move(permit), r, timeout);
        }
    }

    permit.on_admission();
    return make_ready_future<reader_permit::resource_units>(reader_permit::resource_units(std::move(permit), r));
}

void reader_concurrency_semaphore::maybe_admit_waiters() noexcept {
    while (!_wait_list.empty() && can_admit(_wait_list.front().res)) {
        auto& x = _wait_list.front();
        try {
            x.permit.on_admission();
            x.pr.set_value(reader_permit::resource_units(std::move(x.permit), x.res));
        } catch (...) {
            x.pr.set_exception(std::current_exception());
        }
        _wait_list.pop_front();
    }
}

void reader_concurrency_semaphore::on_permit_created(reader_permit::impl& permit) noexcept {
    _permit_list->permits.push_back(permit);
    ++_permit_list->total_permits;
}

void reader_concurrency_semaphore::on_permit_destroyed(reader_permit::impl& permit) noexcept {
    permit.unlink();
    --_permit_list->total_permits;
}

void reader_concurrency_semaphore::on_permit_used() noexcept {
    ++_permit_list->used_permits;
}

void reader_concurrency_semaphore::on_permit_unused() noexcept {
    assert(_permit_list->used_permits);
    --_permit_list->used_permits;
    assert(_permit_list->used_permits >= _permit_list->blocked_permits);
    maybe_admit_waiters();
}

void reader_concurrency_semaphore::on_permit_blocked() noexcept {
    ++_permit_list->blocked_permits;
    assert(_permit_list->used_permits >= _permit_list->blocked_permits);
    maybe_admit_waiters();
}

void reader_concurrency_semaphore::on_permit_unblocked() noexcept {
    assert(_permit_list->blocked_permits);
    --_permit_list->blocked_permits;
}

reader_permit reader_concurrency_semaphore::make_permit(const schema* const schema, const char* const op_name) {
    return reader_permit(*this, schema, std::string_view(op_name));
}

reader_permit reader_concurrency_semaphore::make_permit(const schema* const schema, sstring&& op_name) {
    return reader_permit(*this, schema, std::move(op_name));
}

future<reader_permit> reader_concurrency_semaphore::obtain_permit(const schema* const schema, const char* const op_name, size_t memory,
        db::timeout_clock::time_point timeout) {
    auto permit = reader_permit(*this, schema, std::string_view(op_name));
    return do_wait_admission(permit, memory, timeout).then([permit] (reader_permit::resource_units res) mutable {
        permit.incorporate(std::move(res));
        return std::move(permit);
    });
}

future<reader_permit> reader_concurrency_semaphore::obtain_permit(const schema* const schema, sstring&& op_name, size_t memory,
        db::timeout_clock::time_point timeout) {
    auto permit = reader_permit(*this, schema, std::move(op_name));
    return do_wait_admission(permit, memory, timeout).then([permit] (reader_permit::resource_units res) mutable {
        permit.incorporate(std::move(res));
        return std::move(permit);
    });
}

reader_permit reader_concurrency_semaphore::make_tracking_only_permit(const schema* const schema, const char* const op_name) {
    return reader_permit(*this, schema, std::string_view(op_name));
}

reader_permit reader_concurrency_semaphore::make_tracking_only_permit(const schema* const schema, sstring&& op_name) {
    return reader_permit(*this, schema, std::move(op_name));
}

void reader_concurrency_semaphore::broken(std::exception_ptr ex) {
    while (!_wait_list.empty()) {
        _wait_list.front().pr.set_exception(std::make_exception_ptr(broken_semaphore{}));
        _wait_list.pop_front();
    }
}

std::string reader_concurrency_semaphore::dump_diagnostics() const {
    std::ostringstream os;
    do_dump_reader_permit_diagnostics(os, *this, *_permit_list, "user request");
    return os.str();
}

bool reader_concurrency_semaphore::can_admit(reader_resources res) const {
    return has_available_units(res) && all_used_permits_are_stalled();
}

// A file that tracks the memory usage of buffers resulting from read
// operations.
class tracking_file_impl : public file_impl {
    file _tracked_file;
    reader_permit _permit;

public:
    tracking_file_impl(file file, reader_permit permit)
        : file_impl(*get_file_impl(file))
        , _tracked_file(std::move(file))
        , _permit(std::move(permit)) {
    }

    tracking_file_impl(const tracking_file_impl&) = delete;
    tracking_file_impl& operator=(const tracking_file_impl&) = delete;
    tracking_file_impl(tracking_file_impl&&) = default;
    tracking_file_impl& operator=(tracking_file_impl&&) = default;

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->write_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->write_dma(pos, std::move(iov), pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->read_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->read_dma(pos, iov, pc);
    }

    virtual future<> flush(void) override {
        return get_file_impl(_tracked_file)->flush();
    }

    virtual future<struct stat> stat(void) override {
        return get_file_impl(_tracked_file)->stat();
    }

    virtual future<> truncate(uint64_t length) override {
        return get_file_impl(_tracked_file)->truncate(length);
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return get_file_impl(_tracked_file)->discard(offset, length);
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return get_file_impl(_tracked_file)->allocate(position, length);
    }

    virtual future<uint64_t> size(void) override {
        return get_file_impl(_tracked_file)->size();
    }

    virtual future<> close() override {
        return get_file_impl(_tracked_file)->close();
    }

    virtual std::unique_ptr<file_handle_impl> dup() override {
        return get_file_impl(_tracked_file)->dup();
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return get_file_impl(_tracked_file)->list_directory(std::move(next));
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->dma_read_bulk(offset, range_size, pc).then([this, units = _permit.consume_memory(range_size)] (temporary_buffer<uint8_t> buf) {
            return make_ready_future<temporary_buffer<uint8_t>>(make_tracked_temporary_buffer(std::move(buf), _permit));
        });
    }
};

file make_tracked_file(file f, reader_permit p) {
    return file(make_shared<tracking_file_impl>(f, std::move(p)));
}
