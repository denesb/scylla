/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/util/bool_class.hh>

#include "mutation/tombstone.hh"
#include "schema/schema_fwd.hh"
#include "dht/i_partitioner_fwd.hh"

class tombstone_gc_state;

using is_shadowable = bool_class<struct is_shadowable_tag>;

// Determines whether tombstone may be GC-ed.
using can_gc_fn = std::function<bool(tombstone, is_shadowable)>;

extern can_gc_fn always_gc;
extern can_gc_fn never_gc;

struct max_purgeable {
    enum class timestamp_source {
        none,
        memtable_possibly_shadowing_data,
        other_sstables_possibly_shadowing_data
    };

    operator bool() const { return timestamp != api::missing_timestamp; }

    api::timestamp_type timestamp { api::missing_timestamp };
    timestamp_source source { timestamp_source::none };
};

using max_purgeable_fn = std::function<max_purgeable(const dht::decorated_key&, is_shadowable)>;

extern max_purgeable_fn can_always_purge;
extern max_purgeable_fn can_never_purge;

// Unified interface to check whether a tombstone can be garbage collected.
//
// Encapsulates both checks necessariy to determine this:
// * expiry check - tombstone_gc_state (also abstracted as can_gc_fn)
// * overlap check - max_purgeable_fn
//
// TODO: the encapsulation is incomplete for now, further refactoring is needed
// to be able to replace the usage of the independent use of the two checks with
// that of this class.
//
// See "Tombstone Garbage Collection" in docs/dev/tombstone_gc.md for more details.
class tombstone_gc {
    const tombstone_gc_state* _gc_state;
    max_purgeable_fn _get_max_purgeable;

private:
    tombstone_gc(const tombstone_gc_state* gc_state, max_purgeable_fn get_max_purgeable)
        : _gc_state(gc_state)
        , _get_max_purgeable(std::move(get_max_purgeable)) {
    }

public:
    tombstone_gc(const tombstone_gc_state& gc_state, max_purgeable_fn get_max_purgeable)
        : tombstone_gc(&gc_state, std::move(get_max_purgeable))
    { }

    static tombstone_gc disabled() {
        return tombstone_gc(nullptr, can_never_purge);
    }

    operator bool () const noexcept {
        return _gc_state != nullptr;
    }

    const tombstone_gc_state& get_tombstone_gc_state() const noexcept {
        return *_gc_state;
    }
    max_purgeable_fn get_max_purgeable_fn() const noexcept {
        return _get_max_purgeable;
    }
};

class atomic_cell;
class row_marker;
struct collection_mutation_description;

class compaction_garbage_collector {
public:
    virtual ~compaction_garbage_collector() = default;
    virtual void collect(column_id id, atomic_cell) = 0;
    virtual void collect(column_id id, collection_mutation_description) = 0;
    virtual void collect(row_marker) = 0;
};
