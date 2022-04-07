/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "tombstone.hh"

class flat_mutation_reader_v2;

using stream_id_t = const flat_mutation_reader_v2*;

// Merges range tombstone changes coming from different streams (readers).
//
// Add tombstones by calling apply().
// Updates to the same stream identified by stream_id overwrite each other.
// Applying an empty tombstone to a stream removes said stream.
//
// Call get() to get the tombstone with the highest timestamp from the currently
// active ones.
// Get returns disengaged optional when the last apply() call(s) didn't introduce
// a change in the max tombstone.
//
// Call clear() when leaving the range abruptly (next_partition() or
// fast_forward_to()).
//
// The merger doesn't keep track of the position component of the tombstones.
// Emit them with the current position.
class range_tombstone_change_merger {
    struct stream_tombstone {
        uint64_t stream_id;
        ::tombstone tombstone;
    };

private:
    std::vector<stream_tombstone> _tombstones;
    tombstone _current_tombstone;

private:
    const stream_tombstone* get_tombstone() const {
        const stream_tombstone* max_tomb{nullptr};

        for (const auto& tomb : _tombstones) {
            if (!max_tomb || tomb.tombstone > max_tomb->tombstone) {
                max_tomb = &tomb;
            }
        }
        return max_tomb;
    }

public:
    void apply(uint64_t stream_id, tombstone tomb) {
        auto it = std::find_if(_tombstones.begin(), _tombstones.end(), [&] (const stream_tombstone& tomb) {
            return tomb.stream_id == stream_id;
        });
        if (it == _tombstones.end()) {
            if (tomb) {
                _tombstones.push_back({stream_id, tomb});
            }
        } else {
            if (tomb) {
                it->tombstone = tomb;
            } else {
                auto last = _tombstones.end() - 1;
                if (it != last) {
                    std::swap(*it, *last);
                }
                _tombstones.pop_back();
            }
        }
    }
    void apply(stream_id_t stream_id, tombstone tomb) {
        return apply(reinterpret_cast<uint64_t>(stream_id), tomb);
    }

    std::optional<tombstone> get() {
        const auto* tomb = get_tombstone();
        if (tomb && tomb->tombstone == _current_tombstone) {
            return {};
        } else {
            _current_tombstone = tomb ? tomb->tombstone : tombstone();
            return _current_tombstone;
        }
    }

    void clear() {
        _tombstones.clear();
        _current_tombstone = {};
    }
};


