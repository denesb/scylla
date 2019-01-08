/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "range.hh"
#include <seastar/core/print.hh>

using int_range = nonwrapping_range<int>;

inline
unsigned cardinality(const int_range& r) {
    assert(r.start());
    assert(r.end());
    return r.end()->value() - r.start()->value() + r.start()->is_inclusive() + r.end()->is_inclusive() - 1;
}

inline
unsigned cardinality(const std::optional<int_range>& ropt) {
    return ropt ? cardinality(*ropt) : 0;
}

inline
std::optional<int_range> intersection(const int_range& a, const int_range& b) {
    auto int_tri_cmp = [] (int x, int y) {
        return x < y ? -1 : (x > y ? 1 : 0);
    };
    return a.intersection(b, int_tri_cmp);
}

inline
int_range make_int_range(int start_inclusive, int end_exclusive) {
    if (end_exclusive <= start_inclusive) {
        throw std::runtime_error(format("invalid range: [{:d}, {:d})", start_inclusive, end_exclusive));
    }
    return int_range({start_inclusive}, {end_exclusive - 1});
}
