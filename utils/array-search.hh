/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <cstdint>
#include <limits>

namespace utils {

static constexpr int64_t simple_key_unused_value = std::numeric_limits<int64_t>::min();

/*
 * array_search_gt(value, array, capacity, size)
 *
 * Returns the index of the first element in the array that's greater
 * than the given value.
 *
 * To accomodate the single-instruction-multiple-data variant, observe
 * the following:
 *  - capacity must be a multiple of 4
 *  - any items with indexes in [size, capacity) must be initialized
 *    to std::numeric_limits<int64_t>::min()
 */
int array_search_gt(int64_t val, const int64_t* array, const int capacity, const int size);

}
