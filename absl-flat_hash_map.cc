/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "absl-flat_hash_map.hh"

size_t sstring_hash::operator()(std::string_view v) const noexcept {
    return absl::Hash<std::string_view>{}(v);
}
