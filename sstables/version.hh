/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <stdexcept>
#include <type_traits>
#include <seastar/core/sstring.hh>

namespace sstables {

enum class sstable_version_types { ka, la, mc };
enum class sstable_format_types { big };

inline std::array<sstable_version_types, 3> all_sstable_versions = {
    sstable_version_types::ka,
    sstable_version_types::la,
    sstable_version_types::mc,
};

inline sstable_version_types from_string(const seastar::sstring& format) {
    if (format == "ka") {
        return sstable_version_types::ka;
    }
    if (format == "la") {
        return sstable_version_types::la;
    }
    if (format == "mc") {
        return sstable_version_types::mc;
    }
    throw std::invalid_argument("Wrong sstable format name: " + format);
}

inline seastar::sstring to_string(sstable_version_types format) {
    switch (format) {
        case sstable_version_types::ka: return "ka";
        case sstable_version_types::la: return "la";
        case sstable_version_types::mc: return "mc";
    }
    throw std::runtime_error("Wrong sstable format");
}

inline bool is_latest_supported(sstable_version_types format) {
    return format == sstable_version_types::mc;
}

inline int operator<=>(sstable_version_types a, sstable_version_types b) {
    auto to_int = [] (sstable_version_types x) {
        return static_cast<std::underlying_type_t<sstable_version_types>>(x);
    };
    return to_int(a) - to_int(b);
}

}
