/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "schema/schema.hh"

//TODO - just a skeleton for now
class nonmaterialized_view_info final {
    const schema& _schema;
    raw_nonmaterialized_view_info _raw;
public:
    nonmaterialized_view_info(const schema& schema, const raw_nonmaterialized_view_info& raw_view_info);

    const raw_nonmaterialized_view_info& raw() const {
        return _raw;
    }

    friend bool operator==(const nonmaterialized_view_info& x, const nonmaterialized_view_info& y) {
        return x._raw == y._raw;
    }

    friend fmt::formatter<nonmaterialized_view_info>;
};

template <> struct fmt::formatter<nonmaterialized_view_info> : fmt::formatter<string_view> {
    auto format(const nonmaterialized_view_info& view, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", view._raw);
    }
};
