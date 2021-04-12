/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "cql3/values.hh"

namespace cql3 {

std::ostream& operator<<(std::ostream& os, const raw_value_view& value) {
    seastar::visit(value._data, [&] (fragmented_temporary_buffer::view v) {
        os << "{ value: ";
        using boost::range::for_each;
        for_each(v, [&os] (bytes_view bv) { os << bv; });
        os << " }";
    }, [&] (null_value) {
        os << "{ null }";
    }, [&] (unset_value) {
        os << "{ unset }";
    });
    return os;
}

raw_value_view raw_value::to_view() const {
    switch (_data.index()) {
    case 0:  return raw_value_view::make_value(fragmented_temporary_buffer::view(bytes_view{std::get<bytes>(_data)}));
    case 1:  return raw_value_view::make_null();
    default: return raw_value_view::make_unset_value();
    }
}

raw_value raw_value::make_value(const raw_value_view& view) {
    if (view.is_null()) {
        return make_null();
    }
    if (view.is_unset_value()) {
        return make_unset_value();
    }
    return make_value(linearized(*view));
}

raw_value_view raw_value_view::make_temporary(raw_value&& value) {
    if (!value) {
        return raw_value_view::make_null();
    }
    return raw_value_view(std::move(value).extract_value());
}

raw_value_view::raw_value_view(bytes&& tmp) {
    _temporary_storage = make_lw_shared<bytes>(std::move(tmp));
    _data = fragmented_temporary_buffer::view(bytes_view(*_temporary_storage));
}

}