/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "cql3/attributes.hh"

namespace cql3 {

std::unique_ptr<attributes> attributes::none() {
    return std::unique_ptr<attributes>{new attributes{{}, {}, {}}};
}

attributes::attributes(::shared_ptr<term>&& timestamp, ::shared_ptr<term>&& time_to_live, ::shared_ptr<term>&& timeout)
    : _timestamp{std::move(timestamp)}
    , _time_to_live{std::move(time_to_live)}
    , _timeout{std::move(timeout)}
{ }

bool attributes::is_timestamp_set() const {
    return bool(_timestamp);
}

bool attributes::is_time_to_live_set() const {
    return bool(_time_to_live);
}

bool attributes::is_timeout_set() const {
    return bool(_timeout);
}

int64_t attributes::get_timestamp(int64_t now, const query_options& options) {
    if (!_timestamp) {
        return now;
    }

    auto tval = _timestamp->bind_and_get(options);
    if (tval.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value of timestamp");
    }
    if (tval.is_unset_value()) {
        return now;
    }
  return with_linearized(*tval, [&] (bytes_view val) {
    try {
        data_type_for<int64_t>()->validate(val, options.get_cql_serialization_format());
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception("Invalid timestamp value");
    }
    return value_cast<int64_t>(data_type_for<int64_t>()->deserialize(val));
  });
}

int32_t attributes::get_time_to_live(const query_options& options) {
    if (!_time_to_live)
        return 0;

    auto tval = _time_to_live->bind_and_get(options);
    if (tval.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value of TTL");
    }
    if (tval.is_unset_value()) {
        return 0;
    }
  auto ttl = with_linearized(*tval, [&] (bytes_view val) {
    try {
        data_type_for<int32_t>()->validate(val, options.get_cql_serialization_format());
    }
    catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception("Invalid TTL value");
    }

    return value_cast<int32_t>(data_type_for<int32_t>()->deserialize(val));
  });
    if (ttl < 0) {
        throw exceptions::invalid_request_exception("A TTL must be greater or equal to 0");
    }

    if (ttl > max_ttl.count()) {
        throw exceptions::invalid_request_exception("ttl is too large. requested (" + std::to_string(ttl) +
            ") maximum (" + std::to_string(max_ttl.count()) + ")");
    }

    return ttl;
}


db::timeout_clock::duration attributes::get_timeout(const query_options& options) const {
    auto timeout = _timeout->bind_and_get(options);
    if (timeout.is_null() || timeout.is_unset_value()) {
        throw exceptions::invalid_request_exception("Timeout value cannot be unset/null");
    }
  return with_linearized(*timeout, [&] (bytes_view val) {
    cql_duration duration = value_cast<cql_duration>(duration_type->deserialize(val));
    if (duration.months || duration.days) {
        throw exceptions::invalid_request_exception("Timeout values cannot be expressed in days/months");
    }
    if (duration.nanoseconds % 1'000'000 != 0) {
        throw exceptions::invalid_request_exception("Timeout values cannot have granularity finer than milliseconds");
    }
    if (duration.nanoseconds < 0) {
        throw exceptions::invalid_request_exception("Timeout values must be non-negative");
    }
    return std::chrono::duration_cast<db::timeout_clock::duration>(std::chrono::nanoseconds(duration.nanoseconds));
  });
}

void attributes::collect_marker_specification(variable_specifications& bound_names) const {
    if (_timestamp) {
        _timestamp->collect_marker_specification(bound_names);
    }
    if (_time_to_live) {
        _time_to_live->collect_marker_specification(bound_names);
    }
    if (_timeout) {
        _timeout->collect_marker_specification(bound_names);
    }
}

std::unique_ptr<attributes> attributes::raw::prepare(database& db, const sstring& ks_name, const sstring& cf_name) const {
    auto ts = !timestamp ? ::shared_ptr<term>{} : timestamp->prepare(db, ks_name, timestamp_receiver(ks_name, cf_name));
    auto ttl = !time_to_live ? ::shared_ptr<term>{} : time_to_live->prepare(db, ks_name, time_to_live_receiver(ks_name, cf_name));
    auto to = !timeout ? ::shared_ptr<term>{} : timeout->prepare(db, ks_name, timeout_receiver(ks_name, cf_name));
    return std::unique_ptr<attributes>{new attributes{std::move(ts), std::move(ttl), std::move(to)}};
}

lw_shared_ptr<column_specification> attributes::raw::timestamp_receiver(const sstring& ks_name, const sstring& cf_name) const {
    return make_lw_shared<column_specification>(ks_name, cf_name, ::make_shared<column_identifier>("[timestamp]", true), data_type_for<int64_t>());
}

lw_shared_ptr<column_specification> attributes::raw::time_to_live_receiver(const sstring& ks_name, const sstring& cf_name) const {
    return make_lw_shared<column_specification>(ks_name, cf_name, ::make_shared<column_identifier>("[ttl]", true), data_type_for<int32_t>());
}

lw_shared_ptr<column_specification> attributes::raw::timeout_receiver(const sstring& ks_name, const sstring& cf_name) const {
    return make_lw_shared<column_specification>(ks_name, cf_name, ::make_shared<column_identifier>("[timeout]", true), duration_type);
}

}
