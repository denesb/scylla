/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "exceptions/coordinator_result.hh"
#include "query-request.hh"
#include "schema/schema_fwd.hh"

namespace service {

class storage_proxy;
class storage_proxy_coordinator_query_options;
class storage_proxy_coordinator_query_result;

future<exceptions::coordinator_result<storage_proxy_coordinator_query_result>> query_nonmaterialized_view(
        storage_proxy& sp,
        schema_ptr query_schema,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl,
        storage_proxy_coordinator_query_options optional_params);

} // namespace service
