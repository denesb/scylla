/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "replica/database.hh"
#include "service/storage_proxy.hh"
#include "partition_slice_builder.hh"

namespace service {

/*
bool is_view_key_identical_to_base_key(const schema& view_schema, const schema& base_table_schema) {
    // TODO
    return true;
}
*/

future<exceptions::coordinator_result<storage_proxy_coordinator_query_result>> query_nonmaterialized_view(
        storage_proxy& sp,
        schema_ptr query_schema,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl,
        storage_proxy_coordinator_query_options optional_params) {

    const table_id base_table_id; /* = query_schema.get_base_table_id(); */
    auto& base_table = sp.local_db().find_column_family(base_table_id);
    // TODO: reverse if query_schema is reversed
    auto base_query_schema = base_table.schema();

    auto base_table_slice = partition_slice_builder(*base_query_schema, cmd->slice).build();

    auto base_table_cmd = make_lw_shared<query::read_command>(*cmd);
    base_table_cmd->cf_id = base_query_schema->id();
    base_table_cmd->schema_version = base_query_schema->version();
    base_table_cmd->slice = std::move(base_table_slice);

    return sp.query_result(std::move(base_query_schema), std::move(base_table_cmd), std::move(partition_ranges), cl, std::move(optional_params));
}

} // namespace service
