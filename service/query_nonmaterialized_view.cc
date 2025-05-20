/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "nonmaterialized_view_info.hh"
#include "partition_slice_builder.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"

static logging::logger nmv_log("query_nonmaterialized_view");

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

    auto& db = sp.local_db();

    auto& view_table = db.find_column_family(query_schema->id());

    auto view_schema = view_table.schema();
    if (!view_schema->is_nonmaterialized_view()) {
        on_internal_error(nmv_log, format("table {}.{} ({}) is not a non-materialized view", view_schema->ks_name(), view_schema->cf_name(), view_schema->id()));
    }

    //const table_id base_table_id = view_schema->nonmaterialized_view_info()->raw().base_id();
    const table_id base_table_id = db.find_column_family(view_schema->ks_name(), view_schema->nonmaterialized_view_info()->raw().base_name()).schema()->id();
    auto& base_table = db.find_column_family(base_table_id);

    auto base_query_schema = base_table.schema();
    if (cmd->slice.is_reversed()) {
        base_query_schema = base_query_schema->get_reversed();
    }

    auto base_table_slice = partition_slice_builder(*base_query_schema, cmd->slice).build();

    auto base_table_cmd = make_lw_shared<query::read_command>(*cmd);
    base_table_cmd->cf_id = base_query_schema->id();
    base_table_cmd->schema_version = base_query_schema->version();
    base_table_cmd->slice = std::move(base_table_slice);

    return sp.query_result(std::move(base_query_schema), std::move(base_table_cmd), std::move(partition_ranges), cl, std::move(optional_params));
}

} // namespace service
