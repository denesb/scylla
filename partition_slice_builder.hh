/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <experimental/optional>
#include <vector>

#include "query-request.hh"
#include "schema.hh"

//
// Fluent builder for query::partition_slice.
//
// Selects everything by default, unless restricted. Each property can be
// restricted separately. For example, by default all static columns are
// selected, but if with_static_column() is called then only that column will
// be included. Still, all regular columns and the whole clustering range will
// be selected (unless restricted).
//
class partition_slice_builder {
    std::experimental::optional<query::column_id_vector> _regular_columns;
    std::experimental::optional<query::column_id_vector> _static_columns;
    std::experimental::optional<std::vector<query::clustering_range>> _row_ranges;
    const schema& _schema;
    query::partition_slice::option_set _options;
public:
    partition_slice_builder(const schema& schema);

    partition_slice_builder& with_static_column(bytes name);
    partition_slice_builder& with_no_static_columns();
    partition_slice_builder& with_regular_column(bytes name);
    partition_slice_builder& with_no_regular_columns();
    partition_slice_builder& with_range(query::clustering_range range);
    partition_slice_builder& with_ranges(std::vector<query::clustering_range>);
    partition_slice_builder& without_partition_key_columns();
    partition_slice_builder& without_clustering_key_columns();
    partition_slice_builder& reversed();

    query::partition_slice build();
};
