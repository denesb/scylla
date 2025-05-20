# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for materialized views

import pytest

from .util import new_test_table, new_nonmaterialized_view

def values(res):
    return [r._asdic().values() for r in res]

def test_nonmaterialized_view_exclude_one_column(cql, test_keyspace):
    schema = 'pk int, ck int, v1 text, v2 text, primary key (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        expected_data = []

        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES (?, ?, ?, ?)")

        for ck in range(0, 10):
            row = (1, ck, 'v1-{ck}', 'v2-{ck}')
            cql.execute(insert_stmt, row)
            rows.append(row)

        with new_nonmaterialized_view(cql, table, "pk, ck, vq") as nmv:
            assert values(cql.execute(f"SELECT * FROM {nmv}")) == rows
