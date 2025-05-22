# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for materialized views

import pytest

from .util import new_test_table, new_nonmaterialized_view, create_nonmaterialized_view

def values(res):
    return [tuple(r._asdict().values()) for r in res]

def test_exclude_one_regular_column(cql, test_keyspace):
    schema = 'pk int, ck int, v1 text, v2 text, primary key (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        expected_data = []

        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES (?, ?, ?, ?)")

        rows = []
        view_rows_v1 = []
        view_rows_v2 = []

        for ck in range(0, 10):
            row = (1, ck, f'v1-{ck}', f'v2-{ck}')
            cql.execute(insert_stmt, row)
            rows.append(row)
            view_rows_v1.append((1, ck, f'v1-{ck}'))
            view_rows_v2.append((1, ck, f'v2-{ck}'))

        assert values(cql.execute(f"SELECT * FROM {table}")) == rows

        print(f"rows: {rows}")
        print(f"view_rows_v1: {view_rows_v1}")
        print(f"view_rows_v2: {view_rows_v2}")

        with new_nonmaterialized_view(cql, table, "pk, ck, v1") as nmv:
            assert values(cql.execute(f"SELECT * FROM {nmv}")) == view_rows_v1

        with new_nonmaterialized_view(cql, table, "pk, ck, v2") as nmv:
            assert values(cql.execute(f"SELECT * FROM {nmv}")) == view_rows_v2
            assert values(cql.execute(f"SELECT * FROM {nmv} WHERE pk = 1")) == view_rows_v2
            assert values(cql.execute(f"SELECT pk, ck FROM {nmv} WHERE pk = 1")) == [r[0:2] for r in view_rows_v2]
            assert values(cql.execute(f"SELECT v2 FROM {nmv} WHERE pk = 1")) == [(r[2],) for r in view_rows_v2]


def test_exclude_all_regular_columns(cql, test_keyspace):
    schema = 'pk int, ck int, v1 text, v2 text, primary key (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        expected_data = []

        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES (?, ?, ?, ?)")

        rows = []
        view_rows = []

        for ck in range(0, 10):
            row = (1, ck, f'v1-{ck}', f'v2-{ck}')
            cql.execute(insert_stmt, row)
            rows.append(row)
            view_rows.append(row[:-2])
        print(f"rows: {rows}")
        print(f"view_rows: {view_rows}")
        with new_nonmaterialized_view(cql, table, "pk, ck") as nmv:
            assert values(cql.execute(f"SELECT * FROM {table}")) == rows
            assert values(cql.execute(f"SELECT * FROM {nmv}")) == view_rows
            assert values(cql.execute(f"SELECT * FROM {nmv} WHERE pk = 1")) == view_rows
            assert values(cql.execute(f"SELECT ck FROM {nmv} WHERE pk = 1")) == [(r[1],) for r in view_rows]


def test_exclude_one_static_column(cql, test_keyspace):
    schema = 'pk int, ck int, s1 text static, s2 text static, v text, primary key (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        expected_data = []

        cql.execute(f"INSERT INTO {table} (pk, s1, s2) VALUES (9, 's1', 's2')")

        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")

        rows = []
        view_rows_s1 = []
        view_rows_s2 = []

        for ck in range(0, 10):
            cql.execute(insert_stmt, (9, ck, f'v-{ck}'))
            rows.append((9, ck, 's1', 's2', f'v-{ck}'))
            view_rows_s1.append((9, ck, 's1', f'v-{ck}'))
            view_rows_s2.append((9, ck, 's2', f'v-{ck}'))

        assert values(cql.execute(f"SELECT * FROM {table}")) == rows

        with new_nonmaterialized_view(cql, table, "pk, ck, s1, v") as nmv:
            assert values(cql.execute(f"SELECT * FROM {nmv}")) == view_rows_s1
            assert values(cql.execute(f"SELECT * FROM {nmv} WHERE pk = 9")) == view_rows_s1
            assert values(cql.execute(f"SELECT s1, v FROM {nmv} WHERE pk = 9")) == [r[-2:] for r in view_rows_s1]

        with new_nonmaterialized_view(cql, table, "pk, ck, s2, v") as nmv:
            assert values(cql.execute(f"SELECT * FROM {nmv}")) == view_rows_s2
            assert values(cql.execute(f"SELECT * FROM {nmv} WHERE pk = 9")) == view_rows_s2
            assert values(cql.execute(f"SELECT s2, v FROM {nmv} WHERE pk = 9")) == [r[-2:] for r in view_rows_s2]


def test_exclude_all_static_columns(cql, test_keyspace):
    schema = 'pk int, ck int, s1 text static, s2 text static, v text, primary key (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        expected_data = []

        cql.execute(f"INSERT INTO {table} (pk, s1, s2) VALUES (9, 's1', 's2')")

        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")

        rows = []
        view_rows = []

        for ck in range(0, 10):
            cql.execute(insert_stmt, (9, ck, f'v-{ck}'))
            rows.append((9, ck, 's1', 's2', f'v-{ck}'))
            view_rows.append((9, ck, f'v-{ck}'))

        assert values(cql.execute(f"SELECT * FROM {table}")) == rows

        with new_nonmaterialized_view(cql, table, "pk, ck, v") as nmv:
            assert values(cql.execute(f"SELECT * FROM {nmv}")) == view_rows
            assert values(cql.execute(f"SELECT * FROM {nmv} WHERE pk = 9")) == view_rows
            assert values(cql.execute(f"SELECT v FROM {nmv} WHERE pk = 9")) == [(r[2],) for r in view_rows]


def test_exclude_all_regular_and_static_columns(cql, test_keyspace):
    schema = 'pk int, ck int, s1 text static, s2 text static, v1 text, v2 text, primary key (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        expected_data = []

        cql.execute(f"INSERT INTO {table} (pk, s1, s2) VALUES (9, 's1', 's2')")

        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES (?, ?, ?, ?)")

        rows = []
        view_rows = []

        for ck in range(0, 10):
            cql.execute(insert_stmt, (9, ck, f'v1-{ck}', f'v2-{ck}'))
            rows.append((9, ck, 's1', 's2', f'v1-{ck}', f'v2-{ck}'))
            view_rows.append((9, ck))

        assert values(cql.execute(f"SELECT * FROM {table}")) == rows

        with new_nonmaterialized_view(cql, table, "pk, ck") as nmv:
            assert values(cql.execute(f"SELECT * FROM {nmv}")) == view_rows
            assert values(cql.execute(f"SELECT * FROM {nmv} WHERE pk = 9")) == view_rows
            assert values(cql.execute(f"SELECT ck FROM {nmv} WHERE pk = 9")) == [(r[1],) for r in view_rows]

def test_drop_nonmaterialized_view(cql, test_keyspace):
    schema = 'pk int, ck int, v1 text, v2 text, primary key (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        expected_data = []

        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES (?, ?, ?, ?)")

        rows = []
        view_rows_v1 = []
        view_rows_v2 = []

        for ck in range(0, 10):
            row = (1, ck, f'v1-{ck}', f'v2-{ck}')
            cql.execute(insert_stmt, row)
            rows.append(row)
            view_rows_v1.append((1, ck, f'v1-{ck}'))
            view_rows_v2.append((1, ck, f'v2-{ck}'))

        assert values(cql.execute(f"SELECT * FROM {table}")) == rows

        nmv = create_nonmaterialized_view(cql, table, "pk, ck, v1")
        res = values(cql.execute(f"SELECT * FROM system_schema.nonmaterialized_views"))
        assert len(res) == 1
        print(f"{nmv=}")
        print(f"{res=}")
        cql.execute(f"DROP NONMATERIALIZED VIEW {nmv}")
        res = values(cql.execute(f"SELECT * FROM system_schema.nonmaterialized_views"))
        assert len(res) == 0
        print(f"{res=}")

def test_exclude_keys(cql, test_keyspace):
    schema = 'pk int, ck int, s1 text static, s2 text static, v1 text, v2 text, primary key (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        expected_data = []

        cql.execute(f"INSERT INTO {table} (pk, s1, s2) VALUES (9, 's1', 's2')")

        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES (?, ?, ?, ?)")

        rows = []
        view_rows = []

        for ck in range(0, 10):
            cql.execute(insert_stmt, (9, ck, f'v1-{ck}', f'v2-{ck}'))
            rows.append((9, ck, 's1', 's2', f'v1-{ck}', f'v2-{ck}'))
            view_rows.append(('s1', f'v1-{ck}'))

        assert values(cql.execute(f"SELECT * FROM {table}")) == rows

        with new_nonmaterialized_view(cql, table, "s1, v1") as nmv:
            assert values(cql.execute(f"SELECT * FROM {nmv}")) == view_rows

def test_column_selection(cql, test_keyspace):
    schema = 'pk int, ck int, s1 text static, s2 text static, v1 text, v2 text, primary key (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        expected_data = []

        cql.execute(f"INSERT INTO {table} (pk, s1, s2) VALUES (9, 's1', 's2')")

        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES (?, ?, ?, ?)")

        rows = []
        view_rows = []

        for ck in range(0, 10):
            cql.execute(insert_stmt, (9, ck, f'v1-{ck}', f'v2-{ck}'))
            rows.append((9, ck, 's1', 's2', f'v1-{ck}', f'v2-{ck}'))
            view_rows.append((9, ck))

        assert values(cql.execute(f"SELECT * FROM {table}")) == rows

        with new_nonmaterialized_view(cql, table, "pk, ck") as nmv:
            assert values(cql.execute(f"SELECT pk, ck FROM {nmv}")) == view_rows

def test_failed_select(cql, test_keyspace):
    schema = 'pk int, ck int, s1 text static, s2 text static, v1 text, v2 text, primary key (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        expected_data = []

        cql.execute(f"INSERT INTO {table} (pk, s1, s2) VALUES (9, 's1', 's2')")

        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES (?, ?, ?, ?)")

        rows = []
        view_rows = []

        for ck in range(0, 10):
            cql.execute(insert_stmt, (9, ck, f'v1-{ck}', f'v2-{ck}'))
            rows.append((9, ck, 's1', 's2', f'v1-{ck}', f'v2-{ck}'))
            view_rows.append((9, ck))

        assert values(cql.execute(f"SELECT * FROM {table}")) == rows

        with new_nonmaterialized_view(cql, table, "pk, ck") as nmv:
            try:
                assert values(cql.execute(f"SELECT pk, ck, s1 FROM {nmv}")) == view_rows
                pytest.fail("Accessed a column not in the view")
            except:
                return

def test_insert_after_view_is_created(cql, test_keyspace):
    schema = 'pk int, ck int, s1 text static, s2 text static, v1 text, v2 text, primary key (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        expected_data = []

        cql.execute(f"INSERT INTO {table} (pk, s1, s2) VALUES (9, 's1', 's2')")

        insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v1, v2) VALUES (?, ?, ?, ?)")

        rows = []
        view_rows = []

        for ck in range(0, 10):
            cql.execute(insert_stmt, (9, ck, f'v1-{ck}', f'v2-{ck}'))
            rows.append((9, ck, 's1', 's2', f'v1-{ck}', f'v2-{ck}'))
            view_rows.append((9, ck))

        assert values(cql.execute(f"SELECT * FROM {table}")) == rows

        with new_nonmaterialized_view(cql, table, "pk, ck") as nmv:
            ck = 20
            cql.execute(insert_stmt, (9, ck, f'v1-{ck}', f'v2-{ck}'))
            rows.append((9, ck, 's1', 's2', f'v1-{ck}', f'v2-{ck}'))
            view_rows.append((9, ck))

            assert values(cql.execute(f"SELECT * FROM {table}")) == rows
            assert values(cql.execute(f"SELECT pk, ck FROM {nmv}")) == view_rows
