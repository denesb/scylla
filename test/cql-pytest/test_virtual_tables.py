# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import util
import nodetool
import json
import requests
import cassandra.protocol

def test_snapshots_table(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, 'pk int PRIMARY KEY, v int') as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (0, 0)")
        nodetool.take_snapshot(cql, table, 'my_tag', False)
        res = list(cql.execute(f"SELECT keyspace_name, table_name, snapshot_name, live, total FROM system.snapshots"))
        assert len(res) == 1
        ks, tbl = table.split('.')
        assert res[0][0] == ks
        assert res[0][1] == tbl
        assert res[0][2] == 'my_tag'

def test_clients(scylla_only, cql):
    columns = ', '.join([
        'address',
        'port',
        'client_type',
        'connection_stage',
        'driver_name',
        'driver_version',
        'hostname',
        'protocol_version',
        'shard_id',
        'ssl_cipher_suite',
        'ssl_enabled',
        'ssl_protocol',
        'username',
    ])
    cls = list(cql.execute(f"SELECT {columns} FROM system.clients"))
    for cl in cls:
        assert(cl[0] == '127.0.0.1')
        assert(cl[2] == 'cql')

# We only want to check that the table exists with the listed columns, to assert
# backwards compatibility.
def _check_exists(cql, table_name, columns):
    cols = ", ".join(columns)
    assert list(cql.execute(f"SELECT {cols} FROM system.{table_name}"))

def test_protocol_servers(scylla_only, cql):
    _check_exists(cql, "protocol_servers", ("name", "listen_addresses", "protocol", "protocol_version"))

def test_runtime_info(scylla_only, cql):
    _check_exists(cql, "runtime_info", ("group", "item", "value"))

def test_versions(scylla_only, cql):
    _check_exists(cql, "versions", ("key", "build_id", "build_mode", "version"))

# Check reading the system.config table, which should list all configuration
# parameters. As we noticed in issue #10047, each type of configuration
# parameter can have a different function for printing it out, and some of
# those may be wrong so we want to check as many as we can - including
# specifically the experimental_features option which was wrong in #10047
# and #11003.
def test_system_config_read(scylla_only, cql):
    # All rows should have the columns name, source, type and value:
    rows = list(cql.execute("SELECT name, source, type, value FROM system.config"))
    values = dict()
    for row in rows:
        values[row.name] = row.value
    # Check that experimental_features exists and makes sense.
    # It needs to be a JSON-formatted strings, and the strings need to be
    # ASCII feature names - not binary garbage as it was in #10047,
    # and not numbers-formatted-as-string as in #11003.
    assert 'experimental_features' in values
    obj = json.loads(values['experimental_features'])
    assert isinstance(obj, list)
    assert isinstance(obj[0], str)
    assert obj[0] and obj[0].isascii() and obj[0].isprintable()
    assert not obj[0].isnumeric()  # issue #11003
    # Check formatting of tri_mode_restriction like
    # restrict_replication_simplestrategy. These need to be one of
    # allowed string values 0, 1, true, false or warn - but in particular
    # non-empty and printable ASCII, not garbage.
    assert 'restrict_replication_simplestrategy' in values
    obj = json.loads(values['restrict_replication_simplestrategy'])
    assert isinstance(obj, str)
    assert obj and obj.isascii() and obj.isprintable()


@pytest.fixture(scope="module")
def mutation_dump_table1(cql, test_keyspace):
    """ Prepares a table for the data source table tests to work with."""
    with util.new_test_table(cql, test_keyspace, 'pk1 int, pk2 int, ck1 int, ck2 int, v text, s text static, PRIMARY KEY ((pk1, pk2), ck1, ck2)') as table:
        yield table.split('.')


def test_mutation_dump_scan(cql):
    with pytest.raises(cassandra.protocol.ReadFailure):
        cql.execute("SELECT * FROM system.mutation_dump")


def test_mutation_dump_reverse_read(cql, mutation_dump_table1):
    ks, table = mutation_dump_table1

    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()
    cql.execute(f"INSERT INTO {ks}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")

    with pytest.raises(cassandra.protocol.ReadFailure):
        cql.execute(f"SELECT * FROM system.mutation_dump WHERE keyspace_name = '{ks}' AND table_name = '{table}' AND partition_key = ['{pk1}', '{pk2}'] ORDER BY source DESC")


def test_mutation_dump_source(cql, mutation_dump_table1):
    ks, table = mutation_dump_table1

    def expect_sources(*expected_sources):
        for src in ('memtable', 'row-cache', 'sstable'):
            rows = list(cql.execute(f"SELECT * FROM system.mutation_dump WHERE keyspace_name = '{ks}' AND table_name = '{table}' AND partition_key = ['{pk1}', '{pk2}'] AND source = '{src}'"))
            if src in expected_sources:
                assert len(rows) == 3 # partition-start, clustering-row, partition-end
            else:
                assert len(rows) == 0

    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()
    cql.execute(f"INSERT INTO {ks}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")
    expect_sources('memtable')

    nodetool.flush(cql, f"{ks}.{table}")
    expect_sources('row-cache', 'sstable')

    requests.post(f'{nodetool.rest_api_url(cql)}/system/drop_sstable_caches')
    expect_sources('sstable')

    assert list(cql.execute(f"SELECT v FROM {ks}.{table} WHERE pk1={pk1} AND pk2={pk2} BYPASS CACHE"))[0].v == 'vv'
    expect_sources('sstable')

    assert list(cql.execute(f"SELECT v FROM {ks}.{table} WHERE pk1={pk1} AND pk2={pk2}"))[0].v == 'vv'
    expect_sources('row-cache', 'sstable')

    cql.execute(f"INSERT INTO {ks}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")
    expect_sources('memtable', 'row-cache', 'sstable')


def test_mutation_dump_partition_region(cql, mutation_dump_table1):
    ks, table = mutation_dump_table1

    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()

    cql.execute(f"INSERT INTO {ks}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")
    cql.execute(f"DELETE FROM {ks}.{table} WHERE pk1={pk1} AND pk2={pk2} AND ck1=0 AND ck2>30 AND ck2<100")

    def check_single_region(region, expected_rows):
        res = cql.execute(f"""SELECT * FROM system.mutation_dump
            WHERE
                keyspace_name = '{ks}' AND
                table_name = '{table}' AND
                partition_key = ['{pk1}', '{pk2}'] AND
                source = 'memtable' AND
                partition_region = {region}""")
        for r in res:
            assert r.partition_region == region


    check_single_region(0, 1) # partition_start
    check_single_region(1, 1) # static_row
    check_single_region(2, 3) # clustered (1 row + 2 rtc)
    check_single_region(2, 1) # partition_end

    def check_region_range(region_from_inclusive, region_to_exlusive, expected_rows):
        res = cql.execute(f"""SELECT * FROM system.mutation_dump
            WHERE
                keyspace_name = '{ks}' AND
                table_name = '{table}' AND
                partition_key = ['{pk1}', '{pk2}'] AND
                source = 'memtable' AND
                partition_region >= {region_from_inclusive} AND
                partition_region < {region_to_exlusive}""")
        for r in res:
            assert r.partition_region >= region_from_inclusive
            assert r.partition_region < region_from_exclusive

    check_region_range(0, 1, 1) # partition_start
    check_region_range(0, 2, 2) # partition_start, static_row
    check_region_range(0, 3, 5) # partition_start, static_row, 3 clustered (1 row + 2 rtc)
    check_region_range(0, 4, 6) # partition_start, static_row, 3 clustered (1 row + 2 rtc), partition_end

    check_region_range(1, 2, 1) # static_row
    check_region_range(2, 3, 3) # 3 clustered (1 row + 2 rtc)
    check_region_range(1, 3, 4) # static_row, 3 clustered (1 row + 2 rtc)


def test_mutation_dump_range_tombstone_changes(cql, mutation_dump_table1):
    ks, table = mutation_dump_table1

    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()
    cql.execute(f"INSERT INTO {ks}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")

    rts = 4

    with nodetool.no_autocompaction_context(cql, ks):
        for ck in range(44, 44 - rts, -1):
            cql.execute(f"DELETE FROM {ks}.{table} WHERE pk1={pk1} AND pk2={pk2} AND ck1=0 AND ck2>30 AND ck2<{ck}")
            nodetool.flush(cql, f"{ks}.{table}")

        res = list(cql.execute(f"SELECT * FROM system.mutation_dump WHERE keyspace_name = '{ks}' AND table_name = '{table}' AND partition_key = ['{pk1}', '{pk2}'] AND source = 'sstable'"))
        # partition-start,partition-end + row + rts*range-tombstone-change + 1 closing range-tombstone-change
        expected = 2 + 1 + rts + 1
        assert len(res) == 2 + 1 + rts + 1


def test_mutation_dump_slicing(cql, mutation_dump_table1):
    ks, table = mutation_dump_table1

    pk1 = util.unique_key_int()
    pk2 = util.unique_key_int()

    cql.execute(f"INSERT INTO {ks}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 0, 'vv')")
    cql.execute(f"DELETE FROM {ks}.{table} WHERE pk1={pk1} AND pk2={pk2} AND ck1=0 AND ck2>10 AND ck2<20")
    cql.execute(f"INSERT INTO {ks}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 0, 20, 'vv')")
    cql.execute(f"INSERT INTO {ks}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk1}, {pk2}, 1, 20, 'vv')")

    res = cql.execute(f"""SELECT * FROM system.mutation_dump
        WHERE
            keyspace_name = '{ks}' AND
            table_name = '{table}' AND
            partition_key = ['{pk1}', '{pk2}'] AND
            source = 'memtable' AND
            partition_region = 2 AND
            clustering_key >= ['0', '0']
        """)
    for r in res:
        assert r.partition_region == 0
