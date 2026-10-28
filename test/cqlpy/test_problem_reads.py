# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for system.problem_reads, the samples of problematic reads.

import time

from cassandra.query import SimpleStatement
import pytest

from . import nodetool
from .util import new_test_table, config_value_context


# Sampling is rate limited: one sample per symptom per period, on each shard. Disable the
# rate limit, so a test's read is not dropped because an earlier test's read was sampled.
@pytest.fixture(scope="module")
def no_sampling_rate_limit(cql, scylla_only):
    cql.execute("ALTER CLUSTER WITH problem_reads_sampling = {'sample_period_ms': '0'}")
    yield
    cql.execute("ALTER CLUSTER WITH problem_reads_sampling = null")


def samples(cql, table, symptom, columns):
    ks, tbl = table.split('.')
    rows = cql.execute(f"SELECT symptom, {', '.join(columns)} FROM system.problem_reads WHERE keyspace_name = '{ks}' AND table_name = '{tbl}'")
    return [row for row in rows if row.symptom == symptom]


# Cluster config is applied asynchronously, so repeat the read until it is sampled.
def read_until_sampled(cql, table, statement, symptom, columns, timeout=30):
    deadline = time.time() + timeout
    while True:
        cql.execute(statement)
        found = samples(cql, table, symptom, columns)
        if found or time.time() > deadline:
            return found
        time.sleep(0.1)


def test_unpaged_read_is_sampled(cql, test_keyspace, no_sampling_rate_limit):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 0, 0)")
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 1, 1)")
        unpaged = SimpleStatement(f"SELECT * FROM {table} WHERE pk = 0", fetch_size=None)
        found = read_until_sampled(cql, table, unpaged, 'unpaged', ['paged', 'read_kind', 'result_rows', 'partition_ranges'])
        assert len(found) == 1
        assert found[0].paged is False
        assert found[0].result_rows == 2
        assert len(found[0].partition_ranges) == 1
        start, end = found[0].partition_ranges[0]
        assert start == end
        assert start.inclusive
        assert start.value == ['0']
        assert start.token is not None


# Keys are rendered as CQL literals, one per component, so they can be pasted into a query.
def test_sampled_keys_are_cql_literals(cql, test_keyspace, no_sampling_rate_limit):
    with new_test_table(cql, test_keyspace, "pk1 text, pk2 int, ck1 text, ck2 int, v int, PRIMARY KEY ((pk1, pk2), ck1, ck2)") as table:
        cql.execute(f"INSERT INTO {table} (pk1, pk2, ck1, ck2, v) VALUES ('it''s', 1, 'a', 1, 0)")
        cql.execute(f"INSERT INTO {table} (pk1, pk2, ck1, ck2, v) VALUES ('it''s', 1, 'a', 2, 0)")
        unpaged = SimpleStatement(f"SELECT * FROM {table} WHERE pk1 = 'it''s' AND pk2 = 1 AND ck1 = 'a'", fetch_size=None)
        found = read_until_sampled(cql, table, unpaged, 'unpaged', ['partition_ranges', 'clustering_ranges'])
        assert len(found) == 1
        start, end = found[0].partition_ranges[0]
        assert start.value == ["'it''s'", '1']
        start, end = found[0].clustering_ranges[0]
        assert start == end
        assert start.value == ["'a'"]
        assert start.token is None
        # The literals select the same partition.
        assert len(list(cql.execute(f"SELECT * FROM {table} WHERE pk1 = {found[0].partition_ranges[0][0].value[0]} AND pk2 = 1"))) == 2


# The static columns count every checked read page of the table, and every occurrence of each
# symptom, on this node.
def test_symptoms_are_counted(cql, test_keyspace, no_sampling_rate_limit):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 0, 0)")
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 1, 1)")
        unpaged = SimpleStatement(f"SELECT * FROM {table} WHERE pk = 0", fetch_size=None)
        read_until_sampled(cql, table, unpaged, 'unpaged', ['paged'])
        for _ in range(3):
            cql.execute(unpaged)
        ks, tbl = table.split('.')
        row = cql.execute(f"SELECT reads, symptom_counts, counted_since FROM system.problem_reads WHERE keyspace_name = '{ks}' AND table_name = '{tbl}'").one()
        assert row.symptom_counts['unpaged'] >= 4
        assert row.reads >= row.symptom_counts['unpaged']
        assert row.counted_since is not None


def test_paged_read_is_not_sampled_as_unpaged(cql, test_keyspace, no_sampling_rate_limit):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 0, 0)")
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 1, 1)")
        cql.execute(SimpleStatement(f"SELECT * FROM {table} WHERE pk = 0", fetch_size=1)).all()
        assert samples(cql, table, 'unpaged', ['paged']) == []


def test_tombstone_heavy_read_is_sampled(cql, test_keyspace, no_sampling_rate_limit):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
            "WITH problem_reads_thresholds = {'tombstones': '5'}") as table:
        for ck in range(10):
            cql.execute(f"DELETE FROM {table} WHERE pk = 0 AND ck = {ck}")
        read = SimpleStatement(f"SELECT * FROM {table} WHERE pk = 0", fetch_size=100)
        found = read_until_sampled(cql, table, read, 'tombstones', ['dead_rows', 'threshold', 'threshold_unit'])
        assert len(found) == 1
        assert found[0].dead_rows == 10
        assert found[0].threshold == 5
        assert found[0].threshold_unit == 'tombstones'


def test_unpaged_large_read_is_sampled(cql, test_keyspace, no_sampling_rate_limit):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v text, PRIMARY KEY (pk, ck)") as table:
        for ck in range(20):
            cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, {ck}, '{'x' * 100}')")
        unpaged = SimpleStatement(f"SELECT * FROM {table} WHERE pk = 0", fetch_size=None)
        with config_value_context(cql, 'max_memory_for_unlimited_query_soft_limit', '256'):
            found = read_until_sampled(cql, table, unpaged, 'unpaged_large', ['result_rows', 'threshold', 'threshold_unit'])
        assert len(found) == 1
        assert found[0].result_rows == 20
        assert found[0].threshold == 256
        assert found[0].threshold_unit == 'bytes'
        # Exclusive with unpaged: the read is counted as unpaged_large only.
        assert samples(cql, table, 'unpaged', ['paged']) == []


def test_tombstone_limit_page_is_sampled(cql, test_keyspace, no_sampling_rate_limit):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        for ck in range(20):
            cql.execute(f"DELETE FROM {table} WHERE pk = 0 AND ck = {ck}")
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 100, 0)")
        read = SimpleStatement(f"SELECT * FROM {table} WHERE pk = 0", fetch_size=100)
        with config_value_context(cql, 'query_tombstone_page_limit', '5'):
            found = read_until_sampled(cql, table, read, 'tombstone_limit_page', ['tombstone_limit_reached', 'threshold', 'threshold_unit'])
        assert len(found) == 1
        assert found[0].tombstone_limit_reached
        assert found[0].threshold == 5
        assert found[0].threshold_unit == 'tombstones'


def test_read_amplification_is_sampled(cql, test_keyspace, no_sampling_rate_limit):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
            "WITH problem_reads_thresholds = {'read_amplification': '2'}") as table:
        for ck in range(3):
            cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, {ck}, 0)")
            nodetool.flush(cql, table)
        # The cache would serve the read without opening the sstables.
        read = SimpleStatement(f"SELECT * FROM {table} WHERE pk = 0 BYPASS CACHE", fetch_size=100)
        found = read_until_sampled(cql, table, read, 'read_amplification', ['sstables_read', 'threshold', 'threshold_unit'])
        assert len(found) == 1
        assert found[0].sstables_read >= 2
        assert found[0].threshold == 2
        assert found[0].threshold_unit == 'sstables'


def test_memory_amplification_is_sampled(cql, test_keyspace, no_sampling_rate_limit):
    # Any read uses more memory in total than the size of its result.
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
            "WITH problem_reads_thresholds = {'memory_amplification_min_mb': '0', 'memory_amplification': '1'}") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 0, 0)")
        read = SimpleStatement(f"SELECT * FROM {table} WHERE pk = 0", fetch_size=100)
        found = read_until_sampled(cql, table, read, 'memory_amplification', ['memory_total_bytes', 'result_bytes', 'threshold', 'threshold_unit'])
        assert len(found) == 1
        assert found[0].memory_total_bytes >= found[0].result_bytes
        assert found[0].threshold == 1
        assert found[0].threshold_unit == 'ratio'
