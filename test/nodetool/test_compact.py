#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import subprocess

from rest_api_mock import expected_request

def test_compact_all_keyspaces(nodetool):
    nodetool("compact", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
        expected_request("POST", "/storage_service/keyspace_compaction/system"),
        expected_request("POST", "/storage_service/keyspace_compaction/system_schema")])


def test_compact_keyspace(nodetool):
    nodetool("compact", "system_schema", expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")])


def test_compact_nonexistent_keyspace(nodetool):
    nodetool("compact", "system_schema", expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")])


def test_compact_table(nodetool):
    nodetool("compact", "system_schema", "columns", expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema", params={"cf": "columns"})])

    nodetool("compact", "system_schema", "columns", "computed_columns", expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST",
                             "/storage_service/keyspace_compaction/system_schema",
                             params={"cf": "columns,computed_columns"})])


def test_compact_split_output_compatibility_argument(nodetool):
    dummy_request = [
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "-s", expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--split-output", expected_requests=dummy_request)


def test_compact_token_range_compatibility_argument(nodetool):
    dummy_request = [
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "-st", "0", "-et", "1000", expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--start-token", "0", "--end-token", "1000", expected_requests=dummy_request)


def test_compact_partition_compatibility_argument(nodetool):
    dummy_request = [
            expected_request("GET", "/storage_service/keyspaces", multiple=True, response=["system", "system_schema"]),
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "--partition", "0", expected_requests=dummy_request)


def test_compact_user_defined(nodetool, scylla_only):
    with pytest.raises(subprocess.CalledProcessError) as e:
        nodetool("compact", "--user-defined", "/var/lib/scylla/data/system/local-7ad54392bcdd35a684174e047860b377/"
                 "me-3g8w_11cg_4317k2ppfb6d5vgp0w-big-Data.db")

    err_lines = e.value.stderr.rstrip().split('\n')
    assert err_lines[-1] == "error processing arguments: --user-defined flag is unsupported"
