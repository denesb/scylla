#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import json
import pytest
import subprocess

from rest_api_mock import expected_request


def test_jmx_compatibility_args(nodetool, scylla_only):
    """Check that all JMX arguments inherited to nodetool are ignored.

    These arguments are unused in the scylla-native nodetool and should be
    silently ignored.
    """
    dummy_request = [expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "-u", "us3r", "-pw", "secr3t",
             expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--username", "us3r", "--password", "secr3t",
             expected_requests=dummy_request)
    nodetool("compact", "system_schema", "-u", "us3r", "-pwf", "/tmp/secr3t_file",
             expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--username", "us3r", "--password-file", "/tmp/secr3t_file",
             expected_requests=dummy_request)
    nodetool("compact", "system_schema", "-pp",
             expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--print-port",
             expected_requests=dummy_request)


def test_compact_all_keyspaces(nodetool):
    nodetool("compact", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces?type=all", json.dumps(["system", "system_schema"])),
        expected_request("POST", "/storage_service/keyspace_compaction/system"),
        expected_request("POST", "/storage_service/keyspace_compaction/system_schema")])


def test_compact_keyspace(nodetool):
    nodetool("compact", "system_schema", expected_requests=[
            expected_request("POST", "/storage_service/keyspace_compaction/system_schema")])


def test_compact_table(nodetool):
    nodetool("compact", "system_schema", "columns", expected_requests=[
        expected_request("POST", "/storage_service/keyspace_compaction/system_schema?cf=columns")])

    nodetool("compact", "system_schema", "columns", "computed_columns", expected_requests=[
        expected_request("POST",
                         "/storage_service/keyspace_compaction/system_schema?cf=columns%2Ccomputed_columns")])


def test_compact_split_output_compatibility_argument(nodetool):
    dummy_request = [expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "-s", expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--split-output", expected_requests=dummy_request)


def test_compact_token_range_compatibility_argument(nodetool):
    dummy_request = [expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "-st", "0", "-et", "1000", expected_requests=dummy_request)
    nodetool("compact", "system_schema", "--start-token", "0", "--end-token", "1000", expected_requests=dummy_request)


def test_compact_partition_compatibility_argument(nodetool):
    dummy_request = [expected_request("POST", "/storage_service/keyspace_compaction/system_schema")]

    nodetool("compact", "system_schema", "--partition", "0", expected_requests=dummy_request)


def test_compact_user_defined(nodetool, scylla_only):
    with pytest.raises(subprocess.CalledProcessError) as e:
        nodetool("compact", "--user-defined", "/var/lib/scylla/data/system/local-7ad54392bcdd35a684174e047860b377/"
                 "me-3g8w_11cg_4317k2ppfb6d5vgp0w-big-Data.db")

    err_lines = e.value.stderr.rstrip().split('\n')
    assert err_lines[-1] == "error processing arguments: --user-defined flag is unsupported"
