#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import rest_api_mock
import json
import subprocess


def test_jmx_compatibility_args(server_context, nodetool):
    rest_api_mock.set_expected_requests(server_context, [
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema"),
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema"),
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema"),
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema"),
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema"),
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema")])

    nodetool("compact", "system_schema", "-u", "us3r", "-pw", "secr3t")
    nodetool("compact", "system_schema", "--username", "us3r", "--password", "secr3t")
    nodetool("compact", "system_schema", "-u", "us3r", "-pwf", "/tmp/secr3t_file")
    nodetool("compact", "system_schema", "--username", "us3r", "--password-file", "/tmp/secr3t_file")
    nodetool("compact", "system_schema", "-pp")
    nodetool("compact", "system_schema", "--print-port")


def test_compact_all_keyspaces(server_context, nodetool):
    rest_api_mock.set_expected_requests(server_context, [
        rest_api_mock.request("GET", "/storage_service/keyspaces?type=all", json.dumps(["system", "system_schema"])),
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system"),
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema")])

    nodetool("compact")


def test_compact_keyspace(server_context, nodetool):
    rest_api_mock.set_expected_requests(server_context, [
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema")])

    nodetool("compact", "system_schema")


def test_compact_table(server_context, nodetool):
    rest_api_mock.set_expected_requests(server_context, [
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema?cf=columns")])

    nodetool("compact", "system_schema", "columns")


def test_compact_split_output_compatibility_argument(server_context, nodetool):
    rest_api_mock.set_expected_requests(server_context, [
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema"),
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema")])

    nodetool("compact", "system_schema", "-s")
    nodetool("compact", "system_schema", "--split-output")


def test_compact_token_range_compatibility_argument(server_context, nodetool):
    rest_api_mock.set_expected_requests(server_context, [
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema"),
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema")])

    nodetool("compact", "system_schema", "-st", "0", "-et", "1000")
    nodetool("compact", "system_schema", "--start-token", "0", "--end-token", "1000")


def test_compact_partition_compatibility_argument(server_context, nodetool):
    rest_api_mock.set_expected_requests(server_context, [
        rest_api_mock.request("POST", "/storage_service/keyspace_compaction/system_schema")])

    nodetool("compact", "system_schema", "--partition", "0")


def test_compact_user_defined(server_context, nodetool):
    with pytest.raises(subprocess.CalledProcessError) as e:
        nodetool("compact", "--user-defined", "/var/lib/scylla/data/system/local-7ad54392bcdd35a684174e047860b377/me-3g8w_11cg_4317k2ppfb6d5vgp0w-big-Data.db")

    err_lines = e.value.stderr.rstrip().split('\n')
    assert err_lines[-1] == "error processing arguments: --user-defined flag is unsupported"
