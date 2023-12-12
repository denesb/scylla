#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import enum
import pytest
from rest_api_mock import expected_request
import utils


class scrub_status(enum.Enum):
    successful = 0
    aborted = 1
    unable_to_cancel = 2
    validation_errors = 3


def test_scrub_keyspace(nodetool):
    nodetool("scrub", "ks", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspace_scrub/ks", response=scrub_status.successful.value)])


def test_scrub_one_table(nodetool):
    nodetool("scrub", "ks", "tbl1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspace_scrub/ks", params={"cf": "tbl1"},
                         response=scrub_status.successful.value)])


def test_scrub_two_tables(nodetool):
    nodetool("scrub", "ks", "tbl1", "tbl2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspace_scrub/ks", params={"cf": "tbl1,tbl2"},
                         response=scrub_status.successful.value)])


def test_scrub_non_existent_keyspace(nodetool):
    utils.check_nodetool_fails_with(
            nodetool,
            ("scrub", "non_existent_ks"),
            {"expected_requests": [expected_request("GET", "/storage_service/keyspaces", response=["ks"])]},
            ["nodetool: Keyspace [non_existent_ks] does not exist.",
             "error processing arguments: keyspace non_existent_ks does not exist"])


# We don't test all values for --mode and --qurantine-mode, they are passed as-is
# to the REST API, so their value make no difference when testing nodetool itself.
@pytest.mark.parametrize("table", [[], ["tbl1"], ["tbl1", "tbl2"]])
@pytest.mark.parametrize("mode", [None, ("-m", "ABORT"), ("--mode", "ABORT"), "ABORT"])
@pytest.mark.parametrize("quarantine_mode", [None, "INCLUDE"])
@pytest.mark.parametrize("disable_snapshot", [None, "--no-snapshot", "-ns"])
def test_scrub_options(request, nodetool, table, mode, quarantine_mode, disable_snapshot):
    args = ["scrub", "ks"] + table
    expected_params = {}

    if table:
        expected_params["cf"] = ",".join(table)

    if mode is not None:
        if type(mode) is tuple:
            args += list(mode)
            expected_params["scrub_mode"] = mode[1]
        else:
            args += ["--mode", mode]
            expected_params["scrub_mode"] = mode

    if quarantine_mode is not None:
        if request.config.getoption("nodetool") == "scylla":
            args.append("--quarantine-mode", quarantine_mode)
        else:
            pytest.skip("--quarantine-mode only supported by scylla-nodetool")

    if disable_snapshot:
        args.append(disable_snapshot)
        expected_params["disable_snapshot"] = "true"

    nodetool(*args, expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspace_scrub/ks", params=expected_params,
                         response=scrub_status.successful.value)])


def test_scrub_skip_corrupted(nodetool):
    nodetool("scrub", "ks", "tbl1", "tbl2", "--skip-corrupted", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspace_scrub/ks", params={"cf": "tbl1,tbl2", "scrub_mode": "SKIP"},
                         response=scrub_status.successful.value)])

    nodetool("scrub", "ks", "tbl1", "tbl2", "-s", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspace_scrub/ks", params={"cf": "tbl1,tbl2", "scrub_mode": "SKIP"},
                         response=scrub_status.successful.value)])


def test_scrub_skip_corrupted_with_mode(nodetool):
    utils.check_nodetool_fails_with(
            nodetool,
            ("scrub", "ks", "--skip-corrupted", "--mode", "ABORT"),
            {"expected_requests": [expected_request("GET", "/storage_service/keyspaces", response=["ks"])]},
            ["nodetool: skipCorrupted and scrubMode must not be specified together",
             "error processing arguments: cannot use --skip-corrupted when --mode is used"])


@pytest.mark.parametrize("ignored_opt", ["--reinsert-overflowed-ttl", "-r", ("--jobs", "2"), "-j2"])
def test_scrub_ignored_options(nodetool, ignored_opt):
    args = ["scrub", "ks"]
    if type(ignored_opt) is tuple:
        args += list(ignored_opt)
    else:
        args.append(ignored_opt)

    nodetool(*args, expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspace_scrub/ks", response=scrub_status.successful.value)])


@pytest.mark.parametrize("status", [scrub_status.successful, scrub_status.aborted, scrub_status.validation_errors])
def test_scrub_return_status(nodetool, status):
    nodetool("scrub", "ks", "--mode=VALIDATE", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks"]),
        expected_request("GET", "/storage_service/keyspace_scrub/ks", params={"scrub_mode": "VALIDATE"},
                         response=status.value)])
