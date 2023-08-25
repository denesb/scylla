#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import multiprocessing
import os
import pytest
import random
import rest_api_mock
import subprocess
import sys


def pytest_addoption(parser):
    parser.addoption('--mode', action='store', default='dev',
        help='Scylla build mode to use')


@pytest.fixture(scope="module")
def scylla_path(request):
    mode = request.config.getoption("mode")
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "build", mode, "scylla"))


@pytest.fixture(scope="module")
def rest_api_mock_server():
    ip = f"127.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
    port = random.randint(10000, 65535)
    server_process = multiprocessing.Process(target=rest_api_mock.run_server, args=(ip, port))
    server_process.start()
    try:
        yield (ip, port)
    finally:
        server_process.terminate()
        server_process.join()


@pytest.fixture(scope="function")
def server_context(rest_api_mock_server):
    """Clean the expected requests before and after the test"""
    rest_api_mock.clear_expected_requests(rest_api_mock_server)
    yield rest_api_mock_server
    unconsumed_expected_requests = rest_api_mock.get_expected_requests(rest_api_mock_server)
    # Clear up any unconsumed requests, so the next test starts with a clean slate
    rest_api_mock.clear_expected_requests(rest_api_mock_server)
    assert len(unconsumed_expected_requests) == 0


@pytest.fixture(scope="module")
def nodetool(scylla_path, rest_api_mock_server):
    def invoker(method, *args):
        ip, port = rest_api_mock_server
        cmd = [scylla_path, "nodetool", method, "--logger-log-level", "scylla-nodetool=trace", "-h", ip, "-p", str(port)] + list(args)
        res = subprocess.run(cmd, capture_output=True, text=True)
        sys.stdout.write(res.stdout)
        sys.stderr.write(res.stderr)
        res.check_returncode()

    return invoker
