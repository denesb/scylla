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
    parser.addoption('--nodetool', action='store', choices=["scylla", "cassandra"], default="scylla",
                     help="Which nodetool implementation to run the tests against")
    parser.addoption('--nodetool-path', action='store', default=None,
                     help="Path to the nodetool binary,"
                     " with --nodetool=scylla, this should be the scylla binary,"
                     " with --nodetool=cassandra, this should be the nodetool binary")
    parser.addoption('--jmx-path', action='store', default=None,
                     help="Path to the jmx binary, only used with --nodetool=cassandra")


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def jmx_path(request):
    if request.config.getoption("nodetool") == "scylla":
        return None

    path = request.config.getoption("jmx_path")
    if path is not None:
        return os.path.abspath(path)

    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "scylla-jmx", "scripts",
                                        "scylla-jmx"))


@pytest.fixture(scope="session")
def jmx(jmx_path, rest_api_mock_server):
    if jmx_path is None:
        yield
    else:
        workdir = os.path.join(os.path.dirname(jmx_path), "..")
        ip, port = rest_api_mock_server
        jmx_process = subprocess.Popen([jmx_path, "-a", ip, "-p", port], cwd=workdir, text=True)
        yield
        jmx_process.terminate()
        jmx_process.wait()


@pytest.fixture(scope="session")
def nodetool_path(request):
    if request.config.getoption("nodetool") == "scylla":
        mode = request.config.getoption("mode")
        return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "build", mode, "scylla"))

    path = request.config.getoption("nodetool_path")
    if path is not None:
        return os.path.abspath(path)

    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "tools", "java", "nodetool"))


@pytest.fixture(scope="module")
def nodetool(request, jmx, nodetool_path, rest_api_mock_server):
    def invoker(method, *args, expected_requests=None):
        if expected_requests is not None:
            rest_api_mock.set_expected_requests(rest_api_mock_server, expected_requests)

        ip, port = rest_api_mock_server
        if request.config.getoption("nodetool") == "scylla":
            cmd = [nodetool_path, "nodetool", method, "--logger-log-level", "scylla-nodetool=trace"]
        else:
            cmd = [nodetool_path, method]
        cmd += ["-h", ip, "-p", str(port)]
        cmd += list(args)
        res = subprocess.run(cmd, capture_output=True, text=True)
        sys.stdout.write(res.stdout)
        sys.stderr.write(res.stderr)

        unconsumed_expected_requests = rest_api_mock.get_expected_requests(rest_api_mock_server)
        # Clear up any unconsumed requests, so the next test starts with a clean slate
        rest_api_mock.clear_expected_requests(rest_api_mock_server)

        # Check the return-code first, if the command failed probably not all requests were consumed
        res.check_returncode()
        assert len(unconsumed_expected_requests) == 0

    return invoker
