#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import http.server
import json
import requests
import socketserver
import sys


class request:
    def __init__(self, method, path, response=None):
        self.method = method
        self.path = path
        self.response = response

    def as_json(self):
        res = {"method": self.method, "path": self.path}
        if self.response is not None:
            res["response"] = self.response
        return res

    def __eq__(self, o):
        return self.method == o.method and self.path == o.path

    def __str__(self):
        return json.dumps(self.as_json())


def _make_request(req_json):
    return request(req_json["method"], req_json["path"], req_json.get("response"))


class response_handler(http.server.BaseHTTPRequestHandler):
    EXPECTED_REQUESTS_PATH = "/__expected_requests__"
    expected_requests = []

    def set_headers(self, content_length):
        self.protocol_version = "HTTP/1.1"
        self.send_response(200)
        self.send_header("Content-Length", str(content_length))
        self.send_header("Content-Type", "application/json")
        self.end_headers()

    def do_handle(self, method):
        this_req = request(method, self.path)

        if len(response_handler.expected_requests) == 0:
            self.send_error(500, message="Unexpected request", explain=f"Expected no requests, got: {this_req}")
            return

        expected_req = response_handler.expected_requests[0]
        if this_req != expected_req:
            self.send_error(500, message="Unexpected request", explain=f"Expected {expected_req}, got: {this_req}")
            return

        if expected_req.response is None:
            self.log_message("expected_request: %s, no response", expected_req)
            self.set_headers(0)
        else:
            self.log_message("expected_request: %s, response: %s", expected_req, expected_req.response)
            payload = expected_req.response.encode()
            self.set_headers(len(payload))
            self.wfile.write(payload)

        del response_handler.expected_requests[0]

    def do_GET(self):
        self.close_connection = False
        if self.path == response_handler.EXPECTED_REQUESTS_PATH:
            payload = json.dumps([r.as_json() for r in response_handler.expected_requests])
            payload = payload.encode()
            self.set_headers(len(payload))
            self.wfile.write(payload)
            return

        self.do_handle("GET")

    def do_POST(self):
        if self.path == response_handler.EXPECTED_REQUESTS_PATH:
            content_len = int(self.headers["Content-Length"])
            payload = self.rfile.read(content_len).decode()
            self.log_message("expected_requests: %s", payload)
            payload = json.loads(payload)
            response_handler.expected_requests = list(map(_make_request, payload))
            self.set_headers(len(payload))
            return

        self.do_handle("POST")

    def do_DELETE(self):
        if self.path == response_handler.EXPECTED_REQUESTS_PATH:
            response_handler.expected_requests = []
            self.set_headers(0)
            return

        self.do_handle("DELETE")


def run_server(ip, port):
    with socketserver.TCPServer((ip, port), response_handler) as httpd:
        httpd.serve_forever()


def get_expected_requests(server):
    """Get the expected requests list from the server.

    This will contain all the unconsumed expected request currently on the
    server. Can be used to check whether all expected requests arrived.

    Params:
    * server - resolved `rest_api_mock_server` fixture (see conftest.py).
    """
    ip, port = server
    r = requests.get(f"http://{ip}:{port}{response_handler.EXPECTED_REQUESTS_PATH}")
    r.raise_for_status()
    return [_make_request(r) for r in r.json()]


def clear_expected_requests(server):
    """Clear the expected requests list on the server.

    Params:
    * server - resolved `rest_api_mock_server` fixture (see conftest.py).
    """
    ip, port = server
    r = requests.delete(f"http://{ip}:{port}{response_handler.EXPECTED_REQUESTS_PATH}")
    r.raise_for_status()


def set_expected_requests(server, expected_requests):
    """Set the expected requests list on the server.

    Params:
    * server - resolved `rest_api_mock_server` fixture (see conftest.py).
    * requests - a list of request objects
    """
    ip, port = server
    payload = json.dumps([r.as_json() for r in expected_requests])
    r = requests.post(f"http://{ip}:{port}{response_handler.EXPECTED_REQUESTS_PATH}", data=payload)
    r.raise_for_status()


if __name__ == '__main__':
    run_server(sys.argv[1], int(sys.argv[2]))
