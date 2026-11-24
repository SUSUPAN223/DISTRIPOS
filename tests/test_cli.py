import argparse
import io
import json
import urllib.error
import unittest
from unittest.mock import patch

from cli import GatewayError, SmartGateway


class _Response:
    def __init__(self, body, status=200):
        self.body = json.dumps(body).encode("utf-8")
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.body


class CLIGatewayTests(unittest.TestCase):
    def setUp(self):
        self.gateway = SmartGateway(preferred=1)
        self.gateway.hosts = {n: f"node{n}" for n in range(1, 5)}
        self.calls = []

    def test_http_502_is_not_replayed(self):
        def open_request(request, timeout):
            self.calls.append(request.full_url)
            raise urllib.error.HTTPError(request.full_url, 502, "Bad Gateway", {}, io.BytesIO(
                b'{"ok":false,"error":{"message":"rollback failed"},"trace":[]}'))

        with patch("cli.urllib.request.urlopen", side_effect=open_request):
            with self.assertRaises(GatewayError) as error:
                self.gateway.request("POST", "/transfer", {"kode": "A"})
        self.assertEqual(error.exception.status, 502)
        self.assertEqual(str(error.exception), "rollback failed")
        self.assertEqual(len(self.calls), 1)

    def test_transport_failure_fails_over(self):
        def open_request(request, timeout):
            self.calls.append(request.full_url)
            if "node1" in request.full_url:
                raise urllib.error.URLError(TimeoutError("timeout"))
            return _Response({"ok": True, "data": {"node": 2}, "trace": []})

        with patch("cli.urllib.request.urlopen", side_effect=open_request), patch("builtins.print"):
            result = self.gateway.request("GET", "/status")
        self.assertEqual(result["data"], {"node": 2})
        self.assertEqual(len(self.calls), 2)
        self.assertIn("node2", self.calls[1])
        self.assertEqual(self.gateway.preferred, 2)


if __name__ == "__main__":
    unittest.main()