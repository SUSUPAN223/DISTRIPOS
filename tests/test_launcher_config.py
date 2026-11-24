import unittest
from unittest.mock import patch

import launcher


class LauncherConfigTests(unittest.TestCase):
    def test_lan_client_receives_routable_host_instead_of_loopback(self):
        client = launcher.app.test_client()
        with patch("launcher._poll", return_value=None), patch.dict(launcher._processes, {}, clear=True):
            response = client.get("/launcher/status", headers={"Host": "192.168.1.20:8000"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["api_nodes"]["1"]["host"], "192.168.1.20")
        self.assertEqual(response.json["api_nodes"]["4"]["port"], 8004)

    def test_loopback_client_uses_configured_node_host(self):
        client = launcher.app.test_client()
        with patch("launcher._poll", return_value=None), patch.dict(launcher._processes, {}, clear=True), patch.dict(
            "os.environ", {"HOST_NODE1": "10.0.0.5"}
        ):
            response = client.get("/launcher/status", headers={"Host": "localhost:8000"})
        self.assertEqual(response.json["api_nodes"]["1"]["host"], "10.0.0.5")


if __name__ == "__main__":
    unittest.main()