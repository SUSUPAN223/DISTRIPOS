import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.analytics.mpi_analytics import simulate_audit
from src.common.api import create_app
from src.common.core import StokError, StokStore
from src.common.resilience import panggil_dengan_retry
from src.common.rmi_objects import Cabang, LockManager


class StorageAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StokStore(2, Path(self.tmp.name) / "node.json", auto_load=False)

    def tearDown(self):
        self.tmp.cleanup()

    def test_crud_validation_and_atomic_file(self):
        self.store.tambah_barang("A", "Barang A", 5, 10, 2)
        self.assertEqual(self.store.versi_global, 1)
        with self.assertRaises(StokError) as duplicate:
            self.store.tambah_barang("A", "Lain", 1)
        self.assertEqual(duplicate.exception.kode_http, 409)
        with self.assertRaises(StokError) as missing:
            self.store.get_stok("NO")
        self.assertEqual(missing.exception.kode_http, 404)
        with self.assertRaises(StokError) as insufficient:
            self.store.ubah_stok("A", -6)
        self.assertEqual(insufficient.exception.kode_http, 422)
        for invalid in (0, 2147483648, -2147483649, True):
            with self.assertRaises(StokError):
                self.store.ubah_stok("A", invalid)
        self.store.ubah_stok("A", -1)
        doc = json.loads(self.store.file_path.read_text(encoding="utf-8"))
        self.assertEqual(doc["versi_global"], 2)
        self.assertEqual(doc["barang"]["A"]["stok"], 4)
        self.store.hapus_barang("A")
        self.assertEqual(self.store.versi_global, 3)

    def test_seed_is_repeatable(self):
        seed = [{"kode": "A", "nama": "A", "stok": 8}]
        self.store.seed_data_awal(seed)
        self.store.ubah_stok("A", -2)
        version = self.store.versi_global
        self.store.seed_data_awal(seed)
        self.assertEqual(self.store.get_stok("A")["stok"], 6)
        self.assertEqual(self.store.versi_global, version)


class TransferAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StokStore(2, Path(self.tmp.name) / "node.json", auto_load=False)
        self.store.tambah_barang("A", "Asal", 10, 250, 3)
        self.branch = Cabang(2, self.store)

    def tearDown(self):
        self.tmp.cleanup()

    def test_transfer_creates_destination_with_source_metadata(self):
        def call(_node, _obj, method, *args):
            if method == "acquire":
                return {"token": "t"}
            if method == "release":
                return True
            raise AssertionError(method)

        def rpc(_caller, _target, method, *args):
            if method == "ubah_stok":
                raise __import__("src.common.rpc_client", fromlist=["RemoteCallError"]).RemoteCallError("missing", 404)
            if method == "tambah_barang":
                _, name, qty, price, minimum, _rid = args
                return {"data": {"kode": "A", "nama": name, "stok": qty, "harga": price, "min_stok": minimum}, "trace": []}
            raise AssertionError(method)

        with patch("src.common.rmi_objects._rmi_call", side_effect=call), patch("src.common.rpc_client.panggil_rpc", side_effect=rpc):
            result = self.branch.transfer_stok("A", 3, 3)
        self.assertTrue(result["ok"])
        self.assertEqual(self.store.get_stok("A")["stok"], 7)
        self.assertIn("transfer_upsert_sku", [x["aksi"] for x in result["trace"]])
        self.assertTrue(any(x["aksi"] == "release_lock" for x in result["trace"]))

    def test_simulated_failure_rolls_back_and_releases(self):
        calls = []
        def call(_node, _obj, method, *args):
            calls.append(method)
            return {"token": "t"} if method == "acquire" else True
        with patch("src.common.rmi_objects._rmi_call", side_effect=call):
            result = self.branch.transfer_stok("A", 3, 3, simulasi_gagal=True)
        self.assertFalse(result["ok"])
        self.assertEqual(result["kode_http"], 502)
        self.assertEqual(self.store.get_stok("A")["stok"], 10)
        self.assertTrue(any(x["protokol"] == "ROLLBACK" for x in result["trace"]))
        self.assertIn("release", calls)

    def test_insufficient_stock_rejected_before_debit(self):
        calls = []
        with patch("src.common.rmi_objects._rmi_call", side_effect=lambda *a: calls.append(a) or {"token":"x"}):
            result = self.branch.transfer_stok("A", 11, 3)
        self.assertFalse(result["ok"])
        self.assertEqual(result["kode_http"], 422)
        self.assertEqual(self.store.get_stok("A")["stok"], 10)


class ApiAndResilienceAuditTests(unittest.TestCase):
    def test_node4_write_rejected_and_http_error_envelope(self):
        app = create_app(1)
        response = app.test_client().post("/barang?node=4", json={"kode":"X", "nama":"X", "stok":1})
        self.assertEqual(response.status_code, 400)
        self.assertIn("replika", response.json["error"]["message"])

    def test_api_validation_errors_keep_status(self):
        app = create_app(1)
        client = app.test_client()
        with patch("src.common.api.panggil_rpc", return_value={"data": {}, "trace": []}):
            response = client.post("/barang?node=2", json={"kode":"X", "nama":"X", "stok":1})
            self.assertEqual(response.status_code, 201)
        with patch("src.common.api.panggil_rpc", side_effect=__import__("src.common.rpc_client", fromlist=["RemoteCallError"]).RemoteCallError("SKU duplikat", 409)):
            response = client.post("/barang?node=2", json={"kode":"X", "nama":"X", "stok":1})
            self.assertEqual(response.status_code, 409)
            self.assertEqual(response.json["error"]["message"], "SKU duplikat")

    def test_read_failover_returns_failover_trace(self):
        from src.common.rpc_client import RemoteCallError
        app = create_app(1)
        snapshot = {"versi": 3, "barang": {"A": {"kode":"A", "nama":"A", "stok":4}}}
        with patch("src.common.api.panggil_rpc", side_effect=ConnectionError("offline")), patch(
            "src.common.api.panggil_rmi", return_value={"data":snapshot, "trace":[]}
        ):
            response = app.test_client().get("/total?node=2")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["data"]["total_stok"], 4)
        self.assertEqual(response.json["trace"][0]["protokol"], "FAILOVER")

    def test_dht_unknown_sku_is_clean_miss(self):
        app = create_app(1)
        item = {"data": {"stok": 2}}
        with patch("src.common.api.panggil_rpc", return_value={"data":item, "trace":[]}):
            response = app.test_client().get("/dht/lookup/ABSENT")
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.json["data"]["node_pemilik"])
        self.assertIn("hops", response.json["data"])

    def test_non_idempotent_timeout_not_retried(self):
        attempts = []
        def call():
            attempts.append(1)
            raise TimeoutError("late response")
        with self.assertRaises(TimeoutError):
            panggil_dengan_retry(call, method="ubah_stok", backoff=[])
        self.assertEqual(len(attempts), 1)

    def test_mpi_tolerates_empty_and_missing_json(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("data_node1.json", "data_node2.json", "data_node3.json", "replika_node4.json"):
                (root / name).write_text("", encoding="utf-8")
            result = simulate_audit(root)
            self.assertEqual(result["jumlah_rank"], 4)
            self.assertEqual(result["sku"], [])
            self.assertEqual(result["agregat"]["total_stok"], 0)


if __name__ == "__main__":
    unittest.main()