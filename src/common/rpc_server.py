"""Threaded XML-RPC server tiap node, store lokal, dan push replika async."""
from __future__ import annotations

import copy
import socketserver
import threading
import time
from functools import wraps
from typing import Any, Callable, Dict, Optional
from xmlrpc.server import SimpleXMLRPCServer

from .config import RPC_TIMEOUT, avtree, host_node, port_rpc
from .core import StokError, StokStore
from .logger import buat_trace, panggilan


class _ThreadedXMLRPCServer(socketserver.ThreadingMixIn, SimpleXMLRPCServer):
    daemon_threads = True
    allow_reuse_address = True


def _bungkus(fungsi: Optional[Callable[..., Any]] = None, *, node_id: Optional[int] = None):
    """Decorator envelope standar; mendukung @ _bungkus dan factory node-aware."""
    def decorate(target: Callable[..., Any]) -> Callable[..., Dict[str, Any]]:
        @wraps(target)
        def wrapped(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            try:
                result = target(*args, **kwargs)
                return {"ok": True, "data": result}
            except StokError as exc:
                return {"ok": False, "error": exc.pesan, "kode_http": exc.kode_http,
                        "trace": getattr(exc, "trace", [])}
            except (ValueError, TypeError) as exc:
                return {"ok": False, "error": str(exc), "kode_http": 400,
                        "trace": getattr(exc, "trace", [])}
            except Exception as exc:
                return {"ok": False, "error": str(exc), "kode_http": 500,
                        "trace": getattr(exc, "trace", [])}
        return wrapped
    return decorate(fungsi) if fungsi is not None else decorate


class NodeRPCService:
    def __init__(self, node_id: int, store: Optional[StokStore] = None) -> None:
        if isinstance(node_id, bool) or node_id not in range(1, 5):
            raise ValueError("node_id harus 1..4")
        self.node_id = node_id
        # Node 4 menyimpan format replika multi-sumber, bukan dokumen primer
        # StokStore; RPC Node 4 hanya melayani ping dan terima_replikasi.
        self.store = store or (StokStore(node_id) if node_id in (1, 2, 3) else None)
        self._delay = 0.0
        self._delay_lock = threading.RLock()
        self._replication_targets: tuple[int, ...] = (4,) if node_id in (1, 2, 3) else ()

    def _wait_delay(self) -> None:
        with self._delay_lock:
            delay = self._delay
        if delay:
            time.sleep(delay)

    def _mutasi_selesai(self) -> None:
        if self.node_id not in (1, 2, 3):
            return
        if self.store is None:
            return
        versi = self.store.versi_global
        snapshot = copy.deepcopy(self.store.barang)
        for tujuan in self._replication_targets:
            threading.Thread(
                target=self._push_replication,
                args=(tujuan, versi, snapshot),
                name=f"push-replika-{self.node_id}-to-{tujuan}", daemon=True,
            ).start()

    def _push_replication(self, tujuan: int, versi: int, barang: Dict[str, Any]) -> None:
        try:
            from .rpc_client import panggil_rpc
            result = panggil_rpc(self.node_id, tujuan, "terima_replikasi",
                                 self.node_id, versi, copy.deepcopy(barang))
            panggilan("XML-RPC", "terima_replikasi", self.node_id, tujuan, "ok", versi=versi)
        except Exception as exc:
            panggilan("XML-RPC", "terima_replikasi", self.node_id, tujuan, "error", error=str(exc))

    def ping(self) -> Dict[str, Any]:
        self._wait_delay()
        return {"node_id": self.node_id, "status": "hidup", "ts": time.time()}

    def status(self) -> Dict[str, Any]:
        self._wait_delay()
        if self.node_id == 4:
            from .rmi_objects import get_replika
            return get_replika(4).status()
        return {"node_id": self.node_id, "status": "hidup",
                "versi_global": self.store.versi_global if self.store else None}

    def versi(self) -> Dict[str, Any]:
        self._wait_delay()
        if self.store is None:
            raise StokError("Node 4 tidak memiliki versi primer", 404)
        return {"node_id": self.node_id, "versi_global": self.store.versi_global}

    def atur_delay(self, detik: float) -> Dict[str, Any]:
        if isinstance(detik, bool) or not isinstance(detik, (int, float)) or detik < 0 or detik > 60:
            raise StokError("detik harus angka 0..60", 400)
        with self._delay_lock:
            self._delay = float(detik)
        return {"node_id": self.node_id, "delay": self._delay}

    def daftar(self) -> Any:
        self._wait_delay()
        if self.store is None:
            raise StokError("Node 4 tidak menyediakan katalog primer", 404)
        return self.store.daftar()

    def get_stok(self, kode: str) -> Any:
        self._wait_delay()
        if self.store is None:
            raise StokError("Node 4 tidak menyediakan katalog primer", 404)
        return self.store.get_stok(kode)

    def tambah_barang(self, kode: str, nama: str, stok: int, harga: int = 50000,
                      min_stok: int = 15, request_id: Optional[str] = None) -> Any:
        self._wait_delay()
        if self.store is None:
            raise StokError("Mutasi primer tidak tersedia pada Node 4", 400)
        result = self.store.tambah_barang(kode, nama, stok, harga, min_stok, request_id)
        self._mutasi_selesai()
        return result

    def ubah_stok(self, kode: str, jumlah: int, tipe: str = "kasir",
                  request_id: Optional[str] = None) -> Any:
        self._wait_delay()
        if self.store is None:
            raise StokError("Mutasi primer tidak tersedia pada Node 4", 400)
        result = self.store.ubah_stok(kode, jumlah, tipe, request_id)
        self._mutasi_selesai()
        return result

    def hapus_barang(self, kode: str, request_id: Optional[str] = None) -> Any:
        self._wait_delay()
        if self.store is None:
            raise StokError("Mutasi primer tidak tersedia pada Node 4", 400)
        result = self.store.hapus_barang(kode, request_id)
        self._mutasi_selesai()
        return result

    def hitung_total(self) -> Any:
        self._wait_delay()
        if self.store is None:
            raise StokError("Node 4 tidak menyediakan total katalog primer", 404)
        return self.store.hitung_total()

    def ambil_snapshot(self) -> Any:
        self._wait_delay()
        if self.store is None:
            raise StokError("Node 4 tidak menyediakan snapshot primer", 404)
        return self.store.ambil_snapshot()

    def seed_data_awal(self, data: Any = None) -> Any:
        self._wait_delay()
        if self.store is None:
            raise StokError("Seed primer tidak tersedia pada Node 4", 400)
        result = self.store.seed_data_awal(data)
        self._mutasi_selesai()
        return result

    def terima_replikasi(self, sumber: int, versi_baru: int, barang_dict: Dict[str, Any]) -> Any:
        self._wait_delay()
        if self.node_id != 4:
            raise StokError("Replikasi hanya diterima oleh Node 4", 400)
        from .rmi_objects import get_replika
        result = get_replika(self.node_id).terima_replikasi(sumber, versi_baru, barang_dict)
        return result


class RPCNodeServer:
    def __init__(self, node_id: int, host: Optional[str] = None,
                 store: Optional[StokStore] = None) -> None:
        self.node_id = node_id
        self.host = host or host_node(node_id)
        self.port = port_rpc(node_id)
        self.service = NodeRPCService(node_id, store)
        self.server = _ThreadedXMLRPCServer(
            (self.host, self.port), allow_none=True, logRequests=False,
            bind_and_activate=False,
        )
        self.server.allow_reuse_address = True
        self.server.server_bind()
        self.server.server_activate()
        self.server.register_function(_bungkus(self.service.ping), "ping")
        for name in ("atur_delay", "status", "versi", "daftar", "get_stok", "tambah_barang", "ubah_stok",
                     "hapus_barang", "hitung_total", "ambil_snapshot", "seed_data_awal",
                     "terima_replikasi"):
            self.server.register_function(_bungkus(getattr(self.service, name)), name)
        self._thread: Optional[threading.Thread] = None

    def serve_forever(self) -> None:
        avtree.update_status(self.node_id, {"status": "hidup", "aktif": True})
        self.server.serve_forever(poll_interval=0.25)

    def start(self) -> "RPCNodeServer":
        if self._thread and self._thread.is_alive():
            return self
        self._thread = threading.Thread(target=self.serve_forever,
                                        name=f"RPCServer-{self.node_id}", daemon=True)
        self._thread.start()
        return self

    def shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        avtree.update_status(self.node_id, {"status": "mati", "aktif": False})
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)


def jalankan_rpc_server(node_id: int, host: Optional[str] = None,
                        store: Optional[StokStore] = None) -> RPCNodeServer:
    server = RPCNodeServer(node_id, host, store)
    server.serve_forever()
    return server


def create_server(node_id: int, host: Optional[str] = None,
                  store: Optional[StokStore] = None) -> RPCNodeServer:
    return RPCNodeServer(node_id, host, store)
