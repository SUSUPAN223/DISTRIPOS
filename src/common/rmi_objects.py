"""Pyro5 objects: distributed lease lock, branch transfers, replica snapshots."""
from __future__ import annotations

import copy
import json
import os
import secrets
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import Pyro5.api

from .config import (LEASE_DEFAULT, LOCK_WAIT_TIMEOUT, PROJECT_ROOT, RPC_TIMEOUT,
                     avtree, host_node, port_rmi, rmi_bind_host)
from .core import StokError, StokStore
from .logger import buat_trace, panggilan


_OBJECTS_LOCK = threading.RLock()
_OBJECTS: Dict[tuple[int, str], Any] = {}


def _uri(node_id: int, object_name: str) -> str:
    return f"PYRO:{object_name}@{host_node(node_id)}:{port_rmi(node_id)}"


@Pyro5.api.expose
class LockManager:
    def __init__(self, node_id: int = 1, *, lockmanager_cadangan: bool = False) -> None:
        if node_id not in (1, 4):
            raise ValueError("LockManager hanya berjalan pada Node 1 dan Node 4")
        self.node_id = node_id
        self.lockmanager_cadangan = bool(lockmanager_cadangan or node_id == 4)
        self._locks: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def _bersihkan_kedaluwarsa(self) -> None:
        now = time.monotonic()
        expired = [key for key, data in self._locks.items() if data["expiry"] <= now]
        for key in expired:
            del self._locks[key]

    def acquire(self, kode: str, pemegang: str, lease: float = LEASE_DEFAULT,
                tunggu: float = LOCK_WAIT_TIMEOUT) -> Dict[str, Any]:
        if not isinstance(kode, str) or not kode.strip() or not isinstance(pemegang, str) or not pemegang.strip():
            raise StokError("kode dan pemegang wajib diisi", 400)
        if isinstance(lease, bool) or not isinstance(lease, (int, float)) or lease <= 0:
            raise StokError("lease harus positif", 400)
        if isinstance(tunggu, bool) or not isinstance(tunggu, (int, float)) or tunggu < 0:
            raise StokError("tunggu tidak valid", 400)
        deadline = time.monotonic() + min(float(tunggu), LOCK_WAIT_TIMEOUT)
        while True:
            with self._lock:
                self._bersihkan_kedaluwarsa()
                current = self._locks.get(kode)
                if current is None or current["pemegang"] == pemegang:
                    token = current["token"] if current else secrets.token_urlsafe(24)
                    expiry = time.monotonic() + float(lease)
                    self._locks[kode] = {"pemegang": pemegang, "token": token,
                                         "expiry": expiry, "lease": float(lease),
                                         "created_at": current.get("created_at", time.time()) if current else time.time()}
                    return {"ok": True, "kode": kode, "pemegang": pemegang,
                            "token": token, "lease": float(lease), "expires_at": time.time() + lease}
            if time.monotonic() >= deadline:
                raise StokError(f"Waktu tunggu lock {kode!r} habis", 423)
            time.sleep(min(0.05, max(0.001, deadline - time.monotonic())))

    def release(self, kode: str, pemegang: str, token: Optional[str] = None) -> bool:
        with self._lock:
            self._bersihkan_kedaluwarsa()
            current = self._locks.get(kode)
            if current is None:
                return False
            if current["pemegang"] != pemegang or (token is not None and current["token"] != token):
                raise StokError("Pemegang/token lock tidak cocok", 403)
            del self._locks[kode]
            return True

    def status(self) -> Dict[str, Any]:
        with self._lock:
            self._bersihkan_kedaluwarsa()
            now = time.monotonic()
            locks = {
                kode: {"pemegang": data["pemegang"], "sisa_detik": max(0.0, data["expiry"] - now),
                       "lease": data["lease"], "created_at": data["created_at"]}
                for kode, data in self._locks.items()
            }
            return {"node_id": self.node_id, "lockmanager_cadangan": self.lockmanager_cadangan,
                    "locks": locks, "jumlah_lock": len(locks)}

    def expire_stale(self) -> int:
        with self._lock:
            before = len(self._locks)
            self._bersihkan_kedaluwarsa()
            return before - len(self._locks)


@Pyro5.api.expose
class Cabang:
    def __init__(self, node_id: int, store: Optional[StokStore] = None) -> None:
        if node_id not in (2, 3):
            raise ValueError("Cabang hanya berjalan pada Node 2 dan Node 3")
        self.node_id = node_id
        self.store = store or StokStore(node_id)
        self.jumlah_transfer = 0
        self.riwayat_transfer: list[Dict[str, Any]] = []
        self._lock = threading.RLock()
        self._transfer_lock = threading.RLock()

    def info(self) -> Dict[str, Any]:
        with self._lock:
            return {"node_id": self.node_id, "status": "hidup",
                    "jumlah_transfer": self.jumlah_transfer,
                    "riwayat_transfer": copy.deepcopy(self.riwayat_transfer),
                    "versi_global": self.store.versi_global}

    def transfer_stok(self, kode: str, jumlah: int, ke_node: int, delay: float = 0.0,
                      simulasi_gagal: bool = False, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if isinstance(jumlah, bool) or not isinstance(jumlah, int) or jumlah <= 0:
            raise StokError("jumlah transfer harus integer positif", 400)
        if ke_node not in (2, 3) or ke_node == self.node_id:
            raise StokError("ke_node harus cabang lain (2 atau 3)", 400)
        if isinstance(delay, bool) or not isinstance(delay, (int, float)) or delay < 0:
            return {"ok": False, "error": "delay harus angka nonnegatif",
                    "kode_http": 400, "trace": []}
        if isinstance(simulasi_gagal, bool) is False:
            return {"ok": False, "error": "simulasi_gagal harus boolean",
                    "kode_http": 400, "trace": []}
        if metadata is not None and not isinstance(metadata, dict):
            return {"ok": False, "error": "metadata barang harus berupa object",
                    "kode_http": 400, "trace": []}

        try:
            source_item = self.store.get_stok(kode)
        except StokError as exc:
            return {"ok": False, "error": exc.pesan, "kode_http": exc.kode_http, "trace": []}
        # Metadata always comes from the authoritative source, never from UI input.
        metadata = {key: source_item[key] for key in ("nama", "harga", "min_stok")}

        pemegang = f"node-{self.node_id}:{secrets.token_hex(8)}"
        trace: list[Dict[str, Any]] = []
        lock_node = 1
        token: Optional[str] = None
        lock_acquired = False
        debit_sukses = False
        lock_uri_called = False
        primary_acquire_uncertain = False
        transfer_id = str(uuid.uuid4())
        failure: Optional[StokError] = None
        self._transfer_lock.acquire()
        try:
            try:
                lock_uri_called = True
                lock_result = _rmi_call(1, "LockManager", "acquire", kode, pemegang,
                                        LEASE_DEFAULT, LOCK_WAIT_TIMEOUT)
                token = lock_result.get("token") if isinstance(lock_result, dict) else None
                lock_acquired = True
                trace.append(buat_trace(self.node_id, 1, "Pyro5-RMI", "acquire_lock",
                                        f"kode={kode} lease={LEASE_DEFAULT}"))
            except Exception as primary_error:
                primary_acquire_uncertain = True
                lock_node = 4
                lock_result = _rmi_call(4, "LockManager", "acquire", kode, pemegang,
                                        LEASE_DEFAULT, LOCK_WAIT_TIMEOUT)
                token = lock_result.get("token") if isinstance(lock_result, dict) else None
                trace.append(buat_trace(self.node_id, 1, "FAILOVER", "lockmanager_failover",
                                        f"Node 1 tidak tersedia: {primary_error}", "error"))
                trace.append(buat_trace(self.node_id, 4, "Pyro5-RMI", "acquire_lock_cadangan",
                                        f"kode={kode} lease={LEASE_DEFAULT}"))
                lock_acquired = True

            # Serialize branch-side transfers and debit under the store's own
            # lock so a concurrent cashier operation cannot invalidate the
            # sufficient-stock check between validation and debit.
            with self.store.lock:
                source_now = self.store.barang.get(kode)
                if source_now is None:
                    raise StokError(f"Barang dengan kode {kode!r} tidak ditemukan", 404)
                if source_now["stok"] < jumlah:
                    raise StokError("Stok tidak cukup untuk transfer", 422)
                self.store.ubah_stok(kode, -jumlah, tipe="transfer_keluar",
                                     request_id=f"transfer:{transfer_id}:debit")
            debit_sukses = True
            trace.append(buat_trace(self.node_id, self.node_id, "Pyro5-RMI", "transfer_debit",
                                    f"kode={kode} jumlah={jumlah}"))
            if simulasi_gagal or delay > RPC_TIMEOUT:
                if delay > 0 and not simulasi_gagal:
                    time.sleep(min(float(delay), RPC_TIMEOUT + 0.1))
                raise TimeoutError("RPC tujuan gagal/timeout sesuai fault injection")
            if delay:
                time.sleep(float(delay))
            from .rpc_client import panggil_rpc
            from .rpc_client import RemoteCallError
            try:
                rpc_result = panggil_rpc(self.node_id, ke_node, "ubah_stok", kode, jumlah,
                                         "transfer_masuk", f"transfer:{transfer_id}:kredit")
            except RemoteCallError as destination_error:
                if destination_error.kode_http != 404 or not metadata:
                    raise
                # Only transfers may create a destination SKU. The prior debit
                # has already checked source availability under the branch lock.
                destination_item = self.store.get_stok(kode)
                transfer_item = {**destination_item, **metadata, "kode": kode, "stok": jumlah}
                rpc_result = panggil_rpc(self.node_id, ke_node, "tambah_barang", kode,
                    transfer_item["nama"], jumlah, transfer_item["harga"], transfer_item["min_stok"],
                    f"transfer:{transfer_id}:upsert")
                trace.append(buat_trace(self.node_id, ke_node, "Pyro5-RMI", "transfer_upsert_sku",
                                        f"kode={kode} metadata diwariskan dari Node {self.node_id}"))
            trace.extend(rpc_result.get("trace", []))
            trace.append(buat_trace(self.node_id, ke_node, "Pyro5-RMI", "transfer_kredit",
                                    f"kode={kode} jumlah={jumlah}"))

            with self._lock:
                self.jumlah_transfer += 1
                self.riwayat_transfer.append({"transfer_id": transfer_id, "ts": time.time(),
                    "kode": kode, "jumlah": jumlah, "ke_node": ke_node, "status": "sukses"})
            return {"ok": True, "transfer_id": transfer_id, "dari_node": self.node_id,
                    "ke_node": ke_node, "kode": kode, "jumlah": jumlah, "trace": trace}
        except Exception as exc:
            code = exc.kode_http if isinstance(exc, StokError) else 502
            # A debit may have committed before any later failure (including
            # metadata/target errors). Always compensate before releasing lock.
            if debit_sukses:
                try:
                    self.store.ubah_stok(kode, jumlah, tipe="rollback",
                                         request_id=f"transfer:{transfer_id}:rollback")
                    trace.append(buat_trace(self.node_id, self.node_id, "ROLLBACK", "auto_rollback",
                                            f"kode={kode} jumlah={jumlah}; error={exc}"))
                except Exception as rollback_error:
                    trace.append(buat_trace(self.node_id, self.node_id, "ROLLBACK", "rollback_gagal",
                                            f"kode={kode}; error={rollback_error}", "error"))
                    exc = RuntimeError(f"{exc}; rollback gagal: {rollback_error}")
                    code = 500
            failure = StokError(str(exc), code)
        finally:
            if lock_acquired:
                try:
                    _rmi_call(lock_node, "LockManager", "release", kode, pemegang, token)
                    trace.append(buat_trace(self.node_id, lock_node, "Pyro5-RMI", "release_lock",
                                            f"kode={kode}"))
                except Exception as release_error:
                    trace.append(buat_trace(self.node_id, lock_node, "Pyro5-RMI", "release_lock",
                                            f"kode={kode}; error={release_error}", "error"))
                # Bila acquire utama sampai ke server tetapi responsnya timeout,
                # token tak diketahui; release berdasarkan owner membebaskan
                # lock yang mungkin berhasil dibuat di Node 1.
                if primary_acquire_uncertain:
                    try:
                        _rmi_call(1, "LockManager", "release", kode, pemegang, None)
                    except Exception:
                        pass
            else:
                trace.append(buat_trace(self.node_id, lock_node, "Pyro5-RMI", "release_lock",
                                        f"kode={kode}; lock tidak diakuisisi", "error"))
            if debit_sukses and failure is not None:
                with self._lock:
                    if not any(x.get("transfer_id") == transfer_id for x in self.riwayat_transfer):
                        self.riwayat_transfer.append({"transfer_id": transfer_id, "ts": time.time(),
                            "kode": kode, "jumlah": jumlah, "ke_node": ke_node, "status": "gagal_rollback"})
            self._transfer_lock.release()
        if failure is not None:
            return {"ok": False, "error": failure.pesan, "kode_http": failure.kode_http, "trace": trace}


@Pyro5.api.expose
class Replika:
    def __init__(self, node_id: int = 4, file_path: Optional[os.PathLike[str] | str] = None) -> None:
        if node_id != 4:
            raise ValueError("Replika aktif hanya pada Node 4")
        self.node_id = node_id
        self.file_path = Path(file_path) if file_path else PROJECT_ROOT / "data" / "replika_node4.json"
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.replikasi: Dict[str, Dict[str, Any]] = {str(i): {"versi": 0, "updated_at": 0.0, "barang": {}}
                                                      for i in (1, 2, 3)}
        if self.file_path.exists():
            self._load()

    def _load(self) -> None:
        with self.file_path.open("r", encoding="utf-8") as stream:
            raw = json.load(stream)
        replicas = raw.get("replikasi", {})
        with self._lock:
            for source in (1, 2, 3):
                value = replicas.get(str(source), {})
                version = value.get("versi", 0)
                goods = value.get("barang", {})
                if isinstance(version, bool) or not isinstance(version, int) or version < 0:
                    raise StokError(f"Versi replika Node {source} tidak valid", 500)
                if not isinstance(goods, dict):
                    raise StokError(f"Snapshot Node {source} tidak valid", 500)
                self.replikasi[str(source)] = {"versi": version,
                    "updated_at": float(value.get("updated_at", 0.0)), "barang": copy.deepcopy(goods)}

    def _write_atomic_locked(self) -> None:
        payload = {"node_id": 4, "updated_at": float(time.time()),
                   "replikasi": copy.deepcopy(self.replikasi)}
        fd, tmp = tempfile.mkstemp(prefix=self.file_path.name + ".", suffix=".tmp",
                                   dir=str(self.file_path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
                json.dump(payload, stream, ensure_ascii=False, allow_nan=False, indent=2)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp, self.file_path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def terima_replikasi(self, sumber: int, versi_baru: int,
                         barang_dict: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(sumber, bool) or sumber not in (1, 2, 3):
            raise StokError("sumber replika harus 1..3", 400)
        if isinstance(versi_baru, bool) or not isinstance(versi_baru, int) or versi_baru < 0:
            raise StokError("versi_baru harus integer nonnegatif", 400)
        if not isinstance(barang_dict, dict):
            raise StokError("barang_dict harus berupa object", 400)
        # Validasi penuh dengan schema StokStore, tanpa mengubah store primer.
        normalized = {}
        for kode, item in barang_dict.items():
            normalized[kode] = StokStore._normalisasi_barang(kode, item)
        key = str(sumber)
        with self._lock:
            current = self.replikasi[key]
            if versi_baru < current["versi"]:
                return {"status": "diabaikan_usang", "versi": current["versi"]}
            if versi_baru == current["versi"]:
                if normalized == current["barang"]:
                    return {"status": "sudah_sama", "versi": current["versi"]}
                raise StokError("Versi replika sama dengan isi berbeda", 409)
            old = copy.deepcopy(current)
            self.replikasi[key] = {"versi": versi_baru, "updated_at": float(time.time()),
                                   "barang": copy.deepcopy(normalized)}
            try:
                self._write_atomic_locked()
            except Exception:
                self.replikasi[key] = old
                raise
        return {"status": "diterapkan", "sumber": sumber, "versi": versi_baru,
                "jumlah_sku": len(normalized)}

    def salinan(self, sumber: int) -> Dict[str, Any]:
        if isinstance(sumber, bool) or sumber not in (1, 2, 3):
            raise StokError("sumber replika harus 1..3", 400)
        with self._lock:
            return copy.deepcopy(self.replikasi[str(sumber)])

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {"node_id": 4, "status": "hidup", "replikasi": {
                source: {"versi": data["versi"], "updated_at": data["updated_at"],
                        "jumlah_sku": len(data["barang"])}
                for source, data in copy.deepcopy(self.replikasi).items()}}

    def sinkron_ulang(self) -> Dict[str, Any]:
        from .rpc_client import panggil_rpc
        hasil: Dict[str, Any] = {}
        traces = []
        for sumber in (1, 2, 3):
            try:
                result = panggil_rpc(4, sumber, "ambil_snapshot")
                snapshot = result["data"]
                traces.extend(result.get("trace", []))
                status = self.terima_replikasi(sumber, snapshot.get("versi", snapshot.get("versi_global")),
                                               copy.deepcopy(snapshot["barang"]))
                hasil[str(sumber)] = status
                traces.append(buat_trace(4, sumber, "XML-RPC", "sinkron_ulang",
                    f"versi={snapshot.get('versi', snapshot.get('versi_global'))}; status={status['status']}"))
            except Exception as exc:
                hasil[str(sumber)] = {"status": "gagal", "error": str(exc)}
                traces.append(buat_trace(4, sumber, "XML-RPC", "sinkron_ulang", str(exc), "error"))
        return {"hasil": hasil, "trace": traces}


def _rmi_call(node_id: int, object_name: str, method: str, *args: Any, **kwargs: Any) -> Any:
    import Pyro5.api
    from .resilience import panggil_dengan_retry
    uri = _uri(node_id, object_name)
    with Pyro5.api.Proxy(uri) as proxy:
        # Transfers can spend up to LOCK_WAIT_TIMEOUT acquiring the primary
        # lock and then another bounded wait on the backup after failover.
        # CRUD RPC remains at 3s; the stateful transfer call must outlive its
        # documented lock wait or the client times out before rollback/trace.
        proxy._pyroTimeout = (2 * LOCK_WAIT_TIMEOUT + RPC_TIMEOUT + 2
                              if object_name == "Cabang" and method == "transfer_stok"
                              else RPC_TIMEOUT)
        result = panggil_dengan_retry(getattr(proxy, method), *args, method=method, **kwargs)
    panggilan("Pyro5-RMI", method, node_id, node_id, "ok")
    return result


def get_lockmanager(node_id: int) -> LockManager:
    with _OBJECTS_LOCK:
        return _OBJECTS.setdefault((node_id, "LockManager"),
                                   LockManager(node_id, lockmanager_cadangan=(node_id == 4)))


def get_cabang(node_id: int, store: Optional[StokStore] = None) -> Cabang:
    with _OBJECTS_LOCK:
        key = (node_id, "Cabang")
        if key not in _OBJECTS or store is not None:
            _OBJECTS[key] = Cabang(node_id, store)
        return _OBJECTS[key]


def get_replika(node_id: int = 4) -> Replika:
    with _OBJECTS_LOCK:
        return _OBJECTS.setdefault((node_id, "Replika"), Replika(node_id))


def jalankan_rmi_server(node_id: int, objects: Optional[Dict[str, Any]] = None) -> None:
    """Expose objek node pada daemon Pyro5 port 600N."""
    daemon = Pyro5.api.Daemon(host=rmi_bind_host(node_id), port=port_rmi(node_id))
    if objects is None:
        if node_id == 1:
            objects = {"LockManager": get_lockmanager(1)}
        elif node_id in (2, 3):
            objects = {"Cabang": get_cabang(node_id)}
        else:
            objects = {"LockManager": get_lockmanager(4), "Replika": get_replika(4)}
    for name, obj in objects.items():
        daemon.register(obj, objectId=name)
    avtree.update_status(node_id, {"status": "hidup", "aktif": True,
                                   "lockmanager_cadangan": node_id == 4})
    try:
        daemon.requestLoop()
    finally:
        daemon.close()
