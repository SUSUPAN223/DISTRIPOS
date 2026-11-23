"""Penyimpanan stok lokal berversi, cache LRU, dan persistensi atomik."""
from __future__ import annotations

import copy
import json
import os
import tempfile
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from .config import INT32_MAX, INT32_MIN, LRU_CAPACITY, PROJECT_ROOT, path_data_node, validasi_node_id
from . import logger


class StokError(Exception):
    def __init__(self, pesan: str, kode_http: int = 400) -> None:
        super().__init__(pesan)
        self.pesan = str(pesan)
        self.kode_http = int(kode_http)

    def __str__(self) -> str:
        return self.pesan


class LRUCache:
    """Cache LRU thread-safe dengan kapasitas tetap dan statistik hit/miss."""

    def __init__(self, kapasitas: int = LRU_CAPACITY) -> None:
        if isinstance(kapasitas, bool) or not isinstance(kapasitas, int) or kapasitas < 1:
            raise ValueError("kapasitas cache harus integer positif")
        self.kapasitas = kapasitas
        self._data: OrderedDict[Any, Any] = OrderedDict()
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0

    def get(self, key: Any, default: Any = None) -> Any:
        with self._lock:
            if key not in self._data:
                self.misses += 1
                return default
            self.hits += 1
            self._data.move_to_end(key)
            return copy.deepcopy(self._data[key])

    def put(self, key: Any, value: Any) -> None:
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
            self._data[key] = copy.deepcopy(value)
            while len(self._data) > self.kapasitas:
                self._data.popitem(last=False)

    def invalidate(self, key: Any) -> None:
        with self._lock:
            self._data.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


class StokStore:
    RIWAYAT_MAKS = 200

    def __init__(
        self,
        node_id: int,
        file_path: Optional[os.PathLike[str] | str] = None,
        cache_capacity: int = LRU_CAPACITY,
        auto_load: bool = True,
    ) -> None:
        self.node_id = validasi_node_id(node_id)
        self.file_path = Path(file_path) if file_path is not None else path_data_node(self.node_id)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self._lock = self.lock
        self.cache = LRUCache(cache_capacity)
        self.barang: Dict[str, Dict[str, Any]] = {}
        self.riwayat_mutasi: List[Dict[str, Any]] = []
        self.versi_global = 0
        self.updated_at = 0.0
        self._request_results: Dict[str, Dict[str, Any]] = {}
        if auto_load and self.file_path.exists():
            self._muat()
        elif auto_load and self.node_id in (1, 2, 3):
            self._simpan_atomik_locked()

    @staticmethod
    def _validasi_int32(nama: str, nilai: Any, *, minimum: int = INT32_MIN) -> int:
        if isinstance(nilai, bool) or not isinstance(nilai, int):
            raise StokError(f"{nama} harus berupa integer", 400)
        if nilai < max(INT32_MIN, minimum) or nilai > INT32_MAX:
            raise StokError(f"{nama} harus berada dalam rentang int32", 400)
        return nilai

    @staticmethod
    def _validasi_teks(nama: str, nilai: Any) -> str:
        if not isinstance(nilai, str) or not nilai.strip():
            raise StokError(f"{nama} wajib berupa teks tidak kosong", 400)
        return nilai.strip()

    @staticmethod
    def _normalisasi_barang(kode: str, item: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(item, dict):
            raise StokError("Setiap barang harus berupa objek", 400)
        item_kode = StokStore._validasi_teks("kode", item.get("kode", kode))
        if item_kode != kode:
            raise StokError("Key barang harus sama dengan field kode", 400)
        nama = StokStore._validasi_teks("nama", item.get("nama"))
        stok = StokStore._validasi_int32("stok", item.get("stok"))
        harga = StokStore._validasi_int32("harga", item.get("harga"), minimum=0)
        min_stok = StokStore._validasi_int32("min_stok", item.get("min_stok"), minimum=0)
        updated_at = item.get("updated_at", time.time())
        if isinstance(updated_at, bool) or not isinstance(updated_at, (int, float)):
            raise StokError("updated_at harus float64", 400)
        return {
            "kode": kode,
            "nama": nama,
            "stok": stok,
            "harga": harga,
            "min_stok": min_stok,
            "updated_at": float(updated_at),
        }

    def _dokumen_locked(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "versi_global": self.versi_global,
            "updated_at": float(self.updated_at),
            "barang": copy.deepcopy(self.barang),
            "riwayat_mutasi": copy.deepcopy(self.riwayat_mutasi[-self.RIWAYAT_MAKS:]),
        }

    def _simpan_atomik_locked(self) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        dokumen = self._dokumen_locked()
        fd, tmp_name = tempfile.mkstemp(prefix=self.file_path.name + ".", suffix=".tmp", dir=str(self.file_path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
                json.dump(dokumen, stream, ensure_ascii=False, allow_nan=False, indent=2)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp_name, self.file_path)
            # Fsync direktori bersifat best-effort lintas platform (Windows tidak membuka dir sebagai fd).
            if os.name != "nt":
                dir_fd = os.open(self.file_path.parent, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
        except Exception:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def simpan_atomik(self) -> None:
        with self.lock:
            self._simpan_atomik_locked()

    def _muat(self) -> None:
        try:
            with self.file_path.open("r", encoding="utf-8") as stream:
                data = json.load(stream)
            if not isinstance(data, dict) or data.get("node_id") != self.node_id:
                raise ValueError("node_id dokumen tidak sesuai")
            versi = self._validasi_int32("versi_global", data.get("versi_global"), minimum=0)
            updated = data.get("updated_at", 0.0)
            if isinstance(updated, bool) or not isinstance(updated, (int, float)):
                raise ValueError("updated_at harus numerik")
            barang_raw = data.get("barang", {})
            if not isinstance(barang_raw, dict):
                raise ValueError("barang harus berupa object")
            barang: Dict[str, Dict[str, Any]] = {}
            for kode, item in barang_raw.items():
                kode = self._validasi_teks("kode", kode)
                barang[kode] = self._normalisasi_barang(kode, item)
            riwayat = data.get("riwayat_mutasi", [])
            if not isinstance(riwayat, list):
                raise ValueError("riwayat_mutasi harus berupa array")
            with self.lock:
                self.versi_global = versi
                self.updated_at = float(updated)
                self.barang = barang
                self.riwayat_mutasi = copy.deepcopy(riwayat[-self.RIWAYAT_MAKS:])
                self.cache.clear()
        except (json.JSONDecodeError, OSError, ValueError, TypeError, StokError) as exc:
            raise StokError(f"Dokumen data tidak valid ({self.file_path}): {exc}", 500) from exc

    def _catat_mutasi_locked(self, kode: str, delta: int, tipe: str, stok_akhir: int) -> None:
        self.riwayat_mutasi.append({
            "ts": float(time.time()),
            "kode": kode,
            "delta": delta,
            "tipe": tipe,
            "stok_akhir": stok_akhir,
        })
        self.riwayat_mutasi = self.riwayat_mutasi[-self.RIWAYAT_MAKS:]

    def _commit_versi_locked(self, aksi: str) -> None:
        old = self.versi_global
        self.versi_global = self._validasi_int32("versi_global", old + 1, minimum=0)
        self.updated_at = float(time.time())
        try:
            self._simpan_atomik_locked()
        except Exception:
            self.versi_global = old
            raise
        logger.mutasi_versi(self.node_id, old, self.versi_global, aksi)

    def daftar(self) -> List[Dict[str, Any]]:
        with self.lock:
            return copy.deepcopy([self.barang[k] for k in sorted(self.barang)])

    def get_stok(self, kode: str) -> Dict[str, Any]:
        kode = self._validasi_teks("kode", kode)
        cached = self.cache.get(kode, None)
        if cached is not None:
            return cached
        with self.lock:
            item = self.barang.get(kode)
            if item is None:
                raise StokError(f"Barang dengan kode {kode!r} tidak ditemukan", 404)
            result = copy.deepcopy(item)
            self.cache.put(kode, result)
            return result

    def tambah_barang(
        self,
        kode: str,
        nama: str,
        stok: int,
        harga: int = 50000,
        min_stok: int = 15,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        kode = self._validasi_teks("kode", kode)
        nama = self._validasi_teks("nama", nama)
        stok = self._validasi_int32("stok", stok)
        harga = self._validasi_int32("harga", harga, minimum=0)
        min_stok = self._validasi_int32("min_stok", min_stok, minimum=0)
        with self.lock:
            if request_id and request_id in self._request_results:
                return copy.deepcopy(self._request_results[request_id])
            if kode in self.barang:
                raise StokError(f"Barang dengan kode {kode!r} sudah ada", 409)
            now = float(time.time())
            self.barang[kode] = {"kode": kode, "nama": nama, "stok": stok,
                                 "harga": harga, "min_stok": min_stok, "updated_at": now}
            self.cache.invalidate(kode)
            self._catat_mutasi_locked(kode, stok, "restock", stok)
            self._commit_versi_locked("tambah_barang")
            result = copy.deepcopy(self.barang[kode])
            if request_id:
                self._request_results[request_id] = copy.deepcopy(result)
            return result

    def ubah_stok(
        self,
        kode: str,
        jumlah: int,
        tipe: str = "kasir",
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        kode = self._validasi_teks("kode", kode)
        jumlah = self._validasi_int32("jumlah", jumlah)
        tipe = self._validasi_teks("tipe", tipe)
        with self.lock:
            if request_id and request_id in self._request_results:
                return copy.deepcopy(self._request_results[request_id])
            item = self.barang.get(kode)
            if item is None:
                raise StokError(f"Barang dengan kode {kode!r} tidak ditemukan", 404)
            stok_baru = item["stok"] + jumlah
            self._validasi_int32("stok", stok_baru)
            if stok_baru < 0:
                raise StokError("Stok tidak boleh negatif", 422)
            lama = copy.deepcopy(item)
            try:
                now = float(time.time())
                item["stok"] = stok_baru
                item["updated_at"] = now
                self.cache.invalidate(kode)
                self._catat_mutasi_locked(kode, jumlah, tipe, stok_baru)
                self._commit_versi_locked("ubah_stok")
            except Exception:
                self.barang[kode] = lama
                self.cache.invalidate(kode)
                raise
            result = copy.deepcopy(item)
            if request_id:
                self._request_results[request_id] = copy.deepcopy(result)
            return result

    def hapus_barang(self, kode: str, request_id: Optional[str] = None) -> Dict[str, Any]:
        kode = self._validasi_teks("kode", kode)
        with self.lock:
            if request_id and request_id in self._request_results:
                return copy.deepcopy(self._request_results[request_id])
            item = self.barang.get(kode)
            if item is None:
                raise StokError(f"Barang dengan kode {kode!r} tidak ditemukan", 404)
            removed = copy.deepcopy(item)
            del self.barang[kode]
            self.cache.invalidate(kode)
            try:
                self._commit_versi_locked("hapus_barang")
            except Exception:
                self.barang[kode] = removed
                raise
            if request_id:
                self._request_results[request_id] = {"kode": kode, "dihapus": True}
            return removed

    def hitung_total(self) -> Dict[str, int]:
        with self.lock:
            return {"total_stok": sum(item["stok"] for item in self.barang.values()),
                    "jumlah_sku": len(self.barang)}

    def seed_data_awal(self, data: Optional[Iterable[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        if data is None:
            data = [
                {"kode": "SKU-001", "nama": "Beras 5kg", "stok": 40, "harga": 75000, "min_stok": 10},
                {"kode": "SKU-002", "nama": "Minyak Goreng 1L", "stok": 35, "harga": 18000, "min_stok": 8},
                {"kode": "SKU-003", "nama": "Gula Pasir 1kg", "stok": 25, "harga": 16000, "min_stok": 5},
            ]
        if not isinstance(data, Iterable):
            raise StokError("Data seed harus berupa iterable barang", 400)
        results = []
        for item in data:
            if not isinstance(item, dict):
                raise StokError("Setiap item seed harus berupa objek", 400)
            results.append(self.tambah_barang(
                item.get("kode"), item.get("nama"), item.get("stok"),
                item.get("harga", 50000), item.get("min_stok", 15),
                request_id=f"seed:{self.node_id}:{item.get('kode')}"))
        return results

    def ambil_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {"node_id": self.node_id, "versi": self.versi_global,
                    "updated_at": float(self.updated_at), "barang": copy.deepcopy(self.barang),
                    "riwayat_mutasi": copy.deepcopy(self.riwayat_mutasi)}

    def terapkan_snapshot(self, versi: int, barang_dict: Dict[str, Dict[str, Any]]) -> str:
        versi = self._validasi_int32("versi", versi, minimum=0)
        if not isinstance(barang_dict, dict):
            raise StokError("barang_dict harus berupa object", 400)
        normalized: Dict[str, Dict[str, Any]] = {}
        for kode, item in barang_dict.items():
            kode = self._validasi_teks("kode", kode)
            normalized[kode] = self._normalisasi_barang(kode, item)
        with self.lock:
            if versi < self.versi_global:
                return "diabaikan_usang"
            if versi == self.versi_global:
                if normalized == self.barang:
                    return "sudah_sama"
                raise StokError("Versi sama tetapi isi snapshot berbeda", 409)
            old = (self.versi_global, self.updated_at, self.barang, self.riwayat_mutasi)
            self.versi_global = versi
            self.updated_at = float(time.time())
            self.barang = copy.deepcopy(normalized)
            self.cache.clear()
            try:
                self._simpan_atomik_locked()
            except Exception:
                self.versi_global, self.updated_at, self.barang, self.riwayat_mutasi = old
                raise
            logger.mutasi_versi(self.node_id, old[0], versi, "terapkan_snapshot")
            return "diterapkan"

    def pulihkan_dari_replika(self, rmi_client_fn: Callable[..., Any]) -> str:
        """Ambil snapshot Node 4 dan pulihkan hanya bila versinya lebih baru."""
        try:
            try:
                snapshot = rmi_client_fn(self.node_id)
            except TypeError:
                snapshot = rmi_client_fn()
        except Exception as exc:
            raise StokError(f"Gagal mengambil snapshot replika: {exc}", 503) from exc
        if not isinstance(snapshot, dict):
            raise StokError("Snapshot replika tidak valid", 502)
        versi = snapshot.get("versi", snapshot.get("versi_global"))
        barang = snapshot.get("barang")
        if versi is None or not isinstance(barang, dict):
            raise StokError("Snapshot replika harus memuat versi dan barang", 502)
        return self.terapkan_snapshot(versi, barang)
