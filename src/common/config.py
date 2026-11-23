"""Konfigurasi bersama node DistriPOS."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

RPC_BASE_PORT = int(os.getenv("RPC_BASE_PORT", "5000"))
RMI_BASE_PORT = int(os.getenv("RMI_BASE_PORT", "6000"))
API_BASE_PORT = int(os.getenv("API_BASE_PORT", "8000"))
LAUNCHER_HOST = os.getenv("LAUNCHER_HOST", "127.0.0.1")
LAUNCHER_PORT = int(os.getenv("LAUNCHER_PORT", "8000"))

RPC_TIMEOUT = 3.0
MAX_RETRY = 3
RETRY_BACKOFF = [1, 2]
LEASE_DEFAULT = 10.0
LOCK_WAIT_TIMEOUT = 12.0
HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_MAX_FAIL = 3
CHORD_M = 8
LRU_CAPACITY = 64
INT32_MIN = -2147483648
INT32_MAX = 2147483647
TIDAK_IDEMPOTEN = {"tambah_barang", "ubah_stok", "hapus_barang", "transfer_stok"}

HOSTS: Dict[int, str] = {
    node_id: os.getenv(f"HOST_NODE{node_id}", "127.0.0.1")
    for node_id in range(1, 5)
}


def validasi_node_id(node_id: int) -> int:
    if isinstance(node_id, bool) or not isinstance(node_id, int) or node_id not in range(1, 5):
        raise ValueError("node_id harus berupa integer 1..4")
    return node_id


def host_node(node_id: int) -> str:
    return HOSTS[validasi_node_id(node_id)]


def port_rpc(node_id: int) -> int:
    return RPC_BASE_PORT + validasi_node_id(node_id)


def port_rmi(node_id: int) -> int:
    return RMI_BASE_PORT + validasi_node_id(node_id)


def port_api(node_id: int) -> int:
    return API_BASE_PORT + validasi_node_id(node_id)


# Alias eksplisit memudahkan pemanggil yang menginginkan penamaan get_*.
get_rpc_port = port_rpc
get_rmi_port = port_rmi
get_api_port = port_api


def path_data_node(node_id: int) -> Path:
    node_id = validasi_node_id(node_id)
    if node_id == 4:
        return PROJECT_ROOT / "data" / "replika_node4.json"
    return PROJECT_ROOT / "data" / f"data_node{node_id}.json"


def path_data(node_id: int) -> Path:
    return path_data_node(node_id)


class AVTree:
    """Attribute-Value Tree sederhana dengan pencocokan atribut ter-subset."""

    def __init__(self, nodes: Optional[Iterable[Dict[str, Any]]] = None) -> None:
        self._nodes: Dict[int, Dict[str, Any]] = {}
        for node in nodes or self._default_nodes():
            item = dict(node)
            node_id = validasi_node_id(item.pop("node_id"))
            self._nodes[node_id] = item

    @staticmethod
    def _default_nodes() -> List[Dict[str, Any]]:
        return [
            {"node_id": 1, "peran": "pusat", "aktif": True, "lockmanager_cadangan": False},
            {"node_id": 2, "peran": "cabang", "aktif": True, "lockmanager_cadangan": False},
            {"node_id": 3, "peran": "cabang", "aktif": True, "lockmanager_cadangan": False},
            {"node_id": 4, "peran": "replika", "aktif": True, "lockmanager_cadangan": False},
        ]

    @property
    def tree(self) -> Dict[str, Any]:
        """Representasi atribut bertingkat yang dapat diinspeksi."""
        result: Dict[str, Any] = {}
        for node_id, attrs in self._nodes.items():
            branch = result
            for key in sorted(attrs):
                branch = branch.setdefault(key, {})
            branch["node_id"] = node_id
        return result

    def query(self, kriteria_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(kriteria_dict, dict):
            raise TypeError("kriteria_dict harus berupa dict")
        criteria = dict(kriteria_dict)
        # Alias semantik: pusat adalah node 1, fallback LockManager adalah node 4.
        role = criteria.get("peran")
        if role == "pusat":
            criteria.setdefault("node_id", 1)
        elif role == "lockmanager_cadangan":
            criteria.pop("peran")
            criteria["node_id"] = 4
            criteria["lockmanager_cadangan"] = True
        elif role == "cabang_aktif":
            criteria.pop("peran")
            criteria["peran"] = "cabang"
            criteria["aktif"] = True

        found: List[Dict[str, Any]] = []
        for node_id, attrs in self._nodes.items():
            candidate = {"node_id": node_id, **attrs}
            if all(candidate.get(key) == value for key, value in criteria.items()):
                found.append(candidate)
        return sorted(found, key=lambda item: item["node_id"])

    def update_status(self, node_id: int, status: Any) -> Dict[str, Any]:
        node_id = validasi_node_id(node_id)
        if node_id not in self._nodes:
            role = "pusat" if node_id == 1 else "replika" if node_id == 4 else "cabang"
            self._nodes[node_id] = {"peran": role, "aktif": False, "lockmanager_cadangan": False}
        if isinstance(status, bool):
            self._nodes[node_id]["aktif"] = status
        elif isinstance(status, str):
            self._nodes[node_id]["status"] = status
            self._nodes[node_id]["aktif"] = status.lower() in {"aktif", "up", "healthy", "running"}
        elif isinstance(status, dict):
            self._nodes[node_id].update(status)
            if "status" in status and "aktif" not in status:
                self._nodes[node_id]["aktif"] = str(status["status"]).lower() in {
                    "aktif", "up", "healthy", "running"
                }
        else:
            raise TypeError("status harus bool, str, atau dict")
        return {"node_id": node_id, **self._nodes[node_id]}

    def aktifkan_lockmanager_cadangan(self) -> Dict[str, Any]:
        for node_id, attrs in self._nodes.items():
            attrs["lockmanager_cadangan"] = node_id == 4
        self._nodes.setdefault(4, {"peran": "replika", "aktif": True})
        self._nodes[4]["lockmanager_cadangan"] = True
        self._nodes[4]["aktif"] = True
        return {"node_id": 4, **self._nodes[4]}

    def cabang_aktif(self) -> List[Dict[str, Any]]:
        return self.query({"peran": "cabang_aktif"})


avtree = AVTree()
