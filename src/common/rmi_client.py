"""Pyro5 client dengan timeout, retry sesuai semantik dan failover LockManager."""
from __future__ import annotations

from typing import Any, Dict, Optional

import Pyro5.api
from Pyro5.errors import PyroError

from .config import LEASE_DEFAULT, LOCK_WAIT_TIMEOUT, RPC_TIMEOUT, host_node, port_rmi
from .logger import buat_trace, failover, panggilan
from .resilience import panggil_dengan_retry


def _uri(tujuan: int, nama_objek: str) -> str:
    if isinstance(tujuan, bool) or not isinstance(tujuan, int) or tujuan not in range(1, 5):
        raise ValueError("tujuan harus node 1..4")
    if nama_objek not in {"LockManager", "Cabang", "Replika"}:
        raise ValueError("nama_objek tidak dikenal")
    return f"PYRO:{nama_objek}@{host_node(tujuan)}:{port_rmi(tujuan)}"


def panggil_rmi(pemanggil: int, tujuan: int, nama_objek: str, method: str,
                *args: Any, **kwargs: Any) -> Dict[str, Any]:
    if isinstance(pemanggil, bool) or not isinstance(pemanggil, int) or pemanggil not in range(1, 5):
        raise ValueError("pemanggil harus node 1..4")
    if not isinstance(method, str) or not method or method.startswith("_"):
        raise ValueError("method RMI tidak valid")
    uri = _uri(tujuan, nama_objek)
    trace = buat_trace(pemanggil, tujuan, "Pyro5-RMI", method, f"objek={nama_objek}")
    try:
        with Pyro5.api.Proxy(uri) as proxy:
            proxy._pyroTimeout = RPC_TIMEOUT
            result = panggil_dengan_retry(getattr(proxy, method), *args, method=method,
                                          timeout=RPC_TIMEOUT, **kwargs)
    except PyroError as exc:
        raise ConnectionError(str(exc)) from exc
    panggilan("Pyro5-RMI", method, pemanggil, tujuan, "ok")
    if isinstance(result, dict) and "trace" in result and isinstance(result["trace"], list):
        return {"data": result, "trace": [trace, *result["trace"]]}
    return {"data": result, "trace": [trace]}


def panggil_lockmanager_dengan_failover(
    pemanggil: int,
    method: str,
    *args: Any,
    lease: float = LEASE_DEFAULT,
    tunggu: float = LOCK_WAIT_TIMEOUT,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Coba LockManager Node 1; bila gagal gunakan cadangan Node 4."""
    if method not in {"acquire", "release", "status"}:
        raise ValueError("method LockManager tidak dikenal")
    acquire_args: tuple[Any, ...] = ()
    if method == "acquire":
        if len(args) < 2:
            raise TypeError("acquire memerlukan kode dan pemegang")
        acquire_args = (kwargs.pop("lease", lease), kwargs.pop("tunggu", tunggu))
    try:
        result = panggil_rmi(pemanggil, 1, "LockManager", method,
                             *args, *acquire_args, **kwargs)
        return {**result, "lockmanager_node": 1}
    except Exception as primary_error:
        failover(pemanggil, 4, "lockmanager_failover", error=str(primary_error))
        fallback_trace = buat_trace(pemanggil, 1, "FAILOVER", "lockmanager_failover",
                                    f"Node 1 gagal: {primary_error}", "error")
        fallback = panggil_rmi(pemanggil, 4, "LockManager", method,
                               *args, *acquire_args, **kwargs)
        return {"data": fallback["data"], "lockmanager_node": 4,
                "trace": [fallback_trace, *fallback["trace"]]}


def panggil_lockmanager(pemanggil: int, method: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return panggil_lockmanager_dengan_failover(pemanggil, method, *args, **kwargs)
