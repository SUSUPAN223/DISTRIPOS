"""XML-RPC client bertimeout, retry semantik, serta lineage trace."""
from __future__ import annotations

import socket
import http.client
import xmlrpc.client
from typing import Any, Dict, Tuple

from .config import RPC_TIMEOUT, host_node, port_rpc
from .logger import buat_trace, panggilan
from .resilience import panggil_dengan_retry


class TimeoutTransport(xmlrpc.client.Transport):
    def __init__(self, timeout: float = RPC_TIMEOUT, use_datetime: bool = False) -> None:
        super().__init__(use_datetime=use_datetime)
        if timeout <= 0:
            raise ValueError("timeout harus positif")
        self.timeout = float(timeout)

    def make_connection(self, host: str) -> http.client.HTTPConnection:
        connection = super().make_connection(host)
        connection.timeout = self.timeout
        return connection


class _ProxyContext:
    def __init__(self, url: str) -> None:
        self.proxy = xmlrpc.client.ServerProxy(
            url, transport=TimeoutTransport(RPC_TIMEOUT), allow_none=True,
        )

    def __enter__(self) -> xmlrpc.client.ServerProxy:
        return self.proxy

    def __exit__(self, *_: Any) -> None:
        transport = getattr(self.proxy, "_ServerProxy__transport", None)
        if transport is not None:
            transport.close()


def _parse_result(result: Any) -> Tuple[Any, list[Dict[str, Any]]]:
    if not isinstance(result, dict) or "ok" not in result:
        return result, []
    if not result["ok"]:
        raise RemoteCallError(
            str(result.get("error", "RPC gagal")),
            int(result.get("kode_http", 500)),
            result.get("trace", []),
        )
    return result.get("data"), result.get("trace", []) or []


class RemoteCallError(RuntimeError):
    def __init__(self, message: str, kode_http: int = 500,
                 trace: Optional[list[Dict[str, Any]]] = None) -> None:
        super().__init__(message)
        self.kode_http = kode_http
        self.trace = trace or []


def panggil_rpc(pemanggil: int, tujuan: int, method: str, *args: Any) -> Dict[str, Any]:
    if isinstance(pemanggil, bool) or pemanggil not in range(1, 5):
        raise ValueError("pemanggil harus node 1..4")
    if isinstance(tujuan, bool) or tujuan not in range(1, 5):
        raise ValueError("tujuan harus node 1..4")
    if not isinstance(method, str) or not method or method.startswith("_"):
        raise ValueError("method RPC tidak valid")
    url = f"http://{host_node(tujuan)}:{port_rpc(tujuan)}"
    trace = buat_trace(pemanggil, tujuan, "XML-RPC", method, f"args={len(args)}")
    with _ProxyContext(url) as proxy:
        remote_method = getattr(proxy, method)
        result = panggil_dengan_retry(remote_method, *args, method=method, timeout=RPC_TIMEOUT)
    # Envelope error aplikasi bukan exception jaringan dan tidak boleh diretry.
    if isinstance(result, dict) and result.get("ok") is False:
        _parse_result(result)
    data, remote_trace = _parse_result(result)
    panggilan("XML-RPC", method, pemanggil, tujuan, "ok")
    return {"data": data, "trace": [trace, *remote_trace]}


def buat_proxy(tujuan: int) -> _ProxyContext:
    """Factory proxy XML-RPC bertimeout untuk integrasi internal."""
    return _ProxyContext(f"http://{host_node(tujuan)}:{port_rpc(tujuan)}")
