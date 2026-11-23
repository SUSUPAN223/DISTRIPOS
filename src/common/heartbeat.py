"""Background heartbeat monitor untuk node DistriPOS."""
from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, Optional

from .config import HEARTBEAT_INTERVAL, HEARTBEAT_MAX_FAIL, HOSTS, avtree, port_rpc
from . import logger


class HeartbeatMonitor(threading.Thread):
    def __init__(
        self,
        node_id: int,
        ping_fn: Optional[Callable[[int], Any]] = None,
        *,
        interval: float = HEARTBEAT_INTERVAL,
        max_fail: int = HEARTBEAT_MAX_FAIL,
        tree: Any = None,
        include_self: bool = False,
        autostart: bool = True,
    ) -> None:
        super().__init__(name=f"HeartbeatMonitor-{node_id}", daemon=True)
        if isinstance(node_id, bool) or not isinstance(node_id, int) or node_id not in range(1, 5):
            raise ValueError("node_id harus integer 1..4")
        if interval <= 0 or max_fail < 1:
            raise ValueError("interval dan max_fail harus positif")
        self.node_id = node_id
        self.interval = float(interval)
        self.max_fail = int(max_fail)
        self.tree = tree or avtree
        self.ping_fn = ping_fn or self._ping_rpc
        self.nodes = tuple(range(1, 5)) if include_self else tuple(n for n in range(1, 5) if n != node_id)
        self.gagal_berturut: Dict[int, int] = {n: 0 for n in self.nodes}
        self.status_node: Dict[int, str] = {n: "unknown" for n in self.nodes}
        self.riwayat: Dict[int, Dict[str, Any]] = {
            n: {"last_success": None, "last_attempt": None, "latensi_ms": None,
                "last_error": None, "gagal_berturut": 0}
            for n in self.nodes
        }
        self._stop_event = threading.Event()
        self._state_lock = threading.RLock()
        if autostart:
            self.start()

    def _ping_rpc(self, node_id: int) -> Any:
        import xmlrpc.client

        from .config import RPC_TIMEOUT, host_node
        from .resilience import panggil_dengan_retry
        from .rpc_client import TimeoutTransport

        url = f"http://{host_node(node_id)}:{port_rpc(node_id)}"
        with xmlrpc.client.ServerProxy(url, transport=TimeoutTransport(RPC_TIMEOUT), allow_none=True) as proxy:
            envelope = panggil_dengan_retry(proxy.ping, method="ping")
        if isinstance(envelope, dict) and envelope.get("ok") is False:
            raise ConnectionError(envelope.get("error", "ping gagal"))
        return envelope

    def ping_node(self, node_id: int) -> bool:
        started = time.monotonic()
        attempt_at = time.time()
        try:
            self.ping_fn(node_id)
            latency_ms = (time.monotonic() - started) * 1000.0
            with self._state_lock:
                previous = self.status_node.get(node_id)
                self.gagal_berturut[node_id] = 0
                self.status_node[node_id] = "hidup"
                record = self.riwayat.setdefault(node_id, {})
                record.update({"last_success": time.time(), "last_attempt": attempt_at,
                               "latensi_ms": round(latency_ms, 3), "last_error": None,
                               "gagal_berturut": 0})
                self.tree.update_status(node_id, {"status": "hidup", "aktif": True})
            logger.heartbeat(node_id, "hidup", 0, latensi_ms=round(latency_ms, 3))
            if previous == "mati":
                logger.status_node(node_id, "hidup", perubahan="heartbeat_recovery")
            return True
        except Exception as exc:
            with self._state_lock:
                count = self.gagal_berturut.get(node_id, 0) + 1
                self.gagal_berturut[node_id] = count
                record = self.riwayat.setdefault(node_id, {})
                record.update({"last_attempt": attempt_at, "last_error": str(exc),
                               "gagal_berturut": count})
                if count >= self.max_fail:
                    previous = self.status_node.get(node_id)
                    self.status_node[node_id] = "mati"
                    self.tree.update_status(node_id, {"status": "mati", "aktif": False})
                else:
                    previous = self.status_node.get(node_id)
            logger.heartbeat(node_id, "gagal", count, error=str(exc))
            if count >= self.max_fail and previous != "mati":
                logger.status_node(node_id, "mati", perubahan="heartbeat_failover", gagal_berturut=count)
                logger.failover(node_id, 4 if node_id != 4 else 1, "node_down")
            return False

    def ping_all(self) -> Dict[int, bool]:
        return {node: self.ping_node(node) for node in self.nodes}

    def run(self) -> None:
        while not self._stop_event.is_set():
            started = time.monotonic()
            self.ping_all()
            remaining = self.interval - (time.monotonic() - started)
            if remaining > 0:
                self._stop_event.wait(remaining)

    def stop(self, timeout: Optional[float] = None) -> None:
        self._stop_event.set()
        if self.is_alive() and threading.current_thread() is not self:
            self.join(timeout)

    def snapshot_status(self) -> Dict[int, Dict[str, Any]]:
        with self._state_lock:
            return {
                node: {"status": self.status_node[node], **dict(self.riwayat[node])}
                for node in self.nodes
            }
