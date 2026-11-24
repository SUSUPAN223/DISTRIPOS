"""Runner satu node DistriPOS: REST 800N, XML-RPC 500N, Pyro5 600N."""
from __future__ import annotations

import argparse
import signal
import sys
import threading
import time
from typing import Any, Optional

import Pyro5.api
from werkzeug.serving import make_server

from src.common.api import create_app
from src.common.config import BIND_HOST, avtree, port_api, port_rmi, port_rpc, rmi_bind_host
from src.common.core import StokStore
from src.common.heartbeat import HeartbeatMonitor
from src.common.logger import status_node
from src.common.rmi_objects import Cabang, LockManager, Replika, get_cabang, get_lockmanager, get_replika
from src.common.rpc_server import RPCNodeServer


class NodeRuntime:
    def __init__(self, node_id: int, host: Optional[str] = None) -> None:
        self.node_id = node_id
        self.host = host or BIND_HOST
        self.rmi_host = rmi_bind_host(node_id)
        self.stop_event = threading.Event()
        self.rpc_server: Optional[RPCNodeServer] = None
        self.pyro_daemon: Optional[Pyro5.api.Daemon] = None
        self.pyro_thread: Optional[threading.Thread] = None
        self.heartbeat: Optional[HeartbeatMonitor] = None
        self.http_server = None
        self.store: Optional[StokStore] = None
        self.replika: Optional[Replika] = None
        self.recovery_thread: Optional[threading.Thread] = None

    def _start_pyro(self) -> None:
        self.pyro_daemon = Pyro5.api.Daemon(host=self.rmi_host, port=port_rmi(self.node_id))
        if self.node_id == 1:
            objects: dict[str, Any] = {"LockManager": get_lockmanager(1)}
        elif self.node_id in (2, 3):
            objects = {"Cabang": get_cabang(self.node_id, self.store)}
        else:
            objects = {"LockManager": get_lockmanager(4), "Replika": self.replika or get_replika(4)}
        for name, obj in objects.items():
            self.pyro_daemon.register(obj, objectId=name)
        self.pyro_thread = threading.Thread(target=self.pyro_daemon.requestLoop,
                                            name=f"Pyro5-{self.node_id}", daemon=True)
        self.pyro_thread.start()

    def _startup_recovery(self) -> None:
        """Keep retrying recovery in background until startup peers are available."""
        def recover_loop() -> None:
            from src.common.rmi_client import panggil_rmi
            while not self.stop_event.is_set():
                try:
                    if self.node_id in (1, 2, 3) and self.store is not None:
                        self.store.pulihkan_dari_replika(
                            lambda source: panggil_rmi(self.node_id, 4, "Replika", "salinan", source)["data"]
                        )
                        status_node(self.node_id, "recovery_selesai", sumber=4)
                        return
                    if self.node_id == 4 and self.replika is not None:
                        result = self.replika.sinkron_ulang()
                        failed = [n for n, item in result.get("hasil", {}).items()
                                  if item.get("status") == "gagal"]
                        if not failed:
                            status_node(4, "sinkron_awal_selesai")
                            return
                        status_node(4, "sinkron_menunggu_node", gagal=failed)
                except Exception as exc:
                    status_node(self.node_id, "recovery_menunggu_peer", error=str(exc))
                self.stop_event.wait(2.0)

        self.recovery_thread = threading.Thread(target=recover_loop,
            name=f"Recovery-{self.node_id}", daemon=True)
        self.recovery_thread.start()

    def start(self) -> None:
        if self.node_id in (1, 2, 3):
            self.store = StokStore(self.node_id)
        else:
            self.replika = get_replika(4)

        self.rpc_server = RPCNodeServer(self.node_id, host=self.host, store=self.store)
        self.rpc_server.start()
        self._start_pyro()
        self.heartbeat = HeartbeatMonitor(self.node_id, include_self=True, autostart=True)
        self._startup_recovery()
        app = create_app(self.node_id, self.heartbeat)
        self.http_server = make_server(self.host, port_api(self.node_id), app, threaded=True)
        avtree.update_status(self.node_id, {"status": "hidup", "aktif": True})
        status_node(self.node_id, "hidup", rpc_port=port_rpc(self.node_id),
                    rmi_port=port_rmi(self.node_id), api_port=port_api(self.node_id))
        self.http_server.serve_forever()

    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        if self.heartbeat:
            self.heartbeat.stop(timeout=2.0)
        if self.recovery_thread and self.recovery_thread.is_alive():
            self.recovery_thread.join(timeout=2.0)
        if self.http_server:
            self.http_server.shutdown()
            self.http_server.server_close()
        if self.rpc_server:
            self.rpc_server.shutdown()
        if self.pyro_daemon:
            self.pyro_daemon.shutdown()
            self.pyro_daemon.close()
        avtree.update_status(self.node_id, {"status": "mati", "aktif": False})
        status_node(self.node_id, "mati", perubahan="graceful_shutdown")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", type=int, required=True, choices=(1, 2, 3, 4))
    parser.add_argument("--host", default=BIND_HOST,
                        help="alamat bind REST/XML-RPC; Pyro memakai RMI_BIND_HOST_N")
    args = parser.parse_args(argv)
    runtime = NodeRuntime(args.node, args.host)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: runtime.stop())
    try:
        runtime.start()
    except KeyboardInterrupt:
        runtime.stop()
    except OSError as exc:
        print(f"Node {args.node} gagal start: {exc}", file=sys.stderr)
        runtime.stop()
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
