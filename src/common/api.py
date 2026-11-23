"""Stateless Flask REST Gateway ke XML-RPC, Pyro5, Chord dan MPI."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from flask import Flask, jsonify, request
from flask_cors import CORS

from .chord_dht import ChordNode
from .config import HOSTS, PROJECT_ROOT, avtree, host_node
from .core import StokError
from .heartbeat import HeartbeatMonitor
from .logger import buat_trace
from .rmi_client import panggil_lockmanager_dengan_failover, panggil_rmi
from .rpc_client import RemoteCallError, panggil_rpc


def create_app(node_id: int = 1, heartbeat_monitor: Optional[HeartbeatMonitor] = None) -> Flask:
    if isinstance(node_id, bool) or not isinstance(node_id, int) or node_id not in range(1, 5):
        raise ValueError("node_id harus integer 1..4")
    app = Flask(f"distripos-node-{node_id}")
    CORS(app)
    app.config["NODE_ID"] = node_id
    app.config["HEARTBEAT_MONITOR"] = heartbeat_monitor

    def response(ok: bool, data: Any = None, *, via: str, tujuan: int,
                 trace: Optional[list[dict[str, Any]]] = None, status: int = 200,
                 error: Any = None, **extra: Any):
        payload = {"ok": bool(ok), "data": data, "error": error,
                   "via": via, "node_tujuan": int(tujuan), "trace": trace or [], **extra}
        return jsonify(payload), status

    def error_response(exc: Exception, tujuan: int, via: str = "RPC", trace=None):
        code = getattr(exc, "kode_http", 502 if isinstance(exc, (TimeoutError, ConnectionError, OSError)) else 500)
        if isinstance(exc, RemoteCallError):
            code = exc.kode_http
        code = getattr(exc, "kode_http", code)
        trace = trace if trace is not None else getattr(exc, "trace", [])
        return response(False, None, via=via, tujuan=tujuan, trace=trace,
                        status=code, error={"message": str(exc), "code": code})

    @app.after_request
    def ensure_contract_headers(resp):
        # Seluruh error Flask yang berasal dari routing/JSON juga memenuhi
        # metadata envelope minimum yang dipakai dashboard.
        if resp.is_json:
            try:
                payload = resp.get_json()
                if isinstance(payload, dict):
                    payload.setdefault("ok", resp.status_code < 400)
                    payload.setdefault("via", "RPC")
                    payload.setdefault("node_tujuan", node_id)
                    payload.setdefault("trace", [])
                    payload.setdefault("data", None)
                    payload.setdefault("error", None)
                    resp.set_data(app.json.dumps(payload))
            except (TypeError, ValueError):
                pass
        return resp

    def node_arg(default: Optional[int] = None) -> int:
        try:
            raw = int(request.args.get("node", str(default if default is not None else node_id)))
        except (TypeError, ValueError) as exc:
            raise StokError("node harus integer 1..4", 400) from exc
        if raw not in range(1, 5):
            raise StokError("node harus integer 1..4", 400)
        return raw

    def rpc_call(target: int, method: str, *args: Any) -> Dict[str, Any]:
        return panggil_rpc(node_id, target, method, *args)

    def read_failover(target: int, method: str, *args: Any) -> tuple[Any, list[dict[str, Any]], str, int]:
        try:
            result = rpc_call(target, method, *args)
            return result["data"], result["trace"], "RPC", target
        except Exception as primary_error:
            fail_trace = buat_trace(node_id, 4, "FAILOVER", "read_failover_replika",
                                    f"node={target} method={method}; error={primary_error}", "error")
            replica = panggil_rmi(node_id, 4, "Replika", "salinan", target)
            snapshot = replica["data"]
            trace = [*fail_trace, *replica["trace"]]
            if method == "get_stok":
                item = snapshot.get("barang", {}).get(args[0])
                if item is None:
                    raise StokError(f"Barang {args[0]!r} tidak ditemukan di replika", 404)
                value = item
            elif method == "daftar":
                value = [snapshot["barang"][key] for key in sorted(snapshot.get("barang", {}))]
            elif method == "hitung_total":
                goods = snapshot.get("barang", {}).values()
                value = {"total_stok": sum(int(x["stok"]) for x in goods),
                         "jumlah_sku": len(snapshot.get("barang", {}))}
            elif method == "status":
                value = {"node_id": target, "status": "failover_replika",
                         "versi": snapshot.get("versi"), "stale_possible": True}
            else:
                value = snapshot
            return value, trace, "RPC -> RMI (failover ke Replika)", 4

    @app.errorhandler(404)
    def handle_not_found(exc):
        return response(False, None, via="RPC", tujuan=node_id, status=404,
                        error={"message": "Endpoint tidak ditemukan", "code": 404})

    @app.errorhandler(405)
    def handle_method_not_allowed(exc):
        return response(False, None, via="RPC", tujuan=node_id, status=405,
                        error={"message": "Method tidak diizinkan", "code": 405})

    @app.get("/barang")
    def daftar_barang():
        try:
            target = node_arg()
            data, trace, via, actual = read_failover(target, "daftar")
            return response(True, data, via=via, tujuan=actual, trace=trace)
        except Exception as exc:
            return error_response(exc, node_id)

    @app.get("/barang/<path:kode>")
    def get_barang(kode: str):
        try:
            target = node_arg()
            data, trace, via, actual = read_failover(target, "get_stok", kode)
            return response(True, data, via=via, tujuan=actual, trace=trace)
        except Exception as exc:
            return error_response(exc, node_id)

    @app.post("/barang")
    def tambah_barang():
        body = request.get_json(silent=True) or {}
        try:
            target = node_arg()
            if not all(k in body for k in ("kode", "nama", "stok")):
                raise StokError("kode, nama, stok wajib diisi", 400)
            result = rpc_call(target, "tambah_barang", body["kode"], body["nama"], body["stok"],
                              body.get("harga", 50000), body.get("min_stok", 15), body.get("request_id"))
            trace = list(result["trace"])
            trace.append(buat_trace(target, 4, "XML-RPC", "push_replikasi",
                                    f"sumber={target} versi_mutasi sku={body['kode']} (async)"))
            return response(True, result["data"], via="RPC", tujuan=target, trace=trace, status=201)
        except Exception as exc:
            target = locals().get("target", node_id)
            status_code = getattr(exc, "kode_http", getattr(exc, "status", 500))
            if isinstance(exc, RemoteCallError):
                status_code = exc.kode_http
            return error_response(exc, target) if status_code != 409 else response(
                False, None, via="RPC", tujuan=target, status=409,
                error={"message": str(exc), "code": 409})

    @app.put("/barang/<path:kode>/stok")
    def ubah_stok(kode: str):
        body = request.get_json(silent=True) or {}
        try:
            target = node_arg()
            if "jumlah" not in body:
                raise StokError("jumlah wajib diisi", 400)
            result = rpc_call(target, "ubah_stok", kode, body["jumlah"], body.get("tipe", "kasir"),
                              body.get("request_id"))
            trace = list(result["trace"])
            trace.append(buat_trace(target, 4, "XML-RPC", "push_replikasi",
                                    f"sumber={target} sku={kode} (async)"))
            return response(True, result["data"], via="RPC", tujuan=target, trace=trace)
        except Exception as exc:
            return error_response(exc, locals().get("target", node_id))

    @app.delete("/barang/<path:kode>")
    def hapus_barang(kode: str):
        try:
            target = node_arg()
            result = rpc_call(target, "hapus_barang", kode, request.args.get("request_id"))
            trace = list(result["trace"])
            trace.append(buat_trace(target, 4, "XML-RPC", "push_replikasi",
                                    f"sumber={target} sku={kode} (async)"))
            return response(True, result["data"], via="RPC", tujuan=target, trace=trace)
        except Exception as exc:
            return error_response(exc, locals().get("target", node_id))

    @app.get("/total")
    def total():
        try:
            target = node_arg()
            data, trace, via, actual = read_failover(target, "hitung_total")
            return response(True, data, via=via, tujuan=actual, trace=trace)
        except Exception as exc:
            return error_response(exc, locals().get("target", node_id))

    @app.post("/transfer")
    def transfer():
        body = request.get_json(silent=True) or {}
        source = body.get("dari", node_id)
        try:
            src, dst = body.get("dari"), body.get("ke")
            source = src
            if src not in (2, 3) or dst not in (2, 3) or src == dst:
                raise StokError("dari dan ke harus cabang berbeda (2/3)", 400)
            if not all(k in body for k in ("kode", "jumlah")):
                raise StokError("kode dan jumlah wajib diisi", 400)
            result = panggil_rmi(node_id, src, "Cabang", "transfer_stok", body["kode"],
                                 body["jumlah"], dst, body.get("delay", 0.0),
                                 body.get("simulasi_gagal", False))
            transfer_result = result["data"]
            operation_trace = list(result.get("trace", []))
            if isinstance(transfer_result, dict) and isinstance(transfer_result.get("trace"), list):
                for item in transfer_result["trace"]:
                    if item not in operation_trace:
                        operation_trace.append(item)
            if isinstance(transfer_result, dict) and transfer_result.get("ok") is False:
                return response(False, None, via="RMI", tujuan=src,
                                trace=operation_trace,
                                status=int(transfer_result.get("kode_http", 502)),
                                error={"message": transfer_result.get("error", "Transfer gagal"),
                                       "code": int(transfer_result.get("kode_http", 502))})
            return response(True, transfer_result, via="RMI", tujuan=src,
                            trace=operation_trace)
        except Exception as exc:
            trace = getattr(exc, "trace", [])
            return error_response(exc, source if isinstance(source, int) and source in range(1, 5) else node_id,
                                  via="RMI", trace=trace)

    @app.get("/lock")
    def lock_status():
        try:
            result = panggil_lockmanager_dengan_failover(node_id, "status")
            via = "RMI" if result["lockmanager_node"] == 1 else "RPC -> RMI (failover ke Replika)"
            return response(True, result["data"], via=via, tujuan=result["lockmanager_node"],
                            trace=result["trace"])
        except Exception as exc:
            return error_response(exc, 1, via="RMI")

    @app.get("/cabang/<int:n>/info")
    def cabang_info(n: int):
        if n not in (2, 3):
            return error_response(StokError("n harus 2 atau 3", 400), n, via="RMI")
        try:
            result = panggil_rmi(node_id, n, "Cabang", "info")
            return response(True, result["data"], via="RMI", tujuan=n, trace=result["trace"])
        except Exception as exc:
            return error_response(exc, n, via="RMI")

    @app.get("/replika")
    def replika_status():
        try:
            result = panggil_rmi(node_id, 4, "Replika", "status")
            return response(True, result["data"], via="RMI", tujuan=4, trace=result["trace"])
        except Exception as exc:
            return error_response(exc, 4, via="RMI")

    @app.get("/replika/<int:sumber>")
    def replika_salinan(sumber: int):
        try:
            result = panggil_rmi(node_id, 4, "Replika", "salinan", sumber)
            data = result["data"]
            if request.args.get("include_barang", "false").lower() not in {"1", "true", "yes"}:
                data = {k: v for k, v in data.items() if k != "barang"}
            return response(True, data, via="RMI", tujuan=4, trace=result["trace"])
        except Exception as exc:
            return error_response(exc, 4, via="RMI")

    @app.post("/replika/sinkron_ulang")
    def sinkron_ulang():
        try:
            result = panggil_rmi(node_id, 4, "Replika", "sinkron_ulang")
            return response(True, result["data"], via="RMI", tujuan=4, trace=result["trace"])
        except Exception as exc:
            return error_response(exc, 4, via="RMI")

    @app.get("/status")
    def status():
        monitor: Optional[HeartbeatMonitor] = app.config.get("HEARTBEAT_MONITOR")
        heartbeat_data = monitor.snapshot_status() if monitor else {}
        if monitor:
            heartbeat_data = {n: heartbeat_data.get(n, {"status": "unknown", "latensi_ms": None})
                              for n in range(1, 5)}
        else:
            heartbeat_data = {n: {"status": "unknown", "last_success": None,
                                  "last_attempt": None, "latensi_ms": None,
                                  "gagal_berturut": 0} for n in range(1, 5)}
            def probe(node: int) -> None:
                started = time.monotonic()
                try:
                    rpc_call(node, "ping")
                    heartbeat_data[node].update(status="hidup", latensi_ms=round((time.monotonic()-started)*1000, 3))
                except Exception as exc:
                    heartbeat_data[node].update(status="mati", error=str(exc))
            threads = [threading.Thread(target=probe, args=(n,), daemon=True) for n in range(1, 5)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3.5)
        versions: Dict[str, Any] = {}
        traces = []
        for n in (1, 2, 3):
            try:
                result = rpc_call(n, n, "versi")
                versions[str(n)] = result["data"].get("versi_global")
                traces.extend(result["trace"])
            except Exception:
                versions[str(n)] = None
        try:
            rep = panggil_rmi(node_id, 4, "Replika", "status")
            versions["replika"] = rep["data"].get("replikasi", {})
            traces.extend(rep["trace"])
        except Exception:
            versions["replika"] = None
        tree = avtree.query({})
        return response(True, {"node_id": node_id, "heartbeat": heartbeat_data,
                               "versi_global": versions, "avtree": tree},
                        via="RMI", tujuan=node_id, trace=traces)

    @app.get("/ping/<int:n>")
    def ping(n: int):
        if n not in range(1, 5):
            return error_response(StokError("n harus 1..4", 400), n)
        started = time.monotonic()
        try:
            result = rpc_call(n, "ping")
            data = result["data"]
            data["latensi_ms"] = round((time.monotonic() - started) * 1000, 3)
            return response(True, data, via="RPC", tujuan=n, trace=result["trace"])
        except Exception as exc:
            return error_response(exc, n)

    @app.post("/admin/delay/<int:n>")
    def set_delay(n: int):
        if n not in range(1, 5):
            return error_response(StokError("n harus 1..4", 400), n)
        body = request.get_json(silent=True) or {}
        try:
            result = rpc_call(n, "atur_delay", body.get("detik"))
            return response(True, result["data"], via="RPC", tujuan=n, trace=result["trace"])
        except Exception as exc:
            return error_response(exc, n)

    @app.post("/admin/seed")
    def seed():
        body = request.get_json(silent=True) or {}
        data = body.get("barang")
        traces = []
        result_by_node = {}
        active_nodes = []
        try:
            active_nodes = [int(item["node_id"]) for item in avtree.cabang_aktif()]
        except Exception:
            active_nodes = [1, 2, 3]
        active_nodes = sorted(set([1, *[n for n in active_nodes if n in (2, 3)]]))
        for n in active_nodes:
            try:
                result = rpc_call(n, "seed_data_awal", data)
                result_by_node[str(n)] = {"status": "ok", "data": result["data"]}
                traces.extend(result["trace"])
                traces.append(buat_trace(n, 4, "XML-RPC", "push_replikasi", "seed async"))
            except Exception as exc:
                result_by_node[str(n)] = {"status": "gagal", "error": str(exc)}
        try:
            sync = panggil_rmi(node_id, 4, "Replika", "sinkron_ulang")
            traces.extend(sync["trace"])
            sync_data = sync["data"]
            result_by_node["replika"] = sync_data
            failed_sync = [n for n, item in sync_data.get("hasil", {}).items()
                           if item.get("status") == "gagal"]
            if failed_sync:
                result_by_node["replika"] = {"status": "gagal", "data": sync_data,
                                             "nodes": failed_sync}
        except Exception as exc:
            result_by_node["replika"] = {"status": "gagal", "error": str(exc)}
        failures = [k for k, value in result_by_node.items()
                    if isinstance(value, dict) and value.get("status") == "gagal"]
        return response(not failures, result_by_node, via="RPC -> RMI (failover ke Replika)",
                        tujuan=4, trace=traces, status=207 if failures else 200,
                        error={"message": "Sebagian proses seed gagal", "nodes": failures} if failures else None)

    @app.get("/health")
    def health():
        return response(True, {"node_id": node_id, "status": "hidup"}, via="RPC", tujuan=node_id)

    @app.get("/dht/lookup/<path:kode>")
    def dht_lookup(kode: str):
        try:
            lookup = ChordNode(node_id).cari_sku(kode, start_node=node_id)
            owner = lookup["node_pemilik"]
            try:
                item = rpc_call(owner, owner, "get_stok", kode)
                data_item = item["data"]
                trace = [*lookup["trace"], *item["trace"]]
            except Exception:
                replica = panggil_rmi(node_id, 4, "Replika", "salinan", owner)
                data_item = replica["data"].get("barang", {}).get(kode)
                trace = [*lookup["trace"], buat_trace(owner, 4, "FAILOVER", "dht_read_failover",
                                                       f"kode={kode}", "error"), *replica["trace"]]
            return response(True, {"node_pemilik": owner, "finger_hops": lookup["hops"],
                                   "barang": data_item, "key_id": lookup["key_id"]},
                            via="CHORD-DHT", tujuan=owner, trace=trace)
        except Exception as exc:
            return error_response(exc, node_id, via="CHORD-DHT")

    @app.post("/analytics/mpi")
    def analytics_mpi():
        body = request.get_json(silent=True) or {}
        mpiexec = shutil.which("mpiexec") or shutil.which("mpirun")
        command = ([mpiexec, "-n", "4", sys.executable, "-m", "src.analytics.mpi_analytics"]
                   if mpiexec else [sys.executable, "-m", "src.analytics.mpi_analytics", "--simulate"])
        try:
            proc = subprocess.run(command, cwd=str(PROJECT_ROOT), text=True,
                                  capture_output=True, timeout=120, check=False,
                                  env={**os.environ, "PYTHONUNBUFFERED": "1"})
            if proc.returncode != 0:
                # MPI executable exists but unusable/dependency missing: automatic fallback.
                fallback_cmd = [sys.executable, "-m", "src.analytics.mpi_analytics", "--simulate"]
                fallback_proc = subprocess.run(fallback_cmd, cwd=str(PROJECT_ROOT), text=True,
                                               capture_output=True, timeout=60, check=False)
                if fallback_proc.returncode != 0:
                    raise RuntimeError(fallback_proc.stderr or proc.stderr or "MPI gagal")
                output = fallback_proc.stdout
                mode = "fallback_simulasi"
            else:
                output = proc.stdout
                mode = "mpiexec" if mpiexec else "fallback_simulasi"
            # mpiexec may produce diagnostics on stderr; parse final nonempty stdout JSON line.
            parsed = json.loads([line for line in output.splitlines() if line.strip()][-1])
            if "ok" in parsed and parsed["ok"] is False:
                raise RuntimeError(parsed.get("error", "MPI gagal"))
            parsed["mode"] = mode
            traces = [buat_trace(rank + 1, ((rank + 1) % 4) + 1, "MPI", "audit_parallel",
                                 f"rank={rank} node={rank + 1} sku={len(parsed.get('sku', []))}")
                      for rank in range(4)]
            return response(True, parsed, via="MPI", tujuan=node_id, trace=traces)
        except Exception as exc:
            return error_response(exc, node_id, via="MPI")

    return app


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="REST API node DistriPOS")
    parser.add_argument("--node", type=int, default=1, choices=(1, 2, 3, 4))
    args = parser.parse_args()
    create_app(args.node).run(host="0.0.0.0", port=8000 + args.node, threaded=True)


app = create_app(int(os.getenv("NODE_ID", "1")))

if __name__ == "__main__":
    main()
