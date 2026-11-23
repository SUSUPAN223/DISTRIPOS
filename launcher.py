"""Process launcher klaster dan static web dashboard."""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

ROOT = Path(__file__).resolve().parent
DASHBOARD = ROOT / "dashboard"
app = Flask("distripos-launcher", static_folder=None)
CORS(app)
_lock = threading.RLock()
_processes: Dict[int, subprocess.Popen[Any]] = {}


def _poll(node: int) -> Optional[int]:
    process = _processes.get(node)
    if process is None:
        return None
    code = process.poll()
    if code is not None:
        _processes.pop(node, None)
    return code


@app.get("/")
def index():
    return send_from_directory(DASHBOARD, "index.html")


@app.get("/<path:filename>")
def static_file(filename: str):
    if Path(filename).name not in {"app.js", "style.css", "favicon.ico"}:
        return jsonify({"ok": False, "error": "not found"}), 404
    return send_from_directory(DASHBOARD, filename)


@app.get("/launcher/status")
def launcher_status():
    with _lock:
        result = {}
        for node in range(1, 5):
            code = _poll(node)
            process = _processes.get(node)
            result[str(node)] = {"node_id": node,
                "state": "running" if process and code is None else "stopped",
                "status": "running" if process and code is None else "stopped",
                "pid": process.pid if process and code is None else None,
                "returncode": code}
        return jsonify({"ok": True, "nodes": result})


def _start_node(node: int) -> tuple[dict[str, Any], int]:
    if node not in range(1, 5):
        return {"ok": False, "error": "node harus 1..4"}, 400
    with _lock:
        code = _poll(node)
        process = _processes.get(node)
        if process and code is None:
            return {"ok": True, "node_id": node, "state": "running", "pid": process.pid,
                    "message": "sudah berjalan"}, 200
        process = subprocess.Popen(
            [sys.executable, str(ROOT / "run_node.py"), "--node", str(node)],
            cwd=str(ROOT), stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0),
            start_new_session=(os.name != "nt"),
        )
        _processes[node] = process
        return {"ok": True, "node_id": node, "state": "starting", "pid": process.pid}, 202


@app.post("/launcher/start/<int:n>")
def start_node(n: int):
    payload, code = _start_node(n)
    return jsonify(payload), code


def _stop_node(node: int) -> tuple[dict[str, Any], int]:
    if node not in range(1, 5):
        return {"ok": False, "error": "node harus 1..4"}, 400
    with _lock:
        code = _poll(node)
        process = _processes.get(node)
        if process is None or code is not None:
            return {"ok": True, "node_id": node, "state": "stopped", "pid": None}, 200
        try:
            if os.name == "nt":
                # Terminate process tree: runner may have started internal threads only,
                # but taskkill /T also cleans descendants if a future child process exists.
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                               capture_output=True, check=False, timeout=10)
            else:
                os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
        except (OSError, ProcessLookupError):
            pass
        _processes.pop(node, None)
        return {"ok": True, "node_id": node, "state": "stopped", "pid": process.pid}, 200


@app.post("/launcher/stop/<int:n>")
def stop_node(n: int):
    payload, code = _stop_node(n)
    return jsonify(payload), code


@app.post("/launcher/start_all")
def start_all():
    results = []
    for node in range(1, 5):
        payload, code = _start_node(node)
        results.append(payload)
        if code == 400:
            return jsonify({"ok": False, "nodes": results}), 400
        time.sleep(0.15)
    return jsonify({"ok": True, "nodes": results}), 202


@app.post("/launcher/stop_all")
def stop_all():
    results = []
    for node in (4, 3, 2, 1):
        payload, _ = _stop_node(node)
        results.append(payload)
    return jsonify({"ok": True, "nodes": results})


def _shutdown_children() -> None:
    for node in (4, 3, 2, 1):
        _stop_node(node)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--auto-start", action="store_true", help="mulai Node 1..4 saat launcher aktif")
    parser.add_argument("--host", default=os.getenv("LAUNCHER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("LAUNCHER_PORT", "8000")))
    args = parser.parse_args(argv)
    if args.auto_start:
        for node in range(1, 5):
            _start_node(node)
            time.sleep(0.15)
    try:
        app.run(host=args.host, port=args.port, threaded=True, use_reloader=False)
    finally:
        _shutdown_children()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
