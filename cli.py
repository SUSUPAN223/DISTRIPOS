"""Interactive and scriptable Windows-friendly CLI for the DistriPOS REST API."""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Optional

from dotenv import load_dotenv

from src.common.config import API_BASE_PORT, HOSTS, PROJECT_ROOT


def _configure_console() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            try:
                reconfigure(encoding="utf-8", errors="backslashreplace")
            except (OSError, ValueError):
                pass


def _network_error(exc: BaseException) -> bool:
    return isinstance(exc, (TimeoutError, ConnectionError, OSError, urllib.error.URLError))


class GatewayError(RuntimeError):
    def __init__(self, message: str, status: int, payload: Any = None):
        super().__init__(message)
        self.status = status
        self.payload = payload


class SmartGateway:
    """Fail over only if transport failed; HTTP error responses are never replayed."""

    def __init__(self, preferred: int = 1, timeout: float = 3.0):
        self.preferred = preferred
        self.timeout = timeout
        self.hosts = dict(HOSTS)
        self.api_base_port = API_BASE_PORT

    def request(self, method: str, path: str, payload: Optional[dict[str, Any]] = None,
                preferred: Optional[int] = None) -> dict[str, Any]:
        preferred = self.preferred if preferred is None else preferred
        order = [preferred, *[node for node in (1, 2, 3, 4) if node != preferred]]
        last_error: Optional[BaseException] = None
        for node in order:
            url = f"http://{self.hosts[node]}:{self.api_base_port + node}{path}"
            data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
            request = urllib.request.Request(url, data=data, method=method,
                headers={"Accept": "application/json", **({"Content-Type": "application/json"} if data else {})})
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    raw = response.read().decode("utf-8")
                    body = json.loads(raw) if raw else {}
                    self.preferred = node
                    return self._present(body, response.status)
            except urllib.error.HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                try:
                    error_payload = json.loads(raw)
                except (ValueError, TypeError):
                    error_payload = {"error": {"message": raw or str(exc)}}
                message = error_payload.get("error", {})
                if isinstance(message, dict):
                    message = message.get("message", str(exc))
                raise GatewayError(str(message), exc.code, error_payload) from exc
            except (TimeoutError, ConnectionError, OSError, urllib.error.URLError) as exc:
                last_error = exc
                continue
        raise GatewayError(f"Semua gateway gagal dihubungi: {last_error}", 503) from last_error

    @staticmethod
    def _present(body: dict[str, Any], status: int) -> dict[str, Any]:
        print(f"HTTP {status} | {'OK' if body.get('ok', True) else 'ERROR'}")
        for item in body.get("trace") or []:
            print("TRACE {dari}->{ke} [{protokol}] {aksi}: {ringkasan_data} ({status})".format(
                dari=item.get("dari", "?"), ke=item.get("ke", "?"),
                protokol=item.get("protokol", "?"), aksi=item.get("aksi", "?"),
                ringkasan_data=item.get("ringkasan_data", ""), status=item.get("status", "ok")))
        if body.get("error"):
            print("ERROR:", body["error"])
            if body.get("ok") is False:
                error = body["error"]
                message = error.get("message", str(error)) if isinstance(error, dict) else str(error)
                raise GatewayError(message, status, body)
        else:
            print(json.dumps(body.get("data"), ensure_ascii=True, indent=2))
        return body


def _int_arg(text: str) -> int:
    return int(text)


def dispatch(gateway: SmartGateway, args: argparse.Namespace) -> dict[str, Any]:
    node = args.node
    if getattr(args, "request_id", None) is None and args.command in {"add", "stock", "delete", "transfer"}:
        args.request_id = str(uuid.uuid4())
    if args.command == "list":
        return gateway.request("GET", f"/barang?node={node}")
    if args.command == "get":
        return gateway.request("GET", f"/barang/{urllib.parse.quote(args.kode, safe='')}?node={node}")
    if args.command == "add":
        return gateway.request("POST", f"/barang?node={node}", {
            "kode": args.kode, "nama": args.nama, "stok": args.stok,
            "harga": args.harga, "min_stok": args.min_stok, "request_id": args.request_id})
    if args.command == "stock":
        return gateway.request("PUT", f"/barang/{urllib.parse.quote(args.kode, safe='')}/stok?node={node}", {
            "jumlah": args.jumlah, "tipe": args.tipe, "request_id": args.request_id})
    if args.command == "delete":
        rid = urllib.parse.quote(args.request_id, safe="") if args.request_id else ""
        return gateway.request("DELETE", f"/barang/{urllib.parse.quote(args.kode, safe='')}?node={node}&request_id={rid}")
    if args.command == "transfer":
        return gateway.request("POST", "/transfer", {"dari": args.dari, "ke": args.ke,
            "kode": args.kode, "jumlah": args.jumlah, "simulasi_gagal": args.simulasi_gagal,
            "request_id": args.request_id}, preferred=args.dari)
    if args.command == "dht":
        return gateway.request("GET", f"/dht/lookup/{urllib.parse.quote(args.kode, safe='')}")
    if args.command == "mpi":
        return gateway.request("POST", "/analytics/mpi", {})
    if args.command == "status":
        return gateway.request("GET", "/status")
    if args.command == "lock":
        return gateway.request("GET", "/lock")
    if args.command == "replica":
        return gateway.request("GET", "/replika")
    if args.command == "sync":
        return gateway.request("POST", "/replika/sinkron_ulang", {})
    if args.command == "seed":
        return gateway.request("POST", "/admin/seed", {})
    if args.command == "delay":
        return gateway.request("POST", f"/admin/delay/{args.target}", {"detik": args.seconds})
    raise ValueError(f"Perintah tidak dikenal: {args.command}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CLI inventaris terdistribusi DistriPOS")
    subs = parser.add_subparsers(dest="command")
    subs.add_parser("menu", help="buka menu interaktif")
    subs.add_parser("list", help="daftar SKU").add_argument("--node", type=int, default=1)
    get = subs.add_parser("get", help="baca SKU"); get.add_argument("kode"); get.add_argument("--node", type=int, default=1)
    add = subs.add_parser("add", help="tambah SKU"); add.add_argument("kode"); add.add_argument("nama"); add.add_argument("stok", type=_int_arg); add.add_argument("--harga", type=_int_arg, default=50000); add.add_argument("--min-stok", type=_int_arg, default=15); add.add_argument("--node", type=int, default=1); add.add_argument("--request-id", default=None)
    stock = subs.add_parser("stock", help="mutasi stok"); stock.add_argument("kode"); stock.add_argument("jumlah", type=_int_arg); stock.add_argument("--tipe", default="kasir"); stock.add_argument("--node", type=int, default=1); stock.add_argument("--request-id", default=None)
    delete = subs.add_parser("delete", help="hapus SKU"); delete.add_argument("kode"); delete.add_argument("--node", type=int, default=1); delete.add_argument("--request-id", default=None)
    transfer = subs.add_parser("transfer", help="transfer node cabang 2/3"); transfer.add_argument("dari", type=int, choices=(2, 3)); transfer.add_argument("ke", type=int, choices=(2, 3)); transfer.add_argument("kode"); transfer.add_argument("jumlah", type=_int_arg); transfer.add_argument("--simulasi-gagal", action="store_true"); transfer.add_argument("--request-id", default=None)
    dht = subs.add_parser("dht", help="lookup Chord DHT"); dht.add_argument("kode")
    for name, help_text in (("mpi", "audit MPI"), ("status", "status heartbeat dan versi"), ("lock", "status lock manager"), ("replica", "status replika"), ("sync", "sinkron ulang replika"), ("seed", "seed data awal")):
        subs.add_parser(name, help=help_text)
    delay = subs.add_parser("delay", help="atur delay node (0 untuk reset)"); delay.add_argument("target", type=int, choices=(1, 2, 3, 4)); delay.add_argument("seconds", type=float)
    return parser


MENU = """\n=== DistriPOS | Menu Operasional ===
1) Daftar barang             2) Cari SKU
3) Tambah SKU                4) Mutasi stok kasir
5) Hapus SKU                 6) Transfer antar cabang
7) Lookup Chord DHT          8) Audit MPI
9) Status heartbeat          10) Status lock/replika
11) Seed data awal           12) Sinkron ulang replika
13) Injeksi/reset delay       0) Keluar
"""


def _prompt_int(label: str, default: Optional[int] = None) -> int:
    suffix = f" [{default}]" if default is not None else ""
    value = input(f"{label}{suffix}: ").strip()
    return default if not value and default is not None else int(value)


def interactive(gateway: SmartGateway) -> int:
    _configure_console()
    while True:
        print(MENU)
        choice = input("Pilih operasi: ").strip()
        if choice == "0":
            return 0
        try:
            if choice == "1": args = argparse.Namespace(command="list", node=_prompt_int("Node", 1))
            elif choice == "2": args = argparse.Namespace(command="get", kode=input("Kode SKU: ").strip(), node=_prompt_int("Node", 1))
            elif choice == "3":
                args = argparse.Namespace(command="add", kode=input("Kode SKU: ").strip(), nama=input("Nama: ").strip(), stok=_prompt_int("Stok", 0), harga=_prompt_int("Harga", 50000), min_stok=_prompt_int("Stok minimum", 15), node=_prompt_int("Node", 1), request_id=None)
            elif choice == "4":
                args = argparse.Namespace(command="stock", kode=input("Kode SKU: ").strip(), jumlah=_prompt_int("Perubahan stok (negatif untuk jual)"), tipe="kasir", node=_prompt_int("Node", 2), request_id=None)
            elif choice == "5": args = argparse.Namespace(command="delete", kode=input("Kode SKU: ").strip(), node=_prompt_int("Node", 1), request_id=None)
            elif choice == "6":
                source = _prompt_int("Node asal (2/3)", 2); target = _prompt_int("Node tujuan (2/3)", 3)
                fail = input("Simulasikan kegagalan/rollback? (y/N): ").strip().lower() == "y"
                args = argparse.Namespace(command="transfer", dari=source, ke=target, kode=input("Kode SKU: ").strip(), jumlah=_prompt_int("Jumlah transfer"), simulasi_gagal=fail, request_id=None)
            elif choice == "7": args = argparse.Namespace(command="dht", kode=input("Kode SKU: ").strip())
            elif choice == "8": args = argparse.Namespace(command="mpi")
            elif choice == "9": args = argparse.Namespace(command="status")
            elif choice == "10":
                dispatch(gateway, argparse.Namespace(command="lock")); args = argparse.Namespace(command="replica")
            elif choice == "11": args = argparse.Namespace(command="seed")
            elif choice == "12": args = argparse.Namespace(command="sync")
            elif choice == "13": args = argparse.Namespace(command="delay", target=_prompt_int("Node target", 2), seconds=float(input("Delay detik (0 reset): ").strip() or "0"))
            else:
                print("Pilihan tidak dikenal."); continue
            dispatch(gateway, args)
        except (GatewayError, ValueError, OSError) as exc:
            print(f"Operasi gagal: {exc}")
        input("Tekan Enter untuk kembali ke menu...")


def main(argv: Optional[list[str]] = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    _configure_console()
    parser = _parser()
    args = parser.parse_args(argv)
    gateway = SmartGateway(timeout=float(os.getenv("CLI_HTTP_TIMEOUT", "3")))
    if args.command in (None, "menu"):
        return interactive(gateway)
    try:
        dispatch(gateway, args)
        return 0
    except GatewayError as exc:
        print(f"HTTP {exc.status}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())