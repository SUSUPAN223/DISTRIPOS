"""Audit inventori MPI empat rank dengan mode simulasi tanpa MPI."""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from src.common.config import PROJECT_ROOT, INT32_MAX, INT32_MIN

RANK_TO_NODE = {0: 1, 1: 2, 2: 3, 3: 4}
DEFAULT_PARAMS = {"harga_acuan": 50000, "rasio_safety_stock": 1.25}


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as stream:
        data = json.load(stream)
    return data if isinstance(data, dict) else {}


def _load_cluster_data(data_dir: Path) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, int]]:
    node_docs = {node: _read_json(data_dir / f"data_node{node}.json") for node in (1, 2, 3)}
    replica_doc = _read_json(data_dir / "replika_node4.json")
    keys = sorted({sku for doc in node_docs.values()
                   for sku in (doc.get("barang", {}) if isinstance(doc.get("barang", {}), dict) else {})})
    national: list[dict[str, Any]] = []
    for sku in keys:
        item_source = next((doc.get("barang", {}).get(sku) for doc in node_docs.values()
                            if sku in doc.get("barang", {})), {})
        sku_branch_stocks = {node: int(node_docs[node].get("barang", {}).get(sku, {}).get("stok", 0))
                             for node in (1, 2, 3)}
        total_stock = sum(sku_branch_stocks.values())
        if total_stock == 0 and isinstance(item_source, dict):
            # Some stores model independent branch inventory rather than
            # disjoint regional inventories; an authoritative copy provides
            # the national fallback only when branch stock is otherwise empty.
            sku_branch_stocks[1] = int(item_source.get("stok", 0))
        branch_stocks = sku_branch_stocks
        national.append({"kode": sku, "nama": item_source.get("nama", sku),
                         "harga": int(item_source.get("harga", 0)),
                         "min_stok": int(item_source.get("min_stok", 0)),
                         "stocks": branch_stocks, "total_stok": sum(branch_stocks.values())})
    replica_versions = {}
    replica_snapshots = replica_doc.get("replikasi", {})
    for node in (1, 2, 3):
        replica_versions[str(node)] = int(replica_snapshots.get(str(node), {}).get("versi", 0))
    return national, replica_snapshots, {str(k): v for k, v in replica_versions.items()}


def _audit_chunk(chunk: list[dict[str, Any]], params: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for item in chunk:
        total = int(item["total_stok"])
        min_stok = int(item["min_stok"])
        critical = total < min_stok * float(params["rasio_safety_stock"])
        unit_price = int(item["harga"] or params["harga_acuan"])
        value = total * unit_price
        price_factor = unit_price / max(1, int(params["harga_acuan"]))
        # Data penjualan tidak tersedia dalam dokumen saat ini; turnover
        # dihitung sebagai total stok / rata-rata stok cabang yang positif.
        positive = [int(v) for v in item["stocks"].values() if int(v) > 0]
        avg = sum(positive) / len(positive) if positive else 0.0
        turnover = total / avg if avg else 0.0
        rows.append({"kode": item["kode"], "total_stok": total, "valuasi": value,
                     "stok_kritis": critical, "skor_kekritisan": round(
                         ((min_stok * float(params["rasio_safety_stock"]) - total) /
                          max(1.0, min_stok * float(params["rasio_safety_stock"]))) * price_factor, 6)
                         if critical else 0.0,
                     "inventory_turnover": round(turnover, 6)})
    return {"rows": rows, "total_stok": sum(x["total_stok"] for x in rows),
            "total_valuasi": sum(x["valuasi"] for x in rows),
            "jumlah_stok_kritis": sum(1 for x in rows if x["stok_kritis"])}


def _partition(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i::size] for i in range(size)]


def simulate_audit(data_dir: Optional[Path] = None, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    data_dir = data_dir or PROJECT_ROOT / "data"
    params = dict(DEFAULT_PARAMS if params is None else params)
    national, replicas, replica_versions = _load_cluster_data(data_dir)
    node_docs = {node: _read_json(data_dir / f"data_node{node}.json") for node in (1, 2, 3)}
    chunks = _partition(national, 4)
    rank_results = [_audit_chunk(chunk, params) for chunk in chunks]
    by_sku: Dict[str, dict[str, Any]] = {}
    for result in rank_results:
        for row in result["rows"]:
            by_sku[row["kode"]] = row
    discrepancy = []
    replica_skew = {"1": 0, "2": 0, "3": 0}
    for node in (1, 2, 3):
        snapshot = replicas.get(str(node), {})
        replica_goods = snapshot.get("barang", {}) if isinstance(snapshot, dict) else {}
        keys = set(replica_goods)
        # Vector per SKU/node: local stock minus replicated stock.
        for sku in sorted(set(by_sku) | keys):
            local = int(node_docs[node].get("barang", {}).get(sku, {}).get("stok", 0))
            repl = int(replica_goods.get(sku, {}).get("stok", 0))
            delta = local - repl
            if delta:
                discrepancy.append({"sumber": node, "kode": sku, "stok_lokal": local,
                                    "stok_replika": repl, "delta": delta})
                replica_skew[str(node)] += abs(delta)
    totals = [r["total_stok"] for r in rank_results]
    values = [r["total_valuasi"] for r in rank_results]
    return {
        "mode": "fallback_simulasi",
        "jumlah_rank": 4,
        "parameter": params,
        "sku": [by_sku[key] for key in sorted(by_sku)],
        "agregat": {"total_stok": sum(totals), "total_valuasi": sum(values),
                    "jumlah_stok_kritis": sum(r["jumlah_stok_kritis"] for r in rank_results),
                    "min_stok_per_rank": min(totals, default=0),
                    "max_stok_per_rank": max(totals, default=0)},
        "discrepancy_vector": discrepancy,
        "versi_replika": replica_versions,
        "rank_summary": [{"rank": i, "node_id": RANK_TO_NODE[i], **rank_results[i]}
                         for i in range(4)],
    }


def _run_mpi() -> None:
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    if size != 4:
        if rank == 0:
            print(json.dumps({"ok": False, "error": "MPI harus dijalankan dengan tepat 4 rank"}))
        return

    # Bcast: rank 0 menyiapkan parameter audit, semua rank menerima parameter.
    params = dict(DEFAULT_PARAMS) if rank == 0 else None
    params = comm.bcast(params, root=0)
    data_dir = PROJECT_ROOT / "data"
    if rank == 0:
        national, replica_snapshots, replica_versions = _load_cluster_data(data_dir)
        partitions = _partition(national, size)
    else:
        national, replica_snapshots, replica_versions, partitions = None, None, None, None
    chunk = comm.scatter(partitions, root=0)

    # Komputasi lokal paralel pada tiap rank.
    local_result = _audit_chunk(chunk, params)

    # Isend/Irecv non-blocking: pertukaran log mutasi antar Rank 1 dan 2.
    local_node = RANK_TO_NODE[rank]
    log_doc = _read_json(data_dir / (f"data_node{local_node}.json" if rank in (0, 1, 2)
                                      else "replika_node4.json"))
    local_log = log_doc.get("riwayat_mutasi", [])
    if rank == 3:
        replica = log_doc.get("replikasi", {})
        local_log = [{"sumber": n, "versi": v.get("versi", 0)} for n, v in replica.items()]
    next_rank = (rank + 1) % size
    prev_rank = (rank - 1) % size
    recv_req = comm.irecv(source=prev_rank, tag=710)
    send_req = comm.isend(local_log, dest=next_rank, tag=710)
    received_log = recv_req.wait()
    MPI.Request.Waitall([send_req])
    exchange_summary = {"from_rank": prev_rank, "log_entries": len(received_log or []),
                        "sent_entries": len(local_log)}

    gathered = comm.gather(local_result, root=0)
    total_stocks = comm.reduce(local_result["total_stok"], op=MPI.SUM, root=0)
    min_stock = comm.reduce(local_result["total_stok"], op=MPI.MIN, root=0)
    max_stock = comm.reduce(local_result["total_stok"], op=MPI.MAX, root=0)
    total_value = comm.reduce(local_result["total_valuasi"], op=MPI.SUM, root=0)
    critical_count = comm.reduce(local_result["jumlah_stok_kritis"], op=MPI.SUM, root=0)
    exchanges = comm.gather(exchange_summary, root=0)

    if rank == 0:
        national_rows = [row for result in gathered for row in result["rows"]]
        # Build rank-0 discrepancy vector from snapshots loaded by rank 0.
        _, replica_snapshots, replica_versions = _load_cluster_data(data_dir)
        local_docs = {n: _read_json(data_dir / f"data_node{n}.json") for n in (1, 2, 3)}
        discrepancies = []
        for node in (1, 2, 3):
            repl = replica_snapshots.get(str(node), {}).get("barang", {})
            local = local_docs[node].get("barang", {})
            for sku in sorted(set(local) | set(repl)):
                delta = int(local.get(sku, {}).get("stok", 0)) - int(repl.get(sku, {}).get("stok", 0))
                if delta:
                    discrepancies.append({"sumber": node, "kode": sku,
                                          "stok_lokal": int(local.get(sku, {}).get("stok", 0)),
                                          "stok_replika": int(repl.get(sku, {}).get("stok", 0)),
                                          "delta": delta})
        result = {"mode": "mpiexec", "jumlah_rank": size, "parameter": params,
                  "sku": sorted(national_rows, key=lambda x: x["kode"]),
                  "agregat": {"total_stok": total_stocks, "total_valuasi": total_value,
                              "jumlah_stok_kritis": critical_count,
                              "min_stok_per_rank": min_stock, "max_stok_per_rank": max_stock},
                  "discrepancy_vector": discrepancies, "versi_replika": replica_versions,
                  "log_exchange": exchanges}
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--simulate", action="store_true", help="jalankan simulasi lokal empat rank")
    parser.add_argument("--data-dir", type=Path, default=PROJECT_ROOT / "data")
    args = parser.parse_args(argv)
    if args.simulate:
        print(json.dumps(simulate_audit(args.data_dir), ensure_ascii=False, separators=(",", ":")))
        return 0
    try:
        _run_mpi()
        return 0
    except ImportError as exc:
        print(json.dumps({"ok": False, "error": f"mpi4py tidak tersedia: {exc}"}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
