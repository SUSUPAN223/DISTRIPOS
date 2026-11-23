"""Logging terstruktur dan pembentuk lineage trace."""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict

_lock = threading.RLock()
_logger = logging.getLogger("distripos")
if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(handler)
_logger.setLevel(logging.INFO)
_logger.propagate = False


def _emit(event: str, **fields: Any) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    with _lock:
        _logger.info(json.dumps(record, ensure_ascii=False, default=str, separators=(",", ":")))


def panggilan(*args: Any, **kwargs: Any) -> None:
    """Catat panggilan stub RPC/RMI.

    Mendukung panggilan positional (protokol, aksi, dari, ke, ...) dan keyword
    supaya dapat digunakan langsung dari stub yang berbeda.
    """
    names = ("protokol", "aksi", "dari", "ke", "status", "durasi_ms", "error")
    fields = dict(zip(names, args))
    fields.update(kwargs)
    _emit("panggilan_remote", **fields)


def status_node(node_id: int, status: str, **fields: Any) -> None:
    _emit("status_node", node_id=node_id, status=status, **fields)


def heartbeat(node_id: int, status: str, gagal_berturut: int = 0, **fields: Any) -> None:
    _emit("heartbeat", node_id=node_id, status=status, gagal_berturut=gagal_berturut, **fields)


def failover(dari: int, ke: int, aksi: str, **fields: Any) -> None:
    _emit("failover", dari=dari, ke=ke, aksi=aksi, **fields)


def mutasi_versi(node_id: int, versi_lama: int, versi_baru: int, aksi: str, **fields: Any) -> None:
    _emit("mutasi_versi_global", node_id=node_id, versi_lama=versi_lama,
          versi_baru=versi_baru, aksi=aksi, **fields)


def buat_trace(
    dari: int,
    ke: int,
    protokol: str,
    aksi: str,
    ringkasan_data: str,
    status: str = "ok",
) -> Dict[str, Any]:
    if isinstance(dari, bool) or not isinstance(dari, int) or dari not in range(1, 5):
        raise ValueError("dari harus integer 1..4")
    if isinstance(ke, bool) or not isinstance(ke, int) or ke not in range(1, 5):
        raise ValueError("ke harus integer 1..4")
    if protokol not in {"XML-RPC", "Pyro5-RMI", "FAILOVER", "ROLLBACK", "CHORD-DHT", "MPI"}:
        raise ValueError("protokol trace tidak dikenal")
    if status not in {"ok", "error"}:
        raise ValueError("status trace harus 'ok' atau 'error'")
    return {
        "dari": dari,
        "ke": ke,
        "protokol": protokol,
        "aksi": str(aksi),
        "ringkasan_data": str(ringkasan_data),
        "status": status,
    }
