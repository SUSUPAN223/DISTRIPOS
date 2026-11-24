"""Retry terkontrol sesuai semantik at-least-once/at-most-once."""
from __future__ import annotations

import socket
import time
from typing import Any, Callable, Optional, TypeVar

from .config import MAX_RETRY, RETRY_BACKOFF, RPC_TIMEOUT, TIDAK_IDEMPOTEN

T = TypeVar("T")

_READ_IDEMPOTENT = {
    "get_stok", "daftar", "hitung_total", "status", "salinan", "ping",
    "terima_replikasi", "ambil_snapshot", "info", "sinkron_ulang",
}


def _timeout_errors() -> tuple[type[BaseException], ...]:
    # ProtocolError is an HTTP response from the peer, not a connection
    # timeout. It must never be retried as if the request were unreceived.
    return (TimeoutError, socket.timeout)


def _connection_errors() -> tuple[type[BaseException], ...]:
    # Hanya kegagalan connect yang diketahui request belum terkirim. Reset
    # koneksi/TransportError umum sengaja tidak dianggap aman untuk mutasi.
    return (ConnectionRefusedError,)


def semantik(method: str, idempoten: Optional[bool] = None) -> str:
    name = method.rsplit(".", 1)[-1]
    if idempoten is True or (idempoten is None and name in _READ_IDEMPOTENT):
        return "at-least-once"
    if idempoten is False or name in TIDAK_IDEMPOTEN:
        return "at-most-once"
    # Mutasi yang belum tercantum diperlakukan konservatif sebagai at-most-once.
    return "at-most-once"


def panggil_dengan_retry(
    fungsi: Callable[..., T],
    *args: Any,
    method: Optional[str] = None,
    idempoten: Optional[bool] = None,
    timeout: float = RPC_TIMEOUT,
    max_retry: int = MAX_RETRY,
    backoff: Optional[list[float]] = None,
    on_retry: Optional[Callable[[int, BaseException], None]] = None,
    **kwargs: Any,
) -> T:
    """Jalankan call jaringan dengan batas waktu dan retry semantik.

    timeout dipasang oleh transport jaringan pemanggil. Argumen timeout di sini
    tersedia untuk callback/proxy yang mendukung keyword timeout; callable biasa
    dipanggil tanpa keyword tambahan.
    """
    if timeout <= 0:
        raise ValueError("timeout harus positif")
    if isinstance(max_retry, bool) or not isinstance(max_retry, int) or max_retry < 1:
        raise ValueError("max_retry harus integer >= 1")
    policy = semantik(method or getattr(fungsi, "__name__", ""), idempoten)
    delays = list(RETRY_BACKOFF if backoff is None else backoff)
    timeouts = _timeout_errors()
    connections = _connection_errors()
    attempts = max_retry if policy == "at-least-once" else max_retry

    for attempt in range(1, attempts + 1):
        error: Optional[BaseException] = None
        retryable = False
        try:
            return fungsi(*args, **kwargs)
        except timeouts as exc:
            error = exc
            # Timeout ambigu untuk mutasi: server mungkin sudah commit.
            if policy == "at-most-once":
                raise
            retryable = True
        except connections as exc:
            error = exc
            retryable = True
        if error is None:
            raise RuntimeError("kesalahan retry tidak teridentifikasi")
        if not retryable or attempt >= attempts:
            raise error
        if policy == "at-most-once" and not isinstance(error, connections):
            raise error
        if on_retry:
            on_retry(attempt, error)
        delay = delays[attempt - 1] if attempt - 1 < len(delays) else (delays[-1] if delays else 0.0)
        if delay > 0:
            time.sleep(delay)
    raise RuntimeError("jalur retry berakhir tanpa hasil")


def retry_jaringan(
    fungsi: Callable[..., T],
    *args: Any,
    method: Optional[str] = None,
    **kwargs: Any,
) -> T:
    """Alias ringkas untuk pemanggil RPC/RMI."""
    return panggil_dengan_retry(fungsi, *args, method=method, **kwargs)

