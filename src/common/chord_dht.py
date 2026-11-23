"""Flat naming dan simulasi routing Chord DHT untuk empat node DistriPOS."""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional, Tuple

from .config import CHORD_M, HOSTS, port_api
from .logger import buat_trace


M = CHORD_M
RING_SIZE = 1 << M


def hash_8bit(key: str) -> int:
    if not isinstance(key, str):
        raise TypeError("key DHT harus berupa string")
    return int(hashlib.sha1(key.encode("utf-8")).hexdigest(), 16) % RING_SIZE


def _node_key_id(node_id: int) -> int:
    return hash_8bit(f"node:{node_id}")


def _build_node_ids() -> Dict[int, int]:
    """Node ID SHA-1 deterministik; collision diselesaikan linear probe modulo 256."""
    result: Dict[int, int] = {}
    occupied = set()
    for node_id in range(1, 5):
        candidate = _node_key_id(node_id)
        while candidate in occupied:
            candidate = (candidate + 1) % RING_SIZE
        result[node_id] = candidate
        occupied.add(candidate)
    return result


NODE_IDS: Dict[int, int] = _build_node_ids()
NODE_BY_ID: Dict[int, int] = {ring_id: node for node, ring_id in NODE_IDS.items()}


def _in_interval(value: int, start: int, end: int, *, include_end: bool = False) -> bool:
    """Apakah value berada dalam interval searah jarum jam (start, end] atau (start,end)."""
    value %= RING_SIZE
    start %= RING_SIZE
    end %= RING_SIZE
    if start == end:
        return value != start if not include_end else True
    if start < end:
        return start < value <= end if include_end else start < value < end
    return value > start or value <= end if include_end else value > start or value < end


class ChordNode:
    """Representasi node Chord dengan finger table 8 entri dan routing lookup."""

    def __init__(self, node_id: int, node_ids: Optional[Dict[int, int]] = None) -> None:
        if isinstance(node_id, bool) or not isinstance(node_id, int) or node_id not in range(1, 5):
            raise ValueError("node_id harus integer 1..4")
        self.node_id = node_id
        self.node_ids = dict(node_ids or NODE_IDS)
        if set(self.node_ids) != {1, 2, 3, 4} or len(set(self.node_ids.values())) != 4:
            raise ValueError("node_ids harus memetakan empat node ke ID ring unik")
        self.ring_id = self.node_ids[node_id]
        self._ring = sorted((rid, nid) for nid, rid in self.node_ids.items())
        self.finger_table: List[int] = []
        self.finger_table_nodes: List[int] = []
        self._build_finger_table()

    def _successor_ring_id(self, identifier: int) -> int:
        identifier %= RING_SIZE
        for ring_id, _ in self._ring:
            if ring_id >= identifier:
                return ring_id
        return self._ring[0][0]

    def _build_finger_table(self) -> None:
        self.finger_table = []
        self.finger_table_nodes = []
        for i in range(1, M + 1):
            start = (self.ring_id + (1 << (i - 1))) % RING_SIZE
            successor = self._successor_ring_id(start)
            self.finger_table.append(successor)
            self.finger_table_nodes.append(NODE_BY_ID.get(successor, self._node_for_ring_id(successor)))

    def _node_for_ring_id(self, ring_id: int) -> int:
        for node, rid in self.node_ids.items():
            if rid == ring_id:
                return node
        raise ValueError(f"Tidak ada node untuk ring ID {ring_id}")

    def successor(self, identifier: int) -> int:
        """Kembalikan ID aplikasi node yang menjadi successor identifier."""
        return self._node_for_ring_id(self._successor_ring_id(identifier))

    def closest_preceding_node(self, target: int) -> int:
        for finger_ring_id in reversed(self.finger_table):
            if _in_interval(finger_ring_id, self.ring_id, target):
                return self._node_for_ring_id(finger_ring_id)
        return self.node_id

    def route(self, target: int, max_hops: int = 16) -> Tuple[int, List[int]]:
        """Routing overlay menggunakan finger predecessor, bounded agar bebas loop."""
        target %= RING_SIZE
        # Pemilik dihitung dari successor identifier pada ring. Hops dibentuk
        # sebagai jalur finger-table valid yang monoton mendekati key; pada
        # key wrap-around, successor akhir menjadi hop terakhir tanpa loop.
        owner = self.successor(target)
        current = self.node_id
        hops = [current]
        visited = {current}
        for _ in range(min(max_hops, len(self.node_ids))):
            if current == owner:
                return owner, hops
            node = self if current == self.node_id else ChordNode(current, self.node_ids)
            current_ring = self.node_ids[current]
            target_distance = (target - current_ring) % RING_SIZE
            candidates = []
            for ring_id in node.finger_table:
                distance = (ring_id - current_ring) % RING_SIZE
                candidate_node = self._node_for_ring_id(ring_id)
                if 0 < distance < target_distance and candidate_node not in visited:
                    candidates.append((distance, candidate_node))
            if candidates:
                next_node = max(candidates, key=lambda pair: pair[0])[1]
            else:
                next_node = owner
            if next_node in visited:
                break
            current = next_node
            visited.add(current)
            hops.append(current)
        if owner not in visited:
            hops.append(owner)
        return owner, hops

    def cari_sku(self, kode_sku: str, start_node: int = 1) -> Dict[str, Any]:
        if not isinstance(kode_sku, str) or not kode_sku.strip():
            raise ValueError("kode_sku harus string tidak kosong")
        if isinstance(start_node, bool) or not isinstance(start_node, int) or start_node not in range(1, 5):
            raise ValueError("start_node harus integer 1..4")
        key = kode_sku.strip()
        key_id = hash_8bit(key)
        start = ChordNode(start_node, self.node_ids)
        owner, hops = start.route(key_id)
        traces = []
        for src, dst in zip(hops, hops[1:]):
            traces.append(buat_trace(
                src, dst, "CHORD-DHT", "lookup_sku",
                f"kode={key} key_id={key_id} node_id={self.node_ids[dst]}", "ok"))
        if not traces:
            traces.append(buat_trace(
                start_node, owner, "CHORD-DHT", "lookup_sku",
                f"kode={key} key_id={key_id} pemilik={owner} (lokal)", "ok"))
        return {
            "kode": key,
            "key_id": key_id,
            "node_pemilik": owner,
            "node_pemilik_id": self.node_ids[owner],
            "start_node": start_node,
            "hops": hops,
            "trace": traces,
        }


def cari_sku(kode_sku: str, start_node: int = 1) -> Dict[str, Any]:
    """Helper module-level yang menggunakan ring deterministik standar."""
    return ChordNode(start_node).cari_sku(kode_sku, start_node=start_node)
