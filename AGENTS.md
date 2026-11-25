# AGENTS.md

Guidance for automated agents working on DistriPOS.

## Environment & Commands

- **Python Virtualenv**: `.venv/bin/python` (Python >= 3.10). Dependencies in `requirements.txt`.
- **Environment Config**: `.env` (derived from `.env.example`).
- **Syntax / Compilation Check**:
  ```bash
  python -m compileall -q src run_node.py launcher.py cli.py tests
  ```
- **Run All Python Tests**:
  ```bash
  python -m unittest discover tests
  ```
- **Run Single Python Test File / Case**:
  ```bash
  python -m unittest tests/test_audit_flows.py
  python -m unittest tests.test_audit_flows.StorageAuditTests.test_crud_validation_and_atomic_file
  ```
- **Run JS/Node Tests**:
  ```bash
  node tests/dashboard_gateway.test.cjs
  ```
- **Cluster & Process Execution**:
  - All nodes via launcher: `python launcher.py --auto-start` (launcher + UI at `http://localhost:8000`)
  - Standalone single node: `python run_node.py --node <1|2|3|4>`
  - CLI operations: `python cli.py <list|get|add|stock|transfer|dht|status|lock|replica|seed|sync|mpi>`

## Architecture & Ports

Ports are calculated as `BASE_PORT + node_id` (node IDs are strictly `1..4`):
- **Launcher / Dashboard Web UI**: `8000` (`LAUNCHER_PORT`)
- **REST API**: `8001..8004` (`API_BASE_PORT=8000`)
- **XML-RPC**: `5001..5004` (`RPC_BASE_PORT=5000`)
- **Pyro5 RMI**: `6001..6004` (`RMI_BASE_PORT=6000`)

### Node Roles
- **Node 1 (Pusat)**: Primary `LockManager`, central store, orchestrator.
- **Node 2 & 3 (Cabang)**: Branch stores (`Cabang` RMI object, retail transactions).
- **Node 4 (Replika)**: Backup `LockManager` and passive snapshot replica (`Replika` RMI object).
  - **Constraint**: Node 4 rejects direct write/mutation requests with HTTP 400. It is read-only/replica.

## Operational & Implementation Quirks

- **Data Persistence**: Backed by atomic writes to `data/data_node{1,2,3}.json` and `data/replika_node4.json`. Each store maintains `versi_global` incremented on mutations.
- **Failover vs Replay**:
  - Transport errors (`ConnectionError`, `TimeoutError`, `URLError`) trigger failover to another node gateway.
  - HTTP 4xx and 5xx responses (notably `502` from transfer rollback) **MUST NOT** be replayed across gateways. The original error and trace must be preserved.
- **Non-Idempotent Operations**: `{"tambah_barang", "ubah_stok", "hapus_barang", "transfer_stok"}` are excluded from automatic retry on transport timeouts (`src/common/resilience.py`).
- **Pyro5 Advertising**: Pyro encodes host addresses in exported URIs. Binding to `0.0.0.0` will break client lookups; use `RMI_BIND_HOST_NODEn` or routable `HOST_NODEn`.
- **MPI Analytics**: `src/analytics/mpi_analytics.py` detects whether `mpi4py` and system MPI runtimes are present, falling back to sequential multi-rank simulation (`simulate_audit`) if absent.
