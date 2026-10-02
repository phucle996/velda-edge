# velda-edge

`velda-edge` is the **Composition Root, Process Supervisor, and Lifecycle Owner** of the Velda Edge Data Plane.

> [!IMPORTANT]
> **Architectural Invariant**: `velda-edge` **does NOT process network traffic or proxy requests**. Its sole purpose is assembling dependencies, bootstrapping the process from Last Known Good (LKG) binary artifacts, managing the local Unix Domain Socket (UDS) IPC channel, and performing atomic, lock-free runtime state swaps.

---

## 1. Ultra-Thin Design & Structure

`velda-edge` is architected as an ultra-thin supervisor composed of 6 bounded modules:

```text
crates/velda-edge/src/
├── main.rs            # Binary entrypoint (CLI args, tracing subscriber, signal hooks)
├── bootstrap.rs       # Cold-start initialization, component assembly, and supervisor loop
├── runtime_profile.rs # Node-local runtime sizing (runtime.json & Hardware Probe)
├── config.rs          # Path options and LKG binary artifact loaders (*.bin)
├── runtime.rs         # In-memory Runtime snapshot wrapped in ArcSwap
├── reload.rs          # Hot-reload orchestration and atomic runtime swapping
└── uds.rs             # Unix Domain Socket (velda.sock) IPC server
```

---

## 2. Process Lifecycles

### Cold-Start Bootstrap (`bootstrap.rs` & `runtime_profile.rs`)
Upon process startup, `velda-edge` operates autonomously without requiring the Go Control Plane to be online:

```text
  1. Resolve Runtime Profile (runtime_profile.rs)
     ├── File Priority: Read <runtime_dir>/runtime.json (if present)
     └── Fallback: Probe Hardware (cgroup RAM/cores) -> Tier -> Write back runtime.json
              ↓
  2. LKG Binary Artifacts (runtime/*.bin)
              ↓
  3. Runtime::load_from_storage() (runtime.rs)
              ↓
  4. Initialize SharedRuntime (ArcSwap<Runtime>)
              ↓
  5. Convert Listeners to IngressBindings
              ↓
  6. TrafficEngine::bind() (velda-transport)
              ↓
  7. Spawn UDS IPC Listener (uds.rs)
              ↓
  8. Serve Ingress Traffic
```

### Hardware Adaptive Sizing (`runtime.json`)
- **Orthogonal Dual-Tier Probing**: Separates compute and memory capacity into distinct tiers to accurately size diverse cloud instance types (e.g. AWS `c7g` compute-optimized vs `r7g` memory-optimized):
  - **`CpuTier`** (7 tiers: `Constrained`, `Small`, `Medium`, `Large`, `XLarge`, `TwoXLarge`, `Ultra`): Detected strictly from hardware via cgroup v2 (`cpu.max`), cgroup v1 (`cpu.cfs_quota_us`), or host parallelism. Dynamically tunes `io_workers` (1 to 64), `reconcile_channel_capacity` (32 to 2,048), and L4 forwarding `copy_buffer_size` (8 KB to 64 KB).
  - **`MemoryTier`** (7 tiers: `Constrained` up to `Ultra`): Detected strictly from hardware via cgroup v2 (`memory.max`), cgroup v1 (`memory.limit_in_bytes`), or `/proc/meminfo`. Sizes concrete `max_active_connections` (10,000 to 4,000,000), TCP backlog (512 to 32,768), TCP buffer sizes (64 KB to 4 MB), UDP/QUIC buffer sizes (256 KB to 16 MB), and DNS/LKG cache capacities.
- **7-Tier Sizing Matrix for Transport**:

| Parameter | Constrained | Small | Medium | Large | XLarge | TwoXLarge | Ultra | Governed By |
|---|---|---|---|---|---|---|---|---|
| `io_workers` | 1 | 2 | 4 | 8 | 16 | 32 | 64 | `CpuTier` |
| `copy_buffer_size` | 8 KB | 16 KB | 16 KB | 32 KB | 32 KB | 64 KB | 64 KB | `CpuTier` |
| `reconcile_channel_capacity` | 32 | 64 | 128 | 256 | 512 | 1,024 | 2,048 | `CpuTier` |
| `max_active_connections` | 10,000 | 50,000 | 200,000 | 500,000 | 1,000,000 | 2,000,000 | 4,000,000 | `MemoryTier` |
| `tcp.backlog` | 512 | 1,024 | 2,048 | 4,096 | 8,192 | 16,384 | 32,768 | `MemoryTier` |
| `tcp.recv_buffer_size` | 64 KB | 128 KB | 256 KB | 512 KB | 1 MB | 2 MB | 4 MB | `MemoryTier` |
| `tcp.send_buffer_size` | 64 KB | 128 KB | 256 KB | 512 KB | 1 MB | 2 MB | 4 MB | `MemoryTier` |
| `udp.recv_buffer_size` | 256 KB | 512 KB | 1 MB | 2 MB | 4 MB | 8 MB | 16 MB | `MemoryTier` |
| `udp.send_buffer_size` | 256 KB | 512 KB | 1 MB | 2 MB | 4 MB | 8 MB | 16 MB | `MemoryTier` |

- **7-Tier Sizing Matrix for TLS**:

| Parameter | Constrained | Small | Medium | Large | XLarge | TwoXLarge | Ultra | Governed By |
|---|---|---|---|---|---|---|---|---|
| `tls.session_cache_capacity` | 1,024 | 2,048 | 8,192 | 16,384 | 32,768 | 65,536 | 131,072 | `MemoryTier` (RAM footprint) |
| `tls.max_early_data_size` | 0 B | 4 KB | 8 KB | 8 KB | 16 KB | 16 KB | 32 KB | `MemoryTier` (0-RTT RAM buffer) |
| `tls.send_tls13_tickets` | 1 | 2 | 4 | 4 | 6 | 8 | 8 | `CpuTier` (Parallelism/Workers) |
| `tls.handshake_timeout_secs` | 10 s | 8 s | 5 s | 5 s | 3 s | 3 s | 2 s | `CpuTier` (Crypto CPU / Slowloris defense) |

- **Operator Priority**: Explicit tuning in `<runtime_dir>/runtime.json` is always strictly honored and deep-merged over detected hardware tiers. No generic global env variables are used.
- **Grouped & Flat Overrides**: Supports clean nested structures (`"tcp": { ... }`, `"udp": { ... }`, `"tls": { ... }`) as well as flat backward-compatible overrides (`"worker_threads"`, `"tcp_backlog"`).
- **Zero-Config Baseline**: On new nodes without configuration, edge automatically probes hardware and generates an optimized baseline `runtime.json`.
- **Read-Only Resilient**: If the filesystem is read-only, write-back failures are logged as warnings while the process boots cleanly in memory.

### Hot-Reload Pipeline (`reload.rs` & `uds.rs`)
When `velda-sync` publishes updated binary artifacts to LKG, it notifies `velda-edge` over UDS without sending raw JSON:

```text
  velda-sync
      │
      │ UDS Notification: { changed_domains: ["listeners"], manifest_rev: 42 }
      ▼
  velda.sock (uds.rs)
      │
      ▼
  apply_reload() (reload.rs)
      │
      ├─► 1. Load updated *.bin from runtime_dir
      ├─► 2. Re-use unchanged domains from current snapshot
      ├─► 3. Validate candidate Runtime (addresses, profiles)
      └─► 4. Atomic Swap: shared_runtime.store(Arc::new(candidate))
```

- **Lock-Free $O(1)$ Read**: Worker tasks load the active `Runtime` snapshot via `shared_runtime.load()` without acquiring Mutex locks.
- **Zero Traffic Interruption**: Existing connections drain naturally on their snapshot generation while new connections immediately read the updated pointer.

---

## 3. Configuration & Environment Variables

| Variable | Default | Description |
|---|---|---|
| `VELDA_STORAGE_DIR` | `/var/lib/velda` | Root directory containing `config/` and `runtime/*.bin` |
| `VELDA_SOCKET_PATH` | `/run/velda/edge.sock` | Unix Domain Socket path for reload signaling |
| `RUST_LOG` | `info` | Logging verbosity filter |

---

## 4. Verification & Testing

```bash
# Code formatting
rtk cargo fmt --check

# Strict Clippy validation
rtk cargo clippy -p velda-edge --all-targets --all-features -- -D warnings

# Unit & E2E integration tests (Cold start + LKG + RuntimeProfile tier adaptation)
rtk cargo test -p velda-edge
```

---

## 5. Performance Benchmarks & Architectural Invariants

`velda-edge` includes 4 dedicated standalone benchmark suites validating hot-path performance, concurrency scalability, adversarial resilience, and zero-memory-leak invariants:

```bash
# 1. Single-Thread Latency & Zero-Allocation Invariant (< 15 ns snapshot read)
rtk cargo bench -p velda-edge --bench single_thread_bench

# 2. Multi-Thread Scalability & Live Hot-Reload under Traffic Storm (1-64 threads)
rtk cargo bench -p velda-edge --bench multi_thread_bench

# 3. Adversarial, Flooding & Corrupted runtime.json Fault-Tolerance Suite
rtk cargo bench -p velda-edge --bench adversarial_bench

# 4. High-Intensity Memory Leak & Generation Drop Audit (10M ops, 5,000 swaps)
rtk cargo bench -p velda-edge --bench memory_leak_bench
```

### Benchmark Summary & Invariants

| Benchmark Suite | Evaluated Workload | Target Architectural Invariant | Status |
|---|---|---|---|
| `single_thread_bench` | `SharedRuntime::load()` (5,000,000 ops) | `< 15.0 ns/op`, **0 heap allocations** | **VERIFIED** |
| `single_thread_bench` | `PipelineTable` lookup (2,000,000 ops) | `< 25.0 ns/op`, **0 heap allocations** | **VERIFIED** |
| `single_thread_bench` | `to_dns_resolver_config()` struct conversion | `< 5.0 ns/op`, **0 heap allocations** | **VERIFIED** |
| `multi_thread_bench` | Concurrency scaling (1 to 64 threads) | Linear multi-core scaling, wait-free reads | **VERIFIED** |
| `multi_thread_bench` | Live reload storm (64 threads, 6.4M ops) | 0 reader errors/panics, atomic pointer swap | **VERIFIED** |
| `adversarial_bench` | Malicious listener ID flood (2,000,000 ops) | Deterministic rejection, **0 heap allocations** | **VERIFIED** |
| `adversarial_bench` | High-frequency reload contention (> 500 swaps/s) | Monotonic revision progression, no torn reads | **VERIFIED** |
| `adversarial_bench` | Hostile `runtime.json` injection (SQLi, overflows) | Resilient fallback to hardware tier, no panics | **VERIFIED** |
| `memory_leak_bench` | Sustained request serving (10,000,000 ops) | **0 B Net Heap Delta**, **0 Leaked Allocs** | **VERIFIED** |
| `memory_leak_bench` | Generation drop audit (5,000 full runtime swaps) | **0 B Net Heap Delta**, all generations freed | **VERIFIED** |
| `memory_leak_bench` | Profile resolution audit (5,000 cycles) | **0 B Net Heap Delta**, zero retention | **VERIFIED** |
