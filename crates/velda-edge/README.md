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
     └── Fallback: Probe Hardware (cgroup RAM/cores) -> Continuous Sizing -> Write back runtime.json
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
- **Zero-Quantization Continuous Hardware Sizing**: Instead of arbitrary discrete tiers (which cause cliff effects where 7.9 GB RAM receives half the buffer of 8.1 GB RAM), Velda Edge derives all buffer, queue, window, and concurrency capacities through continuous mathematical formulas computed once at cold-start bootstrap via [`HardwareSizing`]:
  - **CPU Parallelism**: Probed strictly from hardware via cgroup v2 (`cpu.max`), cgroup v1 (`cpu.cfs_quota_us`), or host parallelism. Dynamically tunes `io_workers` (1 to 64), `concurrency_shards` ($[8, 1024]$), and L4 forwarding `copy_buffer_size` (16 KB to 64 KB).
  - **Memory Capacity**: Probed strictly from hardware via cgroup v2 (`memory.max`), cgroup v1 (`memory.limit_in_bytes`), or `/proc/meminfo`. Sizes continuous `max_active_connections` (10,000 to 2,000,000), TCP backlog (1,024 to 32,768), TCP buffer sizes (64 KB to 2 MB), UDP buffer sizes (1 MB to 32 MB), and DNS/LKG cache capacities.
- **Continuous Formulation Invariants**:
  - `concurrency_shards`: `(worker_threads * 2).next_power_of_two().clamp(8, 1024)`.
  - `tcp_recv_buffer_size`: `(ram_bytes / 8192).clamp(64 KB, 2 MB)`.
  - `optimal_max_idle_per_key`: `((min(target_total, ram_safe_cap) / shards) * 2).clamp(128, 4096)`.
- **Operator Priority**: Explicit tuning in `<runtime_dir>/runtime.json` is always strictly honored and deep-merged over detected hardware sizing defaults. No generic global env variables are used.
- **Grouped Overrides**: Supports clean nested structures (`"tcp": { ... }`, `"udp": { ... }`, `"tls": { ... }`) for operator overrides.
- **Zero-Config Baseline**: On new nodes without configuration, edge automatically probes hardware and generates an optimized baseline `runtime.json`.
- **Read-Only Resilient**: If the filesystem is read-only, write-back failures are logged as warnings while the process boots cleanly in memory.

### Hot-Reload Pipeline (`reload.rs` & `lifecycle/ipc.rs`)
When `velda-sync` publishes updated binary artifacts to LKG, it notifies `velda-edge` over UDS without sending raw JSON:

```text
  velda-sync
      │
      │ UDS Notification: { changed_domains: ["listeners"], manifest_rev: 42 }
      ▼
  velda.sock (lifecycle/ipc.rs)
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

# Unit & E2E integration tests (Cold start + LKG + RuntimeProfile continuous sizing adaptation)
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
| `adversarial_bench` | Hostile `runtime.json` injection (SQLi, overflows) | Resilient fallback to continuous hardware defaults, no panics | **VERIFIED** |
| `memory_leak_bench` | Sustained request serving (10,000,000 ops) | **0 B Net Heap Delta**, **0 Leaked Allocs** | **VERIFIED** |
| `memory_leak_bench` | Generation drop audit (5,000 full runtime swaps) | **0 B Net Heap Delta**, all generations freed | **VERIFIED** |
| `memory_leak_bench` | Profile resolution audit (5,000 cycles) | **0 B Net Heap Delta**, zero retention | **VERIFIED** |
