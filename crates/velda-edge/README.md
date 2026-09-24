# velda-edge

`velda-edge` is the **Composition Root, Process Supervisor, and Lifecycle Owner** of the Velda Edge Data Plane.

> [!IMPORTANT]
> **Architectural Invariant**: `velda-edge` **does NOT process network traffic or proxy requests**. Its sole purpose is assembling dependencies, bootstrapping the process from Last Known Good (LKG) binary artifacts, managing the local Unix Domain Socket (UDS) IPC channel, and performing atomic, lock-free runtime state swaps.

---

## 1. Ultra-Thin Design & Structure

`velda-edge` is architected as an ultra-thin supervisor composed of 5 bounded modules:

```text
crates/velda-edge/src/
├── main.rs         # Binary entrypoint (CLI args, tracing subscriber, signal hooks)
├── bootstrap.rs    # Cold-start initialization, component assembly, and supervisor loop
├── config.rs       # Path options and LKG binary artifact loaders (*.bin)
├── runtime.rs      # In-memory Runtime snapshot wrapped in ArcSwap
├── reload.rs       # Hot-reload orchestration and atomic runtime swapping
└── uds.rs          # Unix Domain Socket (velda.sock) IPC server
```

---

## 2. Process Lifecycles

### Cold-Start Bootstrap (`bootstrap.rs`)
Upon process startup, `velda-edge` operates autonomously without requiring the Go Control Plane to be online:

```text
  LKG Storage (runtime/*.bin)
              ↓
  load_from_storage() (config.rs)
              ↓
  Runtime::load_from_storage() (runtime.rs)
              ↓
  Initialize SharedRuntime (ArcSwap<Runtime>)
              ↓
  Convert Listeners to IngressBindings
              ↓
  TrafficEngine::bind() (velda-transport)
              ↓
  Spawn UDS IPC Listener (uds.rs)
              ↓
  Serve Ingress Traffic
```

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
cargo fmt --check

# Strict Clippy validation
cargo clippy -p velda-edge --all-targets --all-features -- -D warnings

# Unit & E2E integration tests (Cold start + UDS hot reload)
cargo test -p velda-edge
```
