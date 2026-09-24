# velda-sync

`velda-sync` is an autonomous background daemon responsible for **acquiring, validating, compiling declarative configuration into domain-isolated binary artifacts (`*.bin`), and persisting a durable Last Known Good (LKG) state on the local node**.

> [!IMPORTANT]
> **Hot-Path Invariant**: `velda-sync` **is not on the request traffic path**. Its sole responsibility is pre-compiling raw configuration into optimized binary artifacts so `velda-edge` can load snapshots directly into RAM with zero CPU cycles spent parsing JSON during startup or hot reload.

---

## 1. Operating Modes & Configuration Providers

`velda-sync` acquires configurations via two pure capability providers:

- **Standalone Mode (`LocalFileProvider`)**: Reads raw configuration files from local disk (default: `/etc/velda/config/`, configurable via `VELDA_CONFIG_DIR`). Ideal for bare-metal, edge devices, or GitOps pipelines.
- **Control Plane Mode (`ControlPlaneProvider`)**: Fetches desired delta configurations over gRPC with TLS / mTLS (configurable via `VELDA_CONTROL_PLANE_ENDPOINT`, `VELDA_CONTROL_PLANE_TIMEOUT_MS`).
- **Autonomous Invariant**: The Control Plane is strictly a provider of *desired state*, never a runtime dependency. If network connectivity fails, `velda-sync` automatically falls back to the local staged LKG, ensuring `velda-edge` continues serving traffic uninterrupted.

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `VELDA_SYNC_MODE` | `standalone` | Mode: `standalone` or `control_plane` |
| `VELDA_CONFIG_DIR` | `/etc/velda/config` | Source configuration directory (Standalone Mode) |
| `VELDA_STORAGE_DIR` | `/var/lib/velda` | Durable LKG storage root (`config/` and `runtime/`) |
| `VELDA_SOCKET_PATH` | `/run/velda/edge.sock`| Unix Domain Socket path for reload signaling |
| `VELDA_POLL_INTERVAL_MS` | `1000` | Polling interval in milliseconds between reconciliation cycles |
| `VELDA_CONTROL_PLANE_ENDPOINT` | `https://localhost:8443` | Control Plane gRPC endpoint (Control Plane Mode) |
| `VELDA_CONTROL_PLANE_TIMEOUT_MS` | `10000` | Network timeout for Control Plane operations (ms) |
| `VELDA_CA_CERT` | _(none)_ | Path to custom CA certificate for TLS verification |
| `VELDA_CLIENT_CERT` | _(none)_ | Path to client certificate for mTLS authentication |
| `VELDA_CLIENT_KEY` | _(none)_ | Path to client private key for mTLS authentication |
| `VELDA_SYNC_LOG_LEVEL` | `info` | Logging verbosity (`trace`, `debug`, `info`, `warn`, `error`) |
| `VELDA_SYNC_LOG_FORMAT` | `compact` | Logging format: `compact` (console) or `json` (production) |

---

## 2. 3-Stage Synchronization Pipeline

The daemon executes a linear, 3-stage pipeline on every reconciliation cycle:

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                              velda-sync                                │
 │                                                                        │
 │  [STAGE 1: PRE-SYNC]  (src/pre_sync.rs)                                │
 │  • Phase 1: Raw Acquisition (Local FS / gRPC fetch_delta)              │
 │  • Phase 2: Verify & Decompress (Gzip decompress + SHA-256 check)      │
 │  • Phase 3: Disk Staging & Autonomous LKG Fallback (staging/ buffer)   │
 │  • Phase 4: Manifest Decode & Schema Version Validation                │
 │                                    │                                   │
 │                                    ▼                                   │
 │  [STAGE 2: SYNC]      (src/sync.rs)                                    │
 │  • Phase 1: Acquire Candidate Manifest from Pre-Sync Stage             │
 │  • Phase 2: Delta & Revision Detection (Filter changed_domains only)   │
 │  • Phase 3: Dispatch Changed Domains to Post-Sync Subsystems           │
 │  • Phase 4: Durable LKG Manifest Finalization (Atomic swap)            │
 │  • Phase 5: Hot-Reload Notification over UDS Socket                    │
 │                                    │                                   │
 │                                    ▼                                   │
 │  [STAGE 3: POST-SYNC] (src/post_sync/)                                 │
 │  Executes 5 isolated domain branches (routes, listeners, upstreams,     │
 │  plugins, tls) across 5 standard phases:                               │
 │  • Phase 1: Entity & Schema Definitions                                │
 │  • Phase 2: Ingest & Parse (JSON -> Typed Structs)                     │
 │  • Phase 3: Semantic Validation & Normalization                        │
 │  • Phase 4: Binary Compilation & Packing (DomainHeader + Bincode)      │
 │  • Phase 5: Atomic Persistence (Durable LKG: *.json + *.bin)           │
 └──────────────────────────────────┬─────────────────────────────────────┘
                                    │
                  IPC Notification  │ (Unix Domain Socket: /run/velda/edge.sock)
                  {                 │  changed_domains: ["upstreams"],
                    manifest_rev,   │  domain_revisions: { "upstreams": 43 },
                    bin_path,       │  domain_bins: { "upstreams": ".../upstreams.bin" }
                    changed_domains }  (Zero JSON payload sent via IPC)
                                    ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │                              velda-edge                                │
 │                                                                        │
 │  • Receive IPC ──► Inspect changed_domains                             │
 │  • Load only changed binary artifact (Zero JSON parse, Zero disk I/O)  │
 │  • Verify DomainHeader Magic (0x56454C44) & Checksum                   │
 │  • Atomic Swap ArcSwap<Snapshot> (Zero traffic interruption)           │
 └────────────────────────────────────────────────────────────────────────┘
```

### Stage 1: Pre-Sync (`src/pre_sync.rs`)
Prepares candidate configurations before reconciliation:
1. **Phase 1: Raw Data Acquisition**: Reads bytes from `LocalFileProvider` or queries gRPC delta from `ControlPlaneProvider`.
2. **Phase 2: Decompression & Integrity Check**: Decompresses Gzip payloads and verifies 32-byte SHA-256 checksums.
3. **Phase 3: Candidate Staging & LKG Fallback**: Buffers candidate files into `staging/`. If the remote Control Plane is unreachable, triggers the **Autonomous Fallback** to retain existing staged LKG.
4. **Phase 4: Manifest Decode**: Deserializes candidate `manifest.json` and enforces `schema_version == 1`.

### Stage 2: Sync (`src/sync.rs`)
Central reconciliation and orchestration engine (`SyncComposition`):
1. **Phase 1: Ingest Candidate Manifest & Fast Short-Circuit**: Receives decoded manifest. If manifest revision and SHA-256 match current state, short-circuits immediately in **~3.7 µs** ($O(1)$) without touching domain files.
2. **Phase 2: Delta & Revision Detection**: Compares SHA-256 content hashes and revision numbers against in-memory state. Unchanged domains are skipped with **zero allocations and zero compute**.
3. **Phase 3: Domain Dispatch**: Delegates modified domains to Stage 3 compilers.
4. **Phase 4: Durable LKG Finalization**: Writes updated `config/manifest.json` atomically (`.tmp` ──► `fsync` ──► `rename`).
5. **Phase 5: Hot-Reload IPC Notification**: Notifies `velda-edge` over Unix Domain Socket with changed domain paths.

### Stage 3: Post-Sync (`src/post_sync/`)
Compiles and persists domain subsystems independently:
- **`routes`**: L7 matching rules (hosts, path prefixes, headers), timeouts, upstream targets.
- **`listeners`**: Socket binds, protocols (HTTP, HTTPS, TCP), TLS profile mappings.
- **`upstreams`**: Backend pools, load-balancing algorithms, health checks, timeouts.
- **`plugins`**: Request/response policy hooks (rate limiting, auth, WAF).
- **`tls`**: TLS certificates, private keys, and SNI profile catalogs.

Each domain branch implements an identical 5-phase lifecycle:
1. **Phase 1: Entity Definitions**: Strongly-typed Rust data structures.
2. **Phase 2: Parse**: Decodes JSON bytes into domain structures.
3. **Phase 3: Validate**: Enforces semantic rules and in-place string normalizations.
4. **Phase 4: Binary Compilation**: Serializes into binary payload with 128-byte `DomainHeader` (Magic `0x56454C44`, Tag, Revision, SHA-256).
5. **Phase 5: Atomic Persistence**: Persists `.json` (for audit) and `.bin` (for Data Plane boot) with POSIX atomic rename.

---

## 3. Storage Layout & Filesystem Safety

Artifacts are persisted under `storage_dir` (default: `/var/lib/velda`):

```text
/var/lib/velda/
├── config/
│   ├── manifest.json  # Active manifest snapshot
│   ├── listeners.json # Canonical JSON for audit and diffing
│   ├── routes.json
│   ├── upstreams.json
│   ├── plugins.json
│   └── tls.json
└── runtime/
    ├── listeners.bin  # Compiled binary artifacts (DomainHeader + Bincode)
    ├── routes.bin     # Data Plane loads snapshots instantly into RAM
    ├── upstreams.bin
    ├── plugins.bin
    └── tls.bin
```

All disk writes adhere to the atomic sequence: **Write Hidden Temp (`.{file}.tmp.{pid}`) ──► `fsync` ──► Atomic `rename`**, guaranteeing that unexpected crashes or power loss can never produce corrupt state.

---

## 4. Performance & Benchmark Registry

For complete multi-stage benchmarks, latency percentiles, throughput measurements, and Big-O linearity analysis, see:
👉 **[benchmark.md](./benchmark.md)**

| Subsystem | Latency / Metric | Complexity |
|---|---|:---:|
| **Reconciler Idle Loop** | **3.81 µs** (Fast Short-Circuit on Manifest Checksum) | $\mathbf{\mathcal{O}(1)}$ |
| **Data Plane Boot (Routes)** | **5.48 ms** (Binary unpack vs 9.83 ms JSON parse, 55.8% smaller) | $\mathcal{O}(N)$ |
| **Streaming SHA-256** | **1.54 GB/s** (Zero heap allocations) | $\mathcal{O}(N)$ |
| **Metrics Recording** | **12 ns** (Atomic in-memory, zero heap allocations) | $\mathbf{\mathcal{O}(1)}$ |
| **Non-blocking Logging** | **1.30 µs** caller latency (771,000 logs/s) | $\mathbf{\mathcal{O}(1)}$ |

