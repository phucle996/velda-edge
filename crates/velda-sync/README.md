# velda-sync

`velda-sync` is an autonomous background daemon responsible for **acquiring, validating, compiling declarative configuration into domain-isolated binary artifacts (`*.bin`), and persisting a durable Last Known Good (LKG) state on the local node**.

> [!IMPORTANT]
> **Hot-Path Invariant**: `velda-sync` **is not on the request traffic path**. Its sole responsibility is pre-compiling raw configuration into optimized binary artifacts so `velda-edge` can load snapshots directly into RAM with zero CPU cycles spent parsing JSON during startup or hot reload.

---

## 1. Architecture & Operating Modes

`velda-sync` operates independently from the Data Plane and supports two configuration acquisition modes:

```text
               ┌───────────────────────┐
               │  Configuration Source │
               └───────────┬───────────┘
                           │
             ┌─────────────┴─────────────┐
             ▼                           ▼
     [Standalone Mode]         [Control Plane Mode]
   /etc/velda/config/            https://cp:8443
   (manifest + modular files)    (remote desired state)
             │                           │
             └─────────────┬─────────────┘
                           ▼
                      velda-sync
```

### 1.1 Standalone Mode (Local Filesystem)
Designed for bare-metal deployments, autonomous edge nodes, or GitOps pipelines:
- Reads a local configuration directory (default: `/etc/velda/config/`, configurable via `VELDA_CONFIG_DIR`) orchestrated by `manifest.json`:
  ```text
  /etc/velda/config/
  ├── manifest.json      # Defines the active revision and versioned domain file entries
  ├── listeners.json     # Bind IP:Port, protocol (http/tcp)
  ├── routes.json        # Downstream matching (host, path), timeouts, target upstream name
  ├── upstreams.json     # Backend resolution (DNS, static endpoints), LB algorithm, timeouts
  ├── plugins.json       # Resource limits, JWT auth, rate limits, WAF, logging
  └── tls.json           # TLS profile catalog (certificate/key resolution by SNI)
  ```
- Operates 100% autonomously with zero external network dependencies.

### 1.2 Control Plane Mode (Remote Managed)
Designed for distributed gateway clusters with centralized management:
- Periodically pulls candidate configurations from a remote Control Plane endpoint (configurable via `VELDA_CONTROL_PLANE_ENDPOINT`, `VELDA_STAGING_DIR`).
- **Autonomous Invariant**: The Control Plane is strictly a provider of *desired state*, never a runtime dependency. If the Control Plane goes offline or network connectivity is severed, `velda-sync` retains the local LKG, and `velda-edge` continues serving traffic uninterrupted from RAM.

### 1.3 Daemon Environment Variables

| Variable | Default | Description |
|---|---|---|
| `VELDA_SYNC_MODE` | `standalone` | Acquisition mode: `standalone` (or `local`) / `control_plane` |
| `VELDA_CONFIG_DIR` | `/etc/velda/config` | Source configuration directory (Standalone Mode) |
| `VELDA_STORAGE_DIR` | `/var/lib/velda` | Durable LKG storage root (`config/` and `runtime/`) |
| `VELDA_SOCKET_PATH` | `/run/velda/edge.sock`| Unix Domain Socket path for reload signaling to `velda-edge` |
| `VELDA_POLL_INTERVAL_MS` | `1000` | Polling interval in milliseconds between reconciliation cycles |
| `VELDA_CONTROL_PLANE_ENDPOINT` | `https://localhost:8443` | Control Plane gRPC endpoint (Control Plane Mode) |
| `VELDA_CONTROL_PLANE_TIMEOUT_MS` | `10000` | Network timeout for Control Plane operations (ms) |
| `VELDA_CA_CERT` | _(none)_ | Optional path to custom CA certificate for Control Plane TLS |
| `VELDA_CLIENT_CERT` | _(none)_ | Optional path to client certificate for mTLS authentication |
| `VELDA_CLIENT_KEY` | _(none)_ | Optional path to client private key for mTLS authentication |

---

## 2. Inter-Process Interaction: Ingest, Compile, & IPC Hot Reload

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                              velda-sync                                │
 │                                                                        │
 │  1. Ingest via Provider (LocalFile / ControlPlane)                     │
 │     Read manifest.json & fetch raw content via Provider port           │
 │                                                  │                     │
 │  2. Delta & Change Detection (SyncComposition):  ▼                     │
 │     Compute SHA-256 & compare revisions: filter changed_domains only   │
 │                                                  │                     │
 │  3. Domain Pipeline Delegation (post_sync/):     ▼                     │
 │     Execute domain pipelines: Parse ──► Validate ──► Compile ──► LKG   │
 │     • listener ──► DomainHeader (TAG_LISTENERS) ──► listeners.bin      │
 │     • route    ──► DomainHeader (TAG_ROUTES)    ──► routes.bin         │
 │     • upstream ──► DomainHeader (TAG_UPSTREAMS) ──► upstreams.bin      │
 │     • plugin   ──► DomainHeader (TAG_PLUGINS)   ──► plugins.bin        │
 │     • tls      ──► DomainHeader (TAG_TLS)       ──► tls.bin            │
 │                                                  │                     │
 │  4. LKG Manifest Finalization:                   ▼                     │
 │     Atomically persist manifest.json (.tmp ──► fsync ──► rename)       │
 │                                                  │                     │
 │  5. Hot Reload IPC Transport (ipc.rs):           ▼                     │
 │     Dispatch SyncNotification over Unix Domain Socket                  │
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
 │  6. Granular Hot Reload Event:                                         │
 │     Receive IPC ──► Inspect changed_domains                            │
 │     ──► Load only upstreams.bin (Zero JSON parsing, Zero disk I/O)    │
 │     ──► Verify DomainHeader Magic & Checksum                           │
 │     ──► Atomic Swap ArcSwap<UpstreamSnapshot>                          │
 │     ──► Preserve 100% active sessions and route match trees            │
 │                                                                        │
 │  7. Serving Hot Path:                                                  │
 │     Client Request ─────────────────────────────► RAM ONLY (Zero I/O) │
 └────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Execution Phases (Application Flow & Domain Branching)

Velda Edge enforces a flat workflow architecture, drawing a strict boundary between **top-level daemon orchestration (Application Composition)** and **isolated domain execution (Domain Subsystems)**.

### 3.1 Daemon Orchestration Lifecycle

Orchestrated centrally by `SyncComposition` in [`src/lib.rs`](file:///home/phucle/Desktop/velda-edge/crates/velda-sync/src/lib.rs):

```text
Provider ──► Manifest Check ──► Hash/Revision Filter ──► Domain Dispatch ──► LKG Manifest ──► IPC Broadcast
```

- **Phase 1 — Ingest via Provider**:
  Initializes the `Provider` based on the configured mode (`LocalFileProvider` or `ControlPlaneProvider`), loads `manifest.json`, and validates the schema version.
- **Phase 2 — Delta & Change Detection**:
  Loads raw content bytes for each file entry in the manifest, computes its SHA-256 hash, and compares `(current_hash, current_rev)` against the in-memory cache. If content is unchanged or revision is not newer, execution skips immediately (**Zero compute / Zero allocation overhead** for static domains).
- **Phase 3 — Domain Pipeline Delegation**:
  For each modified domain in `changed_domains`, delegates execution directly to the corresponding domain owner in `post_sync/` to parse, validate, compile, and persist LKG artifacts.
- **Phase 4 — LKG Manifest Finalization**:
  Once modified domains compile and persist successfully, atomically writes `manifest.json` into the durable LKG directory to ensure cross-domain version consistency.
- **Phase 5 — IPC Hot Reload Broadcast**:
  Dispatches a `SyncNotification` over the UDS socket (`/run/velda/edge.sock`). If the Data Plane has not yet started or the socket is not listening, the notification is safely skipped without crashing the daemon.

---

### 3.2 Domain Data Lifecycle (5-Phase Subsystem Standard)

Each domain module (`listener`, `route`, `upstream`, `plugin`, `tls`) in `post_sync/` encapsulates a complete, self-contained 5-phase lifecycle:

| Phase | Phase Name | Technical Specification & Invariant Guarantees | Representative Function |
|---|---|---|---|
| **Phase 1** | **Entity Definitions** | Defines strongly typed domain configuration structs; eliminates generic dynamic bags (`HashMap<String, Box<dyn Any>>`). | `RouteConfig`, `UpstreamConfig`, ... |
| **Phase 2** | **Parse JSON** | Decodes raw source JSON bytes into typed in-memory Rust structs. Fails with precise error location on schema mismatches. | `parse_<domain>(&[u8])` |
| **Phase 3** | **Integrity & Semantic Validation** | - **Fail-Fast**: Errors immediately on missing required fields or invalid values (zero silent fallbacks).<br>- **In-Place Normalization**: Normalizes strings in-place (`trim_in_place`, `make_ascii_lowercase`) with zero heap re-allocations when strings are already clean.<br>- **Zero-Alloc Tracking**: Enforces unique ID constraints via `HashSet<&str>`. | `validate_<domain>(&mut items)` |
| **Phase 4** | **Binary Compilation & Packing** | - Compiles structured configurations into binary artifacts (`*.bin`) via `bincode`.<br>- **Single-Buffer Optimization**: Allocates a single buffer `vec![0; DOMAIN_HEADER_SIZE]`, serializes payloads directly, computes SHA-256 checksums, and overwrites the 128-byte `DomainHeader` (Magic `0x56454C44`, format version, payload checksum) in-place. | `compile_<domain>_to_binary()` |
| **Phase 5** | **Atomic Persistence (Durable LKG)** | Durably persists both canonical `.json` (for audit and inspection) and `.bin` (for Data Plane boot) using the standard: `.tmp` ──► `fsync` ──► `POSIX rename`. | `persist_<domain>()` |

---

## 4. Last Known Good (LKG) Storage & Filesystem Safety Guarantees

LKG artifacts are stored under the designated storage root (`storage.directory`, default: `/var/lib/velda`):

```text
/var/lib/velda/
├── config/
│   ├── manifest.json  # Currently active manifest snapshot
│   ├── listeners.json # Canonical JSON for each domain (audit, inspection, diffing)
│   ├── routes.json
│   ├── upstreams.json
│   ├── plugins.json
│   └── tls.json
└── runtime/
    ├── listeners.bin  # Domain-isolated compiled binary artifacts (DomainHeader + Bincode)
    ├── routes.bin     # Allows the Data Plane to load instantly and ArcSwap hot-reload with zero I/O
    ├── upstreams.bin
    ├── plugins.bin
    └── tls.bin
```

### Atomic Persistence Guarantees:
All file write operations in `/var/lib/velda/` (both Phase 4 daemon manifest finalization and Phase 5 domain persistence) strictly adhere to a 3-step atomic protocol:
1. **Write to Hidden Temp File**: Writes new data to a unique temporary file on the same filesystem (`.{file}.tmp.{pid}`).
2. **Fsync**: Calls `file.sync_all()` to flush all OS page cache buffers to physical storage media.
3. **Atomic Rename**: Invokes POSIX `rename(2)` to atomically swap the temp file into the destination path.

> [!NOTE]
> This protocol guarantees that even in the event of an abrupt power loss or sudden process termination, the previous LKG state remains 100% intact, preventing partial or corrupted files from ever breaking `velda-edge` startup.
