# velda-core

`velda-core` is the foundational vocabulary crate of the entire Velda Edge Data Plane. This crate strictly defines **data models**, **strongly typed IDs**, **lifecycle contracts**, **hardware topologies**, and the **canonical error taxonomy** shared across all subsystems.

> **Core Invariant**: `velda-core` **does not process requests**; `velda-core` strictly **defines requests and the communication contracts** between components that process them.

---

## 1. System Placement & Architectural Boundaries

`velda-core` sits at the very bottom of the Data Plane dependency graph, completely decoupled from all other internal Velda crates:

```text
       velda-transport   velda-router   velda-upstream   velda-plugin
              │                │               │               │
              └────────────────┴───────┬───────┴───────────────┘
                                       │ (shares common contracts)
                                       ▼
                                  velda-core
```

Within the runtime request pipeline, `velda-core` **is not a processing phase**. It is the concrete set of data types and contracts passed between execution stages:

```text
L4: Client ──► velda-transport ──► Router ──► Upstream
L7: Client ──► velda-transport ──► TLS ──► HTTP ──► Router ──► Plugin ──► Upstream
```

---

## 2. Core Components Owned by `velda-core`

### 2.1 Strongly Typed Identifiers (`src/types.rs`)
Wraps primitive integers (`u64`, `u32`) into distinct domain types to prevent accidental cross-domain misuse at compile-time:

- `RequestId(pub u64)`: Unique identifier for an L7 request. Exactly 8 bytes, zero runtime overhead.
- `RouteId(pub u32)`: Identifier for a compiled routing rule. Exactly 4 bytes.
- `UpstreamId(pub u32)`: Identifier for a selected backend upstream cluster. Exactly 4 bytes.
- `ConnectionId(pub u64)`: Identifier for a physical L4 network connection. Exactly 8 bytes.

All IDs implement: `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `Display`.

---

### 2.2 Layer 4 Network Model (`src/l4/`)
Represents the Transport Layer state (TCP/UDP), completely decoupled from HTTP:

- `L4Request`: Encapsulates `connection_id`, `client_addr`, `local_addr`, `protocol` (`Tcp` or `Udp`), and `peer`.
- `L4Response`: Encapsulates L4 actions (`L4Action::Forward`, `L4Action::Close`, `L4Action::Reject`) and an optional destination target `target: Option<SocketAddr>`.

---

### 2.3 Layer 7 Application Model (`src/l7/`)
Represents HTTP application semantics used for routing, plugins, and upstream forwarding:

- `L7Request`: Encapsulates `Method`, `Uri`, `Version`, `HeaderMap`, and `Body`.
- `L7Response`: Encapsulates `StatusCode`, `Version`, `HeaderMap`, and `Body`.
- `Body`: Enum representing payload data (`Body::Empty`, `Body::Bytes(Bytes)`).

---

### 2.4 Shared Context & State (`src/context.rs`)
Strictly separates the network connection lifecycle from the request lifecycle:

- **`ConnectionContext`**: Represents an active physical connection (carrying `L4Request`). A single connection may persist across multiple sequential requests (HTTP keep-alive) or serve concurrent streams simultaneously (HTTP/2 multiplexing).
- **`RequestContext`**: Represents a single L7 request execution cycle, containing:
  - `l4`: Reference to the underlying `ConnectionContext`.
  - `l7`: The `L7Request` object.
  - `state`: Internal state tracking via `RequestState`.
- **`RequestState`**: Strongly-typed state struct:
  ```rust
  pub struct RequestState {
      pub request_id: RequestId,
      pub route: Option<RouteId>,
      pub upstream: Option<UpstreamId>,
      pub routed: bool,
      pub upstream_started: bool,
      pub upstream_completed: bool,
  }
  ```
  *(Size of `RequestState` is exactly 32 bytes, fitting well within a single 64-byte CPU cacheline).*

---

### 2.5 Lifecycle Contracts & Hooks (`src/lifecycle.rs`)
Defines pipeline lifecycle phases and bounded execution authority for plugin hooks:

- **`Phase`**: Enum of 7 sequential request phases (1 byte):
  `Accept`, `Decode`, `PreRoute`, `Route`, `PreUpstream`, `Upstream`, `PostResponse`.
- **`HookPhase`**: Enum of extensible hook phases (1 byte):
  `PreRoute`, `PreUpstream`, `PostResponse`.
- **`Action`**: Finite authority granted to hook outcomes:
  ```rust
  pub enum Action {
      Continue,              // Proceed to next pipeline stage
      Respond(L7Response),   // Short-circuit pipeline and return early response (401, 403, 429...)
      Reject(Error),         // Terminate request with a specific error
  }
  ```
- **`Hook` trait**: Thread-safe (`Send + Sync`) contract for plugin implementations.

---

### 2.6 Canonical Error Taxonomy (`src/error.rs`)
Provides a unified error classification (`ErrorKind`, 1 byte) allowing inter-crate communication without leaking transport-specific status code mappings:

```rust
pub enum ErrorKind {
    InvalidRequest,       // Malformed request
    Protocol,             // Protocol violation (HTTP/TLS framing)
    RouteNotFound,        // No matching route found
    RouteConfig,          // Route configuration error
    UpstreamUnavailable,  // Backend down or connection pool exhausted
    UpstreamFailure,      // Backend connection failed
    Timeout,              // Connect or read timeout elapsed
    Connection,           // Physical socket I/O error
    Rejected,             // Rejected by policy/plugin (WAF, rate limiter)
    Canceled,             // Client disconnected prematurely
    Internal,             // Unclassified internal error
}
```

---

### 2.7 Hardware Topology: Decoupled CPU & Memory Probing (`src/hardware/`)
Detects physical and container hardware allocations once at cold start from the Linux kernel and cgroups, caching the topology in RAM (`OnceLock`) without reading environment variables:

- `src/hardware/cpu.rs`:
  - `probe_cpu()`: Inspects cgroups v2 (`/sys/fs/cgroup/cpu.max`), cgroups v1 (`cpu.cfs_quota_us`), or falls back to system `available_parallelism`.
  - **`CpuTier`**: `Constrained` (1-2 cores), `Small` (3-4 cores), `Medium` (5-8 cores), `Large` (9-16 cores), `XLarge` (17-32 cores), `TwoXLarge` (33-64 cores), `Ultra` (> 64 cores).
  - Used to dynamically scale concurrency partitions and channel buffer capacities.
- `src/hardware/memory.rs`:
  - `probe_memory()`: Inspects cgroups v2 (`memory.max`), cgroups v1 (`memory.limit_in_bytes`), or `/proc/meminfo` `MemTotal`.
  - **`MemoryTier`**: `Constrained` (< 512 MB), `Small` (512 MB – 2 GB), `Medium` (2 GB – 8 GB), `Large` (8 GB – 32 GB), `XLarge` (32 GB – 64 GB), `TwoXLarge` (64 GB – 128 GB), `Ultra` (> 128 GB).
  - Used to scale TCP/UDP socket buffers, listen backlogs, and DNS/LKG cache capacities.
- `src/hardware/mod.rs`:
  - `HardwareTopology`: Aggregates `CpuProfile` and `MemoryProfile`, exposing `cpu_tier()` and `memory_tier()`.

---

### 2.8 Overload Protection & Memory Circuit Breaker (`src/hardware/overload.rs`)
Proactively defends the Data Plane against Linux Kernel OOMKill (`SIGKILL 9`) during severe load spikes or memory exhaustion, preserving inflight transactions and critical streams:

- **`OverloadLevel`**: 3-level operational load state (1 byte):
  - `Normal (0)`: Safe memory zone. Serves 100% of requests and streaming pipes without restriction.
  - `Shedding (1)`: Memory enters warning threshold. The engine rejects new streaming/duplex pipes (returning generic `429 Too Many Requests`), but allows active inflight streams to drain and complete cleanly.
  - `Critical (2)`: Memory nears physical limits. Proactively rejects new incoming requests, transmits HTTP/2 `GOAWAY` frames to gracefully shed downstream clients, and sheds load immediately.
- **Hardware-Calibrated Watermarks (`OverloadConfig::for_tier`)**:
  - *Architectural Rationale*: An 80% RAM threshold on a 60MB-512MB VPS leaves merely ~12MB of headroom (a small concurrent burst can easily trigger an OOMKill before buffers drain). Conversely, 80% RAM on a 64GB-256GB bare-metal host leaves dozens of gigabytes of unused buffer. Therefore, watermark thresholds adapt dynamically based on `MemoryTier`:
    - `Constrained` (< 512 MB): Shedding 70%, Critical 80% (20% safety headroom).
    - `Small` (512 MB – 2 GB): Shedding 75%, Critical 85% (15% safety headroom).
    - `Medium` (2 GB – 8 GB): Shedding 80%, Critical 88% (12% safety headroom).
    - `Large` (8 GB – 32 GB): Shedding 85%, Critical 92% (8% safety headroom).
    - `XLarge` (32 GB – 64 GB): Shedding 88%, Critical 94% (6% safety headroom).
    - `TwoXLarge` / `Ultra` (> 64 GB): Shedding 90%, Critical 95% (5% safety headroom).
  - Operators can override these defaults dynamically via the `overload` section in `runtime.json`.
- **Hysteresis (Schmitt Trigger)**:
  - Separate entry thresholds (`high_watermark`) and recovery thresholds (`low_watermark`) prevent state flapping under oscillating memory pressure.
- **Zero-Lock Hot-Path (~1ns)**:
  - `OverloadTracker` tracks current level via an `AtomicU8` with `Ordering::Relaxed`. Every request and streaming pipe checks status in ~1ns without mutex contention.
- **cgroups v2/v1 & Slice Subpath Discovery**:
  - Automatically resolves container subpaths via `/proc/self/cgroup` (full compatibility with systemd service slices: `memory.current`, `memory.max`), gracefully falling back to RSS via `/proc/self/statm`.

---

## 3. Explicit Boundaries: What `velda-core` NEVER Does

To maintain absolute zero-allocation efficiency and avoid business logic coupling, `velda-core` **NEVER**:

- ❌ Opens TCP/UDP sockets or executes network I/O *(owned by `velda-transport`)*.
- ❌ Executes TLS handshakes or manages certificates *(owned by `velda-tls`)*.
- ❌ Parses wire-level HTTP protocol frames *(owned by `velda-http1`, `velda-http2`, `velda-http3`)*.
- ❌ Matches routes or executes path regexes *(owned by `velda-router`)*.
- ❌ Resolves DNS or balances load across endpoints *(owned by `velda-lb` and `velda-upstream`)*.
- ❌ Manages connection pools or socket pooling lifecycles *(owned by `velda-connection-pool`)*.
- ❌ Manages plugin chains or hook execution ordering *(owned by `velda-plugin`)*.
- ❌ Spawns Tokio runtime background worker threads *(owned by `velda-edge`)*.
- ❌ Parses JSON runtime configuration files *(owned by `velda-sync`)*.

---

## 4. Layout Verification, Zero-Allocation & Dirty Input Testing

The crate includes two rigorous test suites:

1. **`tests/layout_and_alloc.rs`**:
   - Uses a custom `CountingAllocator` to enforce **zero heap allocations** on all hot-path operations: creating/querying IDs, transitioning `RequestState`, reading context lookups, and checking `Action::Continue`.
   - Validates that `size_of::<RequestState>() <= 64` (fits inside 1 L1 Cacheline) and payload-free enums occupy exactly 1 byte.
2. **`tests/dirty_inputs.rs`**:
   - Tests integer boundary conditions (`0`, `u64::MAX`, `u32::MAX`).
   - Tests dirty and unconventional IP/Socket addresses (`0.0.0.0`, `255.255.255.255`, multicast, link-local, IPv6).
   - Tests dirty URIs, directory traversal attempts, percent-encoded sequences, and large payloads (1MB).
   - Tests out-of-order and conflicting state transitions.
   - Tests extreme error messages (64KB strings) and deeply nested error sources (`with_source`).

---

## 5. Verification & Testing Standards

```bash
cargo fmt --check -p velda-core
cargo check -p velda-core
cargo test -p velda-core
cargo clippy -p velda-core --all-targets --all-features -- -D warnings
```
