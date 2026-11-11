# Prometheus Telemetry Exporter Plugin (`prometheus_exporter`) — Architecture

This document specifies the architectural design, threading model, memory layout, lock-free telemetry aggregation, socket lifecycle handoff, and OpenMetrics serialization engine of the **`prometheus_exporter`** background plugin in Velda Edge.

---

## 1. Architectural Mission & Invariants

The `prometheus_exporter` plugin (`crates/velda-plugin/src/background/prometheus_exporter/`) exposes a production-grade, OpenMetrics-compliant HTTP/1.1 and HTTPS endpoint for scraping gateway metrics (`GET /metrics`) and liveness probes (`GET /healthz`) on a dedicated management port (default `9090`).

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        SHADOW LOGISTICS RUNNER (velda-shadow-0)                        │
│                                                                                        │
│   Incoming Scraper (Prometheus / VictoriaMetrics / Datadog / K8s)                      │
│                                │                                                       │
│                                ▼                                                       │
│                   ┌───────────────────────────┐                                        │
│                   │  TcpListener.accept()     │  (Port 9090, Leased from velda-core)   │
│                   └─────────────┬─────────────┘                                        │
│                                 │ Hard Scrape Timeout (timeout_ms)                     │
│                                 ▼                                                       │
│                   ┌───────────────────────────┐                                        │
│                   │  Concurrency Semaphore    │  (Bounded permits: max_concurrency)    │
│                   └─────────────┬─────────────┘                                        │
│                                 │                                                      │
│                                 ▼                                                       │
│                   ┌───────────────────────────┐                                        │
│                   │  Auth & CORS Verification │  (Precomputed Token / Zero Alloc)      │
│                   └─────────────┬─────────────┘                                        │
│                                 │ (Authorized Scrape Request)                          │
│                                 ▼                                                       │
│   [ FRONTLINE CORES ] ───►┌───────────────────────────┐                                │
│   Per-Core Atomic Slots   │  MetricsStore::snapshot() │  Lock-Free Non-blocking Read   │
│   (CoreSlotTable)         └─────────────┬─────────────┘  Zero Traffic Contention       │
│                                         │                                              │
│                                         ▼                                              │
│                   ┌───────────────────────────┐                                        │
│                   │   format_snapshot()       │  200-Byte LUT (DIGIT_PAIRS)            │
│                   │   OpenMetrics Text Engine │  Zero Division, Single-Pass Assembly   │
│                   └─────────────┬─────────────┘                                        │
│                                 │ Bounded Buffer Guard (max_buffer_size_kb)            │
│                                 ▼                                                       │
│                   ┌───────────────────────────┐                                        │
│                   │   Wire HTTP/1.1 Response  │  Content-Type: text/plain;             │
│                   │   stream.write_all(buf)   │  version=0.0.4; charset=utf-8          │
│                   └───────────────────────────┘                                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### Core Invariants:
1. **Shadow Domain Isolation**: Runs exclusively on `velda-shadow-0` under Linux `SCHED_IDLE`. It never touches traffic event loops, worker pinning, or request hot-path execution.
2. **Lock-Free Point-In-Time Telemetry Extraction**: Frontline workers update telemetry via per-core atomic slot arrays (`CoreSlotTable`). Scrape requests extract point-in-time snapshots (`MetricsStore::snapshot()`) lock-free, guaranteeing zero write stalls and zero cacheline invalidation (MESI thrashing) on the request hot path.
3. **Pure Serialization / Zero Recalculation**: All rates, percentiles, and counters are pre-aggregated and stored as pure 64-bit integers (`u64`/`i64`) by `velda-observability`. The formatter strictly serializes snapshots into text without re-calculating business telemetry or executing floating-point math during serving.
4. **Zero Heap Churn via Reusable Scratch Buffers**: Scrape responses reuse an allocated string buffer across sequential requests, avoiding per-scrape heap allocations and memory fragmentation.
5. **Stack 2-Digit LUT ASCII Conversion**: High-speed integer-to-ASCII serialization using a static 200-byte lookup table (`DIGIT_PAIRS`), eliminating division loops and `std::fmt` runtime parsing.
6. **Strict Bound Enforcement**: Output size is bounded by `max_buffer_size_bytes` to prevent memory exhaustion under adversarial conditions.

---

## 2. Socket Lifecycle Handoff from Core (`velda-core::socket`)

In compliance with the **Hourglass Architecture** and **Subsystem Boundaries**, `prometheus_exporter` does not execute raw socket syscalls, port binding, or process-wide conflict detection:

- **Socket Authority Delegated to Core**: All network listener lifecycles, socket options (`SO_REUSEADDR`, `SO_REUSEPORT`), and port collision checks are centrally owned and managed by [`velda-core::socket::GlobalSocketManager`](../../../../velda-core/src/socket.rs).
- **Port Isolation Invariant**: The management port (e.g. `9090`) is strictly disjoint from all traffic listeners:
  $$\text{ManagementPort} \cap \text{TrafficPorts} = \emptyset$$
  Extreme traffic spikes or DDoS saturation on ports 80/443 can never exhaust socket file descriptors or block telemetry scrapes.
- **Direct Handoff at Bootstrap**: During bootstrap, the runtime environment requests a shadow socket claim (`SocketRole::Shadow`) from `velda-core` for the configured management port. Once validated, the pre-bound `std::net::TcpListener` handle is transferred directly to `PrometheusExporter`.
- **Autonomous Shadow Loop**: Upon receiving the listener handoff, `PrometheusExporter` executes exclusively within `velda-shadow-0` (`SCHED_IDLE`) via its accept loop. Frontline traffic workers (`velda-worker-{0..N}`) remain completely detached from and unaware of the management socket.
- **Clean Deallocation on Shutdown**: On SIGTERM, `PrometheusExporter` terminates its accept loop and invokes `velda_core::global_socket_manager().release_shadow_tcp("prometheus_exporter")`, cleanly returning the socket to the kernel.

---

## 3. Subsystem Architecture & Component Breakdown

The plugin is structured into two cohesive modules:

### 3.1 `server.rs` — Management Connection Engine
- **`ExporterStream`**: Unified enum abstraction over plaintext TCP (`TcpStream`) and TLS (`Box<TlsStream<TcpStream>>`).
- **`PrometheusExporter`**: Autonomous background service owning:
  - `concurrency_limiter: Arc<Semaphore>`: Hard limit on active scrape connections (`max_concurrency`).
  - `expected_auth_header: Option<String>`: Precomputed `Bearer <token>` or `Basic <base64>` header string for $\mathcal{O}(1)$ constant-time matching with zero per-request allocations.
  - `tls_acceptor: Option<TlsAcceptor>`: Rustls engine for HTTPS and optional mTLS verification.
- **Connection Lifecycle**:
  1. Accepts client connection with hard per-scrape timeout (`timeout_ms`).
  2. Parses request line on stack (1024B buffer).
  3. Evaluates CORS preflight (`OPTIONS`) or authentication credentials.
  4. Acquires semaphore permit; rejects with `503 Service Unavailable` if full.
  5. Takes lock-free snapshot from `MetricsStore::snapshot()`.
  6. Renders OpenMetrics text via `formatter::format_snapshot()`.
  7. Streams HTTP/1.1 wire response with `Content-Type: text/plain; version=0.0.4; charset=utf-8`.

### 3.2 `formatter.rs` — High-Speed OpenMetrics Serializer
- **`DIGIT_PAIRS: &[u8; 200]`**: Static 2-digit ASCII lookup table (`b"000102...99"`).
- **`fast_write_u64(buf, n)`**: Stack-based 2-digit integer-to-ASCII serializer with zero division instructions.
- **`fast_write_i64(buf, n)`**: Signed integer serializer with zero-allocation negative sign prefix.
- **`format_snapshot()`**: Single-pass byte assembly rendering:
  - Pipeline request counts (`velda_requests_total`) by status class (`2xx`, `4xx`, `5xx`).
  - Active streams (`velda_http_active_streams`).
  - Downstream & upstream network byte counters (`rx_bytes_total`, `tx_bytes_total`).
  - Upstream connection pool metrics (`active_connections`, `idle_connections`, `leases_total`, `timeouts_total`).
  - Request latency & TTFB histograms (`_bucket{le="..."}`, `_sum`, `_count`).
  - Global connection and TLS handshake counters.

---

## 4. Memory Layout & Zero-Allocation Invariants

### 4.1 Lookup Table Memory Footprint (`DIGIT_PAIRS`)
```text
Offset (Dec)   0   1   2   3   4   5   6   7   8   9  ... 198 199
ASCII Bytes   '0' '0' '0' '1' '0' '2' '0' '3' '0' '4' ... '9' '9'
Values         00      01      02      03      04  ...  99
```
- Exactly **200 bytes** stored in the `.rodata` segment.
- Fits into less than **4 contiguous L1 Data Cache lines** (64 bytes each).
- Once accessed during the first metric line, it remains permanently resident in L1D cache ($< 1\text{ns}$ access latency).

### 4.2 Reusable Scratch Buffer Lifecycle
```text
  [ Heap String Scratch Buffer ]
  ├── Pre-reserved Capacity: pipelines.len() * 640B + 2048B
  ├── format_snapshot(&snapshot, &mut buffer, max_bytes)
  │     ├── buffer.clear() (Preserves allocated memory capacity)
  │     ├── buffer.as_mut_vec() (Direct byte writing via unsafe)
  │     └── BufferOverflow check: assert!(buf.len() <= max_bytes)
  └── Streamed to Socket ──► Retained for Next Scrape (Zero Reallocations)
```

---

## 5. Scrape Protection & DoS Hardening

| Protection Layer | Parameter | Default | Behavioral Contract |
| :--- | :--- | :---: | :--- |
| **Concurrency Bounding** | `scrape.max_concurrency` | `16` | Enforced via `tokio::sync::Semaphore`. Surplus scrapers are dropped immediately with `503 Service Unavailable`, preventing CPU starvation. |
| **Hard Scrape Timeout** | `scrape.timeout_ms` | `5000` | Bounded by `tokio::time::timeout`. Slowloris scrapers or stalled TCP connections are terminated, reclaiming file descriptors. |
| **Buffer Overflow Guard** | `scrape.max_buffer_size_kb` | `256` | Strictly bounds rendered text. If cardinality explodes, serialization halts immediately returning `PluginError::BufferOverflow`. |
| **Authentication Token** | `auth.mode` | `"none"` | Precomputes formatted header strings at startup; evaluates scraper headers in constant time without string allocations. |

---

## 6. Graceful Draining & Lifecycle Sequence

```mermaid
sequenceDiagram
    autonumber
    actor Admin as Runtime Supervisor
    participant Exp as PrometheusExporter (velda-shadow-0)
    participant Core as GlobalSocketManager (velda-core)
    participant Scraper as Prometheus Scraper

    Admin->>Exp: Broadcast shutdown signal (watch::Receiver)
    Note over Exp: Accept loop exits immediately.<br/>Stops receiving new connections.
    Exp->>Core: release_shadow_tcp("prometheus_exporter")
    Note over Core: Listener closed;<br/>Port 9090 returned to OS kernel.
    Scraper->>Exp: Subsequent scrape attempt
    Note over Scraper: Connection Refused (Port Cleanly Closed)
```
