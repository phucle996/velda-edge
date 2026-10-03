# Velda Edge — Architecture Specification

This document specifies the architectural design, core invariants, memory model, and end-to-end request processing workflows of **Velda Edge**.

---

## 1. High-Level System Architecture

Velda Edge is a performance-oriented, polyglot edge platform organized into three distinct tiers:

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                           OPERATOR & UI TIER                           │
 │        Modern Console (React 19.3 + TypeScript + Vite 8 + Tailwind v4) │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │ HTTP / JSON API
                                     ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │                          CONTROL PLANE TIER                            │
 │            Go 1.27 Clean Architecture (PostgreSQL + pgx/v5)            │
 │   - Declarative Schema Validation & Versioned Config Management        │
 │   - Compiles Domain Models into Fast-Loading Binary Artifacts (*.bin)  │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │ UDS Notification & LKG Sync
                                     ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │                           DATA PLANE TIER                              │
 │            Rust 1.98 / Edition 2024 High-Performance Edge Engine       │
 │   - In-Memory Lock-Free Serving via ArcSwap<Runtime>                   │
 │   - Strict Protocol Isolation: HTTP/1.1, HTTP/2, HTTP/3, gRPC, TCP, UDP│
 │   - AOT Pre-Compiled Upstream Pipelines (Zero-IO, Zero-Lookup)        │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │
           ┌─────────────────────────┴─────────────────────────┐
           ▼                                                   ▼
 ┌───────────────────┐                               ┌───────────────────┐
 │ Downstream Client │                               │  Upstream Backend │
 └───────────────────┘                               └───────────────────┘
```

---

## 2. Core Architectural Invariants

### 2.1 The Hot-Path Invariant (Zero-IO in Serving)
Request processing in the Data Plane hot path (`crates/`) **never**:
- Parses JSON or YAML.
- Performs disk I/O.
- Executes synchronous Control Plane RPCs or HTTP queries.
- Compiles configurations or builds routing trees dynamically.

All declarative configs are parsed, validated, and compiled into RAM (`RuntimeSnapshot`) ahead of time by `velda-config`/`velda-sync` before publication to the hot path.

### 2.2 Flat Workflows over Deep Abstraction
Velda Edge strictly rejects deep inheritance chains (`Base -> Abstract -> Manager -> Coordinator -> Handler -> Adapter`).
- Structs own state/data.
- Functions own behavior.
- Every request lifecycle is readable from top to bottom in a single flat workflow:
  $$\text{decode} \longrightarrow \text{pre\_route} \longrightarrow \text{route} \longrightarrow \text{pre\_upstream} \longrightarrow \text{upstream} \longrightarrow \text{post\_response} \longrightarrow \text{encode}$$

### 2.3 Strict Protocol & Pipeline Isolation Invariant
Declared protocol over dynamic sniffing.
- `http` listeners strictly serve HTTP/1.1 and HTTP/2 Web traffic through `HttpRouter`. They **never** evaluate gRPC routes or sniff `Content-Type: application/grpc`.
- `grpc` listeners are dedicated RPC ingress pipelines through `GrpcRouter`. They **never** evaluate HTTP routes.
- Listeners, pipelines, and upstream tables maintain strict protocol boundaries without cross-protocol data pollution.

### 2.4 Canonical Vocabulary: The `Endpoint` Invariant
The term `Endpoint` (`velda_core::Endpoint`) is strictly reserved across the entire codebase to denote a **physical backend destination target** (`address: SocketAddr`, `weight: u32`).
- Listener ports are called `Listener` or `Binding`.
- Client sockets are called `Peer` or `ClientAddr`.
- Routing paths are called `Route`, `Path`, or `Prefix`.

---

## 3. End-to-End Request Pipeline & Flat Handoff

The Data Plane processes all traffic in a 2-stage flat handoff architecture:

```text
CLIENT (Downstream Wire)
       │
       ▼
════════════════════════════════════════════════════════════════════════════════
1. INGRESS STAGE (velda-transport & Downstream TLS)
════════════════════════════════════════════════════════════════════════════════
   • TCP Listener / UDP Socket accepts physical connection.
   • If TLS is active: Executes downstream TLS termination via rustls,
     validating ALPN strictly against declared listener protocol.
   • Static Path Classification: Hands off connection directly to the
     matching protocol pipeline (Zero runtime protocol guessing).
       │
       ▼
════════════════════════════════════════════════════════════════════════════════
2. L7 PIPELINE STAGE (Downstream Frame Decode & Route Match)
════════════════════════════════════════════════════════════════════════════════
   • Protocol pipeline decodes downstream wire frames:
       - HTTP/1.1: Reads request head via zero-copy buffer.
       - HTTP/2: Decodes multiplexed HEADERS frames (RFC 9113).
       - HTTP/3: Decodes QUIC datagrams via Http3Engine (RFC 9114).
       - gRPC: Receives length-prefixed binary message frames.
   • Executes O(1) route lookup in compiled Router ──> resolves `upstream_name`.
   • Retrieves pre-compiled upstream processor from protocol-isolated table:
       `rt.upstreams.{proto}.get(&route.upstream_name)`
       │
       ▼ (Single-line handoff: `upstream.dispatch_pipe(...)`)
════════════════════════════════════════════════════════════════════════════════
3. UPSTREAM HANDOFF STAGE (Upstream Execution Core)
════════════════════════════════════════════════════════════════════════════════
   The autonomous Upstream instance manages the entire backend lifecycle:
   1. Filter Health: Eliminates tripped endpoints (Circuit Breaker).
   2. Single-round LB: Executes `LbAlgorithm` (RR, LeastConn, IpHash, P2C) exactly ONCE.
   3. Connection Acquisition:
       - HTTP/1 & TCP: Checks pool reuse (HIT) or opens socket + TLS handshake (MISS).
       - HTTP/2 & HTTP/3: Reuses existing persistent multiplexed client connection.
   4. Pipe Execution: Downstream and Upstream exchange stream data according
      to pre-computed `strategy` (Buffered, ServerStream, ClientStream, Duplex).
   5. RAII Release: Drop guard records health metrics (`record_success`/`record_failure`)
      and returns healthy connections to the pool.
       │
       ▼
CLIENT RECEIVES RESPONSE
```

---

## 4. Pre-Compiled In-Memory Tables (AOT State Tree)

To enforce the **Zero-IO Hot-Path Invariant**, Velda Edge compiles all declarative configurations ahead of time into a static in-memory state tree hosted within `Runtime` (`ArcSwap<Runtime>`):

```text
RuntimeSnapshot (Gen N, Revision: u64)
│
├── 1. INGRESS & PIPELINES ───────────────────────────── [Instant Protocol Classification]
│   │
│   ├── [PathKind::L4Direct] (Bypass L7 Pipeline)
│   │   ├── Raw TCP Listener ──────────────────> Direct Handoff ──> rt.router.l4 ──> rt.upstreams.tcp
│   │   └── Raw UDP Listener ──────────────────> Direct Handoff ──> rt.router.l4 ──> rt.upstreams.udp
│   │
│   └── [PathKind::L7Handoff] (rt.pipelines: PipelineTable)
│       ├── tcp: HashMap<ListenerId, TcpPipeline>
│       │   ├── Http1 ── { tls_enabled, streaming, config: Http1Config }
│       │   ├── Http2 ── { tls_enabled, streaming, config: Http2Config }
│       │   └── Grpc  ── { tls_enabled, streaming, config: GrpcConfig }
│       │
│       └── udp: HashMap<ListenerId, UdpPipeline>
│           ├── Http3 ── { tls_enabled, streaming, config: Http3Config }
│           └── Grpc  ── { tls_enabled, streaming, config: GrpcConfig }
│
├── 2. rt.router (Router) ────────────────────────────── [Protocol-Isolated Route Matching]
│   │
│   ├── l4:    (listener_id, proto) ───────────────────> L4Route    { upstream_name }
│   ├── http1: (listener_id, path/host/method/headers) ─> Http1Route { upstream_name, timeout_ms, plugins: Vec<PluginId> }
│   ├── http2: (listener_id, path/host/method/headers) ─> Http2Route { upstream_name, timeout_ms, plugins: Vec<PluginId> }
│   ├── http3: (listener_id, path/host/method/headers) ─> Http3Route { upstream_name, timeout_ms, plugins: Vec<PluginId> }
│   └── grpc:  (listener_id, service/method) ───────────> GrpcRoute  { upstream_name, timeout_ms, plugins: Vec<PluginId> }
│
├── 3. rt.upstreams (UpstreamTable) ───────────────────── [Autonomous Pre-Compiled Backends]
│   │
│   │   * Every Upstream owns its DEDICATED Load Balancer and isolated Pool/Cache (NO shared pool).
│   │
│   ├── tcp:   upstream_name ──> TcpUpstream
│   │   ├── inner: EdgeUpstream ───────────────> [Discovery: Vec<Endpoint> + HealthTracker + LbAlgorithm + Timeouts]
│   │   └── pool: Arc<ConnectionPool> ─────────> Dedicated L4 TCP Connection Pool (Idle Keep-Alive)
│   │
│   ├── udp:   upstream_name ──> UdpUpstream
│   │   ├── inner: EdgeUpstream ───────────────> [Discovery: Vec<Endpoint> + HealthTracker + LbAlgorithm + Timeouts]
│   │   └── socket: Arc<UdpSocket> ────────────> Dedicated Non-Blocking UDP Datagram Socket
│   │
│   ├── http1: upstream_name ──> Http1Upstream
│   │   ├── inner: EdgeUpstream ───────────────> [Discovery: Vec<Endpoint> + HealthTracker + LbAlgorithm + Timeouts]
│   │   ├── pool: Arc<ConnectionPool> ─────────> Dedicated HTTP/1.1 Keep-Alive Connection Pool
│   │   ├── [PRE-COMPILED] tls_engine ─────────> Option<Arc<TlsClientEngine>> (Pre-baked Root CAs + mTLS)
│   │   ├── [PRE-COMPILED] target_sni ─────────> Option<String> (Pre-resolved Target SNI Hostname)
│   │   └── [PRE-COMPILED] strategy ───────────> Http1PipeStrategy (Buffered | ServerStream | ClientStream | Duplex)
│   │
│   ├── http2: upstream_name ──> Http2Upstream
│   │   ├── inner: EdgeUpstream ───────────────> [Discovery: Vec<Endpoint> + HealthTracker + LbAlgorithm + Timeouts]
│   │   ├── [PRE-COMPILED] target_sni ─────────> Option<String> (Pre-resolved Target SNI for HTTPS/2)
│   │   ├── client_cache: RwLock<HashMap> ─────> Dedicated Multiplexed H2 Client Conns (RFC 9113)
│   │   └── [PRE-COMPILED] strategy ───────────> Http2PipeStrategy (Buffered | ServerStream | ClientStream | Duplex)
│   │
│   ├── http3: upstream_name ──> Http3Upstream
│   │   ├── inner: EdgeUpstream ───────────────> [Discovery: Vec<Endpoint> + HealthTracker + LbAlgorithm + Timeouts]
│   │   ├── [PRE-COMPILED] target_sni ─────────> Option<String> (Pre-resolved QUIC TLS 1.3 SNI)
│   │   ├── client_cache: RwLock<HashMap> ─────> Dedicated Persistent QUIC Client Conns (RFC 9114)
│   │   └── [PRE-COMPILED] strategy ───────────> Http3PipeStrategy (Buffered | ServerStream | ClientStream | Duplex)
│   │
│   └── grpc:  upstream_name ──> GrpcUpstream
│       ├── inner: EdgeUpstream ───────────────> [Discovery: Vec<Endpoint> + HealthTracker + LbAlgorithm + Timeouts]
│       ├── [PRE-COMPILED] target_sni ─────────> Option<String> (Pre-resolved Target SNI for gRPCS)
│       ├── [PRE-COMPILED] streaming ──────────> StreamingMode
│       └── [PRE-COMPILED] strategy ───────────> GrpcPipeStrategy (Buffered | ServerStream | ClientStream | Duplex)
│
└── 4. rt.tls (TLS Engines) ──────────────────────────── [Pre-Compiled Cryptographic Contexts]
    │
    ├── tls_server: Option<TlsServerEngine> ───> rustls::ServerConfig (SNI Resolver: Host ──> Cert Chain + Key)
    └── tls_client: Option<TlsClientEngine> ───> rustls::ClientConfig (Native Root CAs + mTLS Credentials)
```

### Key Hot-Path Invariants in the State Tree

- **Per-Upstream Pool & Cache Isolation**: Pools are **never** shared globally. Every single Upstream instance (`TcpUpstream`, `Http1Upstream`, `Http2Upstream`, etc.) owns its private, dedicated connection pool or multiplexed client cache.
- **Dedicated Per-Upstream Load Balancers & Timeouts**: Every Upstream owns its private `LbAlgorithm` enum variant (RR, WRR, LeastConn, Random, IpHash, P2C) and `UpstreamTimeouts { connect, idle, request }`. Rotation counters and connection states are completely isolated between backends.
- **Physical Endpoint Topology in Discovery**: Backend endpoints are pre-resolved into canonical `Vec<Endpoint>` (`address: SocketAddr`, `weight: u32`) avoiding dynamic DNS lookups during request execution.
- **Multiplexed Cache vs Single-Request Pool**:
  - **TCP & HTTP/1.1**: Use dedicated `ConnectionPool` because sockets serve 1 request at a time (Head-of-Line blocking).
  - **HTTP/2, HTTP/3, gRPC**: Use dedicated `client_cache` to multiplex hundreds of concurrent streams over persistent established pipes, eliminating connection churn.
- **L4 Direct Bypass vs L7 Handoff**: Raw listeners bypass L7 decoders and pipeline tables completely (`PathKind::L4Direct`), dispatching straight to `rt.router.l4`.
- **Pre-Compiled Route Plugins**: Plugin hook chains (`Vec<PluginId>`) are pre-bound to each Route record for zero dynamic resolution during routing.
- **Zero Protocol Sniffing**: `rt.pipelines` resolves the wire protocol directly from `listener_id` in $O(1)$. No inspecting payloads or sniffing `Content-Type: application/grpc`.
- **Strict Protocol Isolation**: `rt.router` isolates HTTP and gRPC tables into disjoint memory areas. HTTP traffic never touches or iterates over gRPC route rules.
- **Pre-Baked Upstream TLS**: Outbound TLS engines, target SNIs, and root stores are compiled directly into the upstream struct at snapshot build time, eliminating runtime certificate lookups.
- **Single-Line Upstream Handoff**: The L7 pipeline executes dispatch via a direct one-line handoff:
  ```rust
  let upstream = rt.upstreams.http1.get(&route.upstream_name)?;
  upstream.dispatch_pipe(client_req, downstream_stream).await;
  ```


---

## 5. Memory Model & Lock-Free State Swaps

```text
                    SharedRuntime (Arc<ArcSwap<Runtime>>)
                                      │
                   ┌──────────────────┴──────────────────┐
                   ▼                                     ▼
        Active Snapshot (Gen N)               Candidate Snapshot (Gen N+1)
        - listeners: Vec<Listener>             - listeners: Vec<Listener>
        - router: Router                      - router: Router
        - upstreams: UpstreamTable            - upstreams: UpstreamTable
        - pipelines: PipelineTable            - pipelines: PipelineTable
```

- **Wait-Free Reads**: Worker tasks load the active snapshot via atomic pointer dereference (`shared_runtime.load()`), taking `< 15 ns` with **zero heap allocations**.
- **Atomic Swapping**: When `velda-sync` pushes an update, the supervisor compiles a new `Runtime` candidate and atomically swaps the pointer via `ArcSwap::store()`.
- **Natural Generation Draining**: Existing in-flight requests continue executing against Generation $N$ until completion, while new connections immediately read Generation $N+1$. No connection resets, no request drops.

---

## 6. Circuit Breaker & Health Tracking State Machine

The dual-engine health system combines passive inline circuit breaking with active health probes:

```text
               record_success() [Reset fails = 0, Swap unhealthy_since_ms = 0]
       ┌─────────────────────────────────────────────────────────────────────────┐
       │                                                                         │
       ▼                                                                         │
┌─────────────┐       consecutive_fails >= max_failures        ┌─────────────┐   │
│   HEALTHY   │ ─────────────────────────────────────────────> │   TRIPPED   │   │
│ (since = 0) │            CAS(0 -> timestamp_ms)              │ (since > 0) │   │
└─────────────┘                                                └──────┬──────┘   │
       ▲                                                              │          │
       │                                                              │ cooldown │
       │                                                              │ elapsed  │
       │                        record_success()                      ▼          │
       │                 ┌───────────────────────────── ┌───────────────┐        │
       └─────────────────┤ Reset fails = 0              │   HALF-OPEN   │ ───────┘
                         │ Swap unhealthy_since_ms = 0  │ (Trial Probe) │ failure
                         └───────────────────────────── └───────────────┘ CAS new timestamp
```

- **Zero-False-Positive Guarantee**: Only physical L4 socket connect errors (timeout, `ECONNREFUSED`) trigger `record_failure`. HTTP 4xx/5xx responses from backends are valid application traffic and are **never** penalized.
- **Failover Resilience**: If an endpoint fails during connection acquisition, the upstream automatically attempts failover to the next healthy candidate in the slice before returning 502.

---

## 7. Polyglot Monorepo Subsystem Boundaries

| Subsystem | Technology | Responsibility |
|---|---|---|
| `crates/velda-core` | Rust 2024 | Fundamental vocabulary (`Endpoint`, `L7Request`, `L7Response`, strongly typed IDs). |
| `crates/velda-transport` | Rust 2024 | Traffic Ingress, socket accept loops, L4 bidirectional byte forwarding. |
| `crates/velda-tls` | Rust 2024 | Downstream & upstream TLS termination, ALPN validation (`rustls`). |
| `crates/velda-http1` | Rust 2024 | RFC 9112 HTTP/1.1 zero-copy text framing & streaming pipe. |
| `crates/velda-http2` | Rust 2024 | RFC 9113 HTTP/2 binary multiplexing & stream responder. |
| `crates/velda-http3` | Rust 2024 | RFC 9114 HTTP/3 QUIC packet state machine & engine shards. |
| `crates/velda-grpc` | Rust 2024 | gRPC length-prefixed streaming, canonical status codes. |
| `crates/velda-router` | Rust 2024 | O(1) route lookup tables (Path, Host, Method, Headers). |
| `crates/velda-upstream` | Rust 2024 | Generic protocol-agnostic backend lifecycle, health tracker, and pool. |
| `crates/velda-lb` | Rust 2024 | Pure in-memory load balancing algorithms (RR, WRR, LeastConn, P2C, Maglev, RingHash). |
| `crates/velda-edge` | Rust 2024 | Composition root, pipeline integration, pre-compiled upstream tables, hot-reload. |
| `control-plane/` | Go 1.27 | Declarative state management, PostgreSQL CTE-first persistence, binary compilation. |
| `ui/` | React 19.3 | Modern administrative operations console. |
