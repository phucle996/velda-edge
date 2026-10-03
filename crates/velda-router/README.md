# velda-router

High-performance, protocol-isolated in-memory routing engine for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Invariant**: `Router = Pure In-Memory Path & Protocol Evaluator`.
> `velda-router` owns route matching (`Path`, `Host`, `Method`, `Service`, `Authority`) and route selection.
> It performs **zero dynamic protocol sniffing, zero heap allocation, and zero I/O on the request serving hot path**.

---

## 1. Subsystem Architecture & Ingress Protocol Isolation

`velda-router` enforces the **Explicit Ingress Protocol & Pipeline Isolation Invariant** ([`AGENTS.md §2.7`](../../AGENTS.md)): listeners explicitly declare their protocol, and requests evaluate directly against dedicated, protocol-isolated sub-routers without dynamic header inspection or protocol fallbacks.

```text
                                Downstream Request
                                        │
                      ┌─────────────────┴─────────────────┐
                      ▼                                   ▼
              L4 Direct Traffic                   L7 Application Stream
             (Raw TCP / Raw UDP)            (Declared Protocol on Ingress)
                      │                                   │
         ┌────────────┴────────────┐                      │
         ▼                         ▼                      │
┌─────────────────┐       ┌─────────────────┐             │
│    TcpRouter    │       │    UdpRouter    │             │
│   (O(1) Map)    │       │   (O(1) Map)    │             │
└────────┬────────┘       └────────┬────────┘             │
         │                         │                      │
         └────────────┬────────────┘                      │
                      │                                   │
         ┌────────────┴────────────┬──────────────────────┴──────────────────┐
         ▼                         ▼                      ▼                  ▼
      HTTP/1.1                  HTTP/2                 HTTP/3              gRPC
    (Text Stream)           (Multiplex H2)          (QUIC Datagram)      (H2 Framing)
         │                         │                      │                  │
         ▼                         ▼                      ▼                  ▼
┌─────────────────┐       ┌─────────────────┐    ┌─────────────────┐ ┌───────────────┐
│   Http1Router   │       │   Http2Router   │    │   Http3Router   │ │  GrpcRouter   │
└────────┬────────┘       └────────┬────────┘    └────────┬────────┘ └───────┬───────┘
         │                         │                      │                  │
         └─────────────────────────┼──────────────────────┴──────────────────┘
                                   │
                                   ▼
                         Selected `Route`
                   (Upstream ID + Action Pipeline)
```

---

## 2. Two-Tier Routing Engine (Exact + Compressed Radix Trie)

For L7 HTTP protocols (HTTP/1.1, HTTP/2, HTTP/3), routes often combine fixed endpoints (`/login`, `/healthz`) with parameterized or nested prefixes (`/api/v1/*`, `/users/*`). 

Rather than relying on non-deterministic regular expressions (which suffer from exponential backtracking / ReDoS) or slow recursive path slicing, `velda-router` deploys a deterministic **Two-Tier Engine**:

```text
Incoming Path: "/api/v1/orders/12345/details"
                     │
                     ▼
       ┌───────────────────────────┐
       │   Tier 1: Exact Match     │ ───[HIT]───► Match Host & Method ──► Route Found
       │  (O(1) FxHashMap Lookup)  │
       └─────────────┬─────────────┘
                     │ [MISS]
                     ▼
       ┌───────────────────────────┐
       │   Tier 2: Prefix Match    │
       │   (Compressed Radix Trie) │ ───[HIT]───► Longest Prefix Match ──► Route Found
       │    O(P) Early Termination │
       └─────────────┬─────────────┘
                     │ [MISS]
                     ▼
         Deterministic None (404)
```

1. **Tier 1 — Exact Match ($O(1)$)**:
   - Direct hash lookup on exact path strings (`exact_path`) via Fibonacci hashing (`FxHashMap`).
   - Resolves in **~38 ns** on 5,000-route tables with **0 heap allocations**.
2. **Tier 2 — Compressed Radix Trie (Patricia Trie) ($O(P)$)**:
   - Custom in-house zero-allocation Trie ([`trie.rs`](src/l7/trie.rs)) caching edge `first_byte` for branch selection without pointer chasing.
   - Strictly $O(P)$ complexity (where $P$ is the matched prefix depth, not the full URI length $M$). Early-terminates as soon as an edge diverges.
   - Evaluates prefix matching in **~29 ns** with **0 heap allocations** across 100 to 5,000 routes.
   - Zero external automaton dependencies (`aho-corasick` eliminated).

---

## 3. Subsystem Breakdown

### 3.1 Layer 4 Routers (`TcpRouter`, `UdpRouter`)
- **Location**: [`src/l4/tcp.rs`](src/l4/tcp.rs), [`src/l4/udp.rs`](src/l4/udp.rs)
- **Scope**: L4 Raw TCP and UDP streams.
- **Data Model**: Dedicated, protocol-isolated `TcpRouter` and `UdpRouter` storing `TcpRoute` and `UdpRoute`.
- **Performance**: Direct lookup in **0.24 ns** (TCP) and **1.80 ns** (UDP).

### 3.2 Layer 7 HTTP Routers (`Http1Router`, `Http2Router`, `Http3Router`)
- **Location**: [`src/l7/http1.rs`](src/l7/http1.rs), [`src/l7/http2.rs`](src/l7/http2.rs), [`src/l7/http3.rs`](src/l7/http3.rs)
- **Scope**: HTTP/1.1 text streaming, HTTP/2 binary framing, and HTTP/3 QUIC datagram streams.
- **Request Views**: Zero-copy borrowed projections (`Http1RouteRequest<'a>`, `Http2RouteRequest<'a>`, `Http3RouteRequest<'a>`) holding `&'a str` references to `path`, `host`, and `method`.
- **Filter Rules**: Enforces URI path, optional `Host` header (case-insensitive with fast byte-scanned `clean_host`), and optional HTTP `Method`.

### 3.3 Layer 7 gRPC Router (`GrpcRouter`)
- **Location**: [`src/l7/grpc.rs`](src/l7/grpc.rs)
- **Scope**: Dedicated RPC routing over HTTP/2 framing.
- **Request View**: `GrpcRouteRequest<'a>` holding `service`, `method`, and optional `authority`.
- **Dispatch**: Direct hash map lookup on `service` name, followed by method and authority matching; falls back to `catch_all` wildcard services (`"*"`) if configured.
- **Performance**: Service dispatch in **10.94 ns**; zero-alloc URI slice parse & match in **52.13 ns**.

---

## 4. Invariants & Engineering Guarantees

1. **Hot-Path Zero-Allocation Invariant**:
   All route resolution functions (`route_tcp`, `route_udp`, `route_http1`, `route_http2`, `route_http3`, `route_grpc`) take borrowed request views and return `Option<&Route>`. **No `String`, `Vec`, or heap memory is allocated during lookup.**
2. **Deterministic Lookups & ReDoS Immunity**:
   No dynamic regex engines are used on the serving path. All prefix matching uses deterministic, non-backtracking Radix Trie traversal.
3. **Lock-Free Concurrency**:
   Compiled `Router` structures are immutable once built and safely shared across CPU threads via `Arc<Router>` or `ArcSwap<Runtime>`. Readers execute concurrent lookups with zero cross-core locking or cache contention.
4. **Clean Protocol Boundaries**:
   HTTP routes never match gRPC traffic, and gRPC routes never match HTTP paths. Cross-protocol targets are validated and rejected ahead-of-time during snapshot compilation.

---

## 5. Usage Example

```rust
use velda_core::{RouteId, UpstreamId};
use velda_router::{
    Http1Route, Http1RouteRequest, RouterBuilder, TcpRoute,
};

// 1. Build routes using the builder
let mut builder = RouterBuilder::default();

// Add an L4 TCP route
builder = builder.add_tcp_route(TcpRoute::new(
    RouteId::new(1),
    "listener-tcp-1",
    UpstreamId::new(10),
    "postgres_cluster",
));

// Add an L7 HTTP/1.1 prefix route
let http_route = Http1Route::new(
    RouteId::new(2),
    "listener-http-1",
    "/api/v1",
    UpstreamId::new(20),
    "api_service",
)
.with_host("api.velda.io")
.with_method("GET");

builder = builder.add_http1_route(http_route);

// 2. Compile into immutable in-memory Router
let router = builder.build().expect("Router compilation succeeded");

// 3. Hot-path zero-allocation lookup
let req = Http1RouteRequest::new("/api/v1/users/profile")
    .with_host("api.velda.io")
    .with_method("GET");

if let Some(route) = router.route_http1("listener-http-1", &req) {
    assert_eq!(route.upstream_name, "api_service");
}
```

---

## 6. Empirical Performance Summary

Empirical data extracted from production benchmarks on a 5,000-route, 1,000-upstream dataset:

| Target Scenario | Operation / Path | Latency / op | Allocs / op | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **L4 TCP Route** | `l4-in-0:tcp` | **0.24 ns** | **0.00** | **4,150.2 M ops/s** |
| **L4 UDP Route** | `l4-in-9:udp` | **1.80 ns** | **0.00** | **555.7 M ops/s** |
| **L7 HTTP Exact** | `/endpoints/action_0005/exec` | **38.52 ns** | **0.00** | **25.9 M ops/s** |
| **L7 HTTP Prefix** | `/api/v1/service_1000/orders/items/42` | **29.61 ns** | **0.00** | **33.7 M ops/s** |
| **L7 HTTP Miss** | `/unknown/unmatched/path/404` | **9.36 ns** | **0.00** | **106.8 M ops/s** |
| **L7 gRPC Exact** | `service.v1.Service_0007` | **10.94 ns** | **0.00** | **91.4 M ops/s** |
| **L7 gRPC Parse+Route** | `/service.v1.Service_0007/CreateOrder` | **52.13 ns** | **0.00** | **19.1 M ops/s** |
| **Multicore (256 Workers)**| Mixed Concurrent Traffic | **2.50 ns** | **0.00** | **399.3 M ops/s** |
