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
                      ▼                                   │
             ┌─────────────────┐                          │
             │    L4Router     │                          │
             │   (O(1) Slot)   │                          │
             └────────┬────────┘                          │
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

## 2. Two-Tier Routing Engine (Exact + Aho-Corasick)

For L7 HTTP protocols (HTTP/1.1, HTTP/2, HTTP/3), routes often combine fixed endpoints (`/login`, `/healthz`) with parameterized or nested prefixes (`/api/v1/*`, `/users/*`). 

Rather than relying on non-deterministic regular expressions (which suffer from exponential backtracking / ReDoS) or slow recursive path slicing, `velda-router` deploys a deterministic **Two-Tier Engine**:

```text
Incoming Path: "/api/v1/orders/12345/details"
                     │
                     ▼
       ┌───────────────────────────┐
       │   Tier 1: Exact Match     │ ───[HIT]───► Match Host & Method ──► Route Found
       │  (O(1) Hash Map Lookup)   │
       └─────────────┬─────────────┘
                     │ [MISS]
                     ▼
       ┌───────────────────────────┐
       │   Tier 2: Prefix Match    │
       │   (Aho-Corasick Automaton)│ ───[HIT]───► Longest Prefix Match ──► Route Found
       │    O(M) URI Length Scan   │
       └─────────────┬─────────────┘
                     │ [MISS]
                     ▼
         Deterministic None (404)
```

1. **Tier 1 — Exact Match ($O(1)$)**:
   - Direct hash lookup on exact path strings (`exact_path`).
   - Resolves in **~65 ns** on 5,000-route tables with **0 heap allocations**.
2. **Tier 2 — Anchored Aho-Corasick Prefix Match ($O(M)$)**:
   - Compiled finite-state automaton ([`AhoCorasick`](https://docs.rs/aho-corasick)) configured with `LeftmostLongest` match semantics and `Anchored::Yes`.
   - Scans the URI string **exactly once** in $O(M)$ time where $M$ is the path length in bytes, completely independent of whether the table contains 100 or 5,000 prefixes.
   - Evaluates prefixes in **~86 ns** with **0 heap allocations**.

---

## 3. Subsystem Breakdown

### 3.1 Layer 4 Router (`L4Router`)
- **Location**: [`src/l4/`](src/l4/)
- **Scope**: L4 Raw TCP and UDP streams.
- **Data Model**: `ListenerL4Router` holds dedicated `Option<L4Route>` slots for `Tcp` and `Udp`.
- **Performance**: Direct slot lookup in **15.07 ns** (66.4M ops/s).

### 3.2 Layer 7 HTTP Routers (`Http1Router`, `Http2Router`, `Http3Router`)
- **Location**: [`src/l7/http1.rs`](src/l7/http1.rs), [`src/l7/http2.rs`](src/l7/http2.rs), [`src/l7/http3.rs`](src/l7/http3.rs)
- **Scope**: HTTP/1.1 text streaming, HTTP/2 binary framing, and HTTP/3 QUIC datagram streams.
- **Request Views**: Zero-copy borrowed projections (`Http1RouteRequest<'a>`, `Http2RouteRequest<'a>`, `Http3RouteRequest<'a>`) holding `&'a str` references to `path`, `host`, and `method`.
- **Filter Rules**: Enforces URI path, optional `Host` header (case-insensitive), and optional HTTP `Method`.

### 3.3 Layer 7 gRPC Router (`GrpcRouter`)
- **Location**: [`src/l7/grpc.rs`](src/l7/grpc.rs)
- **Scope**: Dedicated RPC routing over HTTP/2 framing.
- **Request View**: `GrpcRouteRequest<'a>` holding `service`, `method`, and optional `authority`.
- **Dispatch**: Direct hash map lookup on `service` name, followed by method and authority matching; falls back to `catch_all` wildcard services (`"*"`) if configured.
- **Performance**: Service dispatch in **41.45 ns**; zero-alloc URI slice parse & match in **51.63 ns** (24.1M ops/s).

---

## 4. Invariants & Engineering Guarantees

1. **Hot-Path Zero-Allocation Invariant**:
   All route resolution functions (`route_l4`, `route_http1`, `route_http2`, `route_http3`, `route_grpc`) take borrowed request views and return `Option<&Route>`. **No `String`, `Vec`, or heap memory is allocated during lookup.**
2. **Deterministic Lookups & ReDoS Immunity**:
   No backtracking regex engines (`regex` crate with dynamic NFA/DFA) are used on the serving path. All prefix matching uses anchored Aho-Corasick.
3. **Lock-Free Concurrency**:
   Compiled `Router` structures are immutable once built and safely shared across CPU threads via `Arc<Router>` or `ArcSwap<Runtime>`. Readers execute concurrent lookups with zero cross-core locking or cache contention.
4. **Clean Protocol Boundaries**:
   HTTP routes never match gRPC traffic, and gRPC routes never match HTTP paths. Cross-protocol targets are validated and rejected ahead-of-time during snapshot compilation.

---

## 5. Usage Example

```rust
use velda_core::{RouteId, TransportProtocol, UpstreamId};
use velda_router::{
    Http1Route, Http1RouteRequest, L4Route, RouterBuilder,
};

// 1. Build routes using the builder
let mut builder = RouterBuilder::default();

// Add an L4 route
builder.add_l4(L4Route::new(
    RouteId::new("l4-rule-1"),
    "listener-tcp-1",
    TransportProtocol::Tcp,
    UpstreamId::new("postgres-backend"),
    "postgres_cluster",
));

// Add an L7 HTTP/1.1 prefix route
let http_route = Http1Route::new(
    RouteId::new("http-rule-1"),
    "listener-http-1",
    "/api/v1",
    UpstreamId::new("api-backend"),
    "api_service",
)
.with_host("api.velda.io")
.with_method("GET");

builder.add_http1(http_route);

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

Empirical data extracted from [`benchmark.md`](benchmark.md) on a 5,000-route, 1,000-upstream production dataset:

| Target Scenario | Operation / Path | Latency / op | Allocs / op | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **L4 TCP Route** | `listener:tcp` | **15.07 ns** | **0.00** | **66.4 M ops/s** |
| **L4 UDP Route** | `listener:udp` | **24.58 ns** | **0.00** | **40.7 M ops/s** |
| **L7 HTTP Exact** | `/endpoints/action_0005/exec` | **65.70 ns** | **0.00** | **15.2 M ops/s** |
| **L7 HTTP Prefix** | `/api/v1/service_1000/orders/items/42` | **86.73 ns** | **0.00** | **11.5 M ops/s** |
| **L7 HTTP Miss** | `/unmatched/path/404` | **45.04 ns** | **0.00** | **22.2 M ops/s** |
| **L7 gRPC Exact** | `service.v1.Service_0007` | **41.45 ns** | **0.00** | **24.1 M ops/s** |
| **Multicore (128 Workers)**| Concurrent mixed traffic | **9.66 ns** | **0.00** | **103.5 M ops/s** |
