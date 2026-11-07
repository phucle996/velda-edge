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

## 2. Strict Single Responsibility Principle (SRP) Module Layout

The codebase strictly enforces single responsibility across self-contained modules without arbitrary helper splitting:

| Module | Responsibility | Microarchitectural Optimization |
| :--- | :--- | :--- |
| [`src/method.rs`](src/method.rs) | HTTP Method bitmask parsing & matching | 1-cycle integer bitmask matching (`MethodMask`), parallel multi-route packing (`match_packed_methods`) across 64-bit & dual 32-bit registers. |
| [`src/host.rs`](src/host.rs) | Domain & authority matching | SWAR colon scanning (`swar_find_colon`), Reverse Block-by-Block tokenization (`ReverseHostLabels`, `HostLabelBlock`), Dual-64 / Quad-32 SWAR wildcard matching (`CompiledHost`). |
| [`src/path.rs`](src/path.rs) | URI path normalization & Prefix matching | SIMD vector path normalizer (`normalize_path`), Length-Guarded Dual-Chunk SWAR (`starts_with_prefix_swar`), flat cache-resident Slash-Block table (`PathPrefixMatcher` / `SlashBlockPrefixTable`) with **64-byte L1D cache-aligned nodes** (`SlashBlockNode`), hardware temporal prefetching (`prefetch_l1_keep`). |
| [`src/l7/http1.rs`](src/l7/http1.rs) | HTTP/1.1 protocol routing | Hot/Cold Data-Oriented Separation (`Http1HotCandidate`), 64-byte aligned [`ListenerHttp1Router`], 1-cycle method/host filter. |
| [`src/l7/http2.rs`](src/l7/http2.rs) | HTTP/2 protocol routing | Dedicated multiplexed H2 route table, Hot/Cold Separation (`Http2HotCandidate`), 64-byte aligned [`ListenerHttp2Router`]. |
| [`src/l7/http3.rs`](src/l7/http3.rs) | HTTP/3 QUIC protocol routing | Dedicated QUIC datagram route table, Hot/Cold Separation (`Http3HotCandidate`), 64-byte aligned [`ListenerHttp3Router`]. |
| [`src/l7/grpc.rs`](src/l7/grpc.rs) | gRPC RPC service/method routing | SWAR 1-cycle URI delimiter scan (`swar_find_first_delimiter`), Hot/Cold Separation (`GrpcHotCandidate`), 64-byte aligned [`ListenerGrpcRouter`]. |
| [`src/l4/tcp.rs`](src/l4/tcp.rs) | L4 Raw TCP port dispatch | Direct $\mathcal{O}(1)$ slot lookup in **0.24 ns**. |
| [`src/l4/udp.rs`](src/l4/udp.rs) | L4 Raw UDP port dispatch | Atomic contiguous endpoint rotation in **1.67 ns**. |

---

## 3. Two-Tier Routing Engine (Exact + Flat Slash-Block Prefix Table)

For L7 HTTP protocols (HTTP/1.1, HTTP/2, HTTP/3), routes combine fixed endpoints (`/login`, `/healthz`) with parameterized or nested prefixes (`/api/v1/*`, `/users/*`). 

Rather than relying on pointer-chasing tree structures or slow recursive path slicing, `velda-router` deploys a deterministic **Two-Tier Engine**:

```text
Incoming Path: "/api/v1/orders/12345/details"
                     │
                     ▼
       ┌───────────────────────────┐
       │   Tier 1: Exact Match     │ ───[HIT]───► 1-Cycle Method & Host Check ──► Route Found
       │  (O(1) FxHashMap Lookup)  │
       └─────────────┬─────────────┘
                     │ [MISS]
                     ▼
       ┌───────────────────────────┐
       │   Tier 2: Prefix Match    │
       │  (Flat Slash-Block Table) │ ───[HIT]───► Longest Prefix Match ──► Route Found
       │    O(P) Early Termination │
       └─────────────┬─────────────┘
                     │ [MISS]
                     ▼
         Deterministic None (404)
```

1. **Tier 1 — Exact Match ($O(1)$)**:
   - Direct hash lookup on exact path strings (`exact_path`) via Fibonacci hashing (`FxHashMap`).
   - Resolves in **~14.67 ns** on 5,000-route tables with **0 heap allocations**.
2. **Tier 2 — Flat Cache-Resident Slash-Block Prefix Table ($O(P)$)**:
   - Flat contiguous node arena ([`src/path.rs`](src/path.rs)) with **64-byte L1D cache-aligned nodes** (`SlashBlockNode`), eliminating linked tree pointers.
   - Evaluates prefix matching in **strictly $\mathcal{O}(P)$ complexity** (where $P$ is the matched prefix depth, not the full URI length $M$). Early-terminates as soon as an edge diverges.
   - Uses hardware-accelerated **Length-Guarded Dual-Chunk SWAR & SIMD** across all 4 register tiers (`Bits256`, `Bits128`, `Bits64`, `Bits32`), probed once at boot via [`global_hardware_topology()`].

---

## 4. Hot/Cold Data-Oriented Separation & L1D Cache Line Alignment

To maximize L1D cache residency and eliminate Pseudo-LRU (PLRU) cache line eviction:
1. **Separation of Concerns**:
   - Full route definitions (`Http1Route`, `Http2Route`, etc.) contain cold metadata (`listener_id: String`, `upstream_name: String`, `plugins: Vec<String>`) exceeding 200+ bytes.
   - They reside in a contiguous `routes: Vec<Route>` arena per listener and are **never scanned** during the search loop.
2. **Hot Candidate Matching**:
   - Hot search structures (`exact_routes`, `prefix_routes`) store compact, 16-byte-aligned [`Http1HotCandidate`] descriptors.
   - Route-to-request evaluation (`matches_req`) executes entirely within compact L1D lines. The cold route is dereferenced exactly **ONCE** when returning `Some(&Route)`.
3. **64-Byte Cache Line Alignment**:
   - `SlashBlockNode` and listener routers (`ListenerHttp1Router`, `ListenerGrpcRouter`) are declared `#[repr(C, align(64))]` to align strictly with CPU L1D cache lines, preventing split-line memory bus stalls.
4. **Hardware Temporal Prefetching**:
   - Hot lookup entry points execute `prefetch_l1_keep` (using `_mm_prefetch(..., _MM_HINT_T0)` on x86-64 and `prfm pldl1keep` on AArch64) to mark hot tables Most-Recently-Used in L1D cache.

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

Empirical data extracted from production benchmarks on a 5,000-route, 1,000-upstream dataset in RAM:

| Target Scenario | Operation / Path | Latency / op | Allocs / op | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **L4 TCP Route** | `l4-in-0:tcp` | **0.24 ns** | **0.00** | **4,100.8 M ops/s** |
| **L4 UDP Route** | `l4-in-9:udp` | **1.67 ns** | **0.00** | **598.2 M ops/s** |
| **L7 HTTP Exact** | `/endpoints/action_0005/exec` | **14.67 ns** | **0.00** | **68.2 M ops/s** |
| **L7 HTTP Prefix** | `/api/v1/service_1000/orders/items/42` | **31.87 ns** | **0.00** | **31.4 M ops/s** |
| **L7 HTTP Miss** | `/unknown/unmatched/path/404` | **9.20 ns** | **0.00** | **108.7 M ops/s** |
| **L7 gRPC Exact** | `service.v1.Service_0007` | **12.00 ns** | **0.00** | **83.4 M ops/s** |
| **L7 gRPC Parse+Route** | `/service.v1.Service_0007/CreateOrder` | **15.92 ns** | **0.00** | **62.8 M ops/s** |
| **Multicore Peak (16 Workers)**| Mixed Concurrent Traffic | **4.06 ns** | **0.00** | **246.3 M ops/s** |
| **Multicore Peak (64 Workers)**| Mixed Concurrent Traffic | **3.92 ns** | **0.00** | **254.9 M ops/s** |
