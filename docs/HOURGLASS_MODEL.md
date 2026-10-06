# Velda Edge — The Hourglass Architecture (Mô hình Đồng Hồ Cát)

This document specifies the **North Star Mental Model**, design philosophy, and cognitive framework of **Velda Edge**. It provides contributors, architects, and AI agents with a clear mental model to reason about traffic flow, subsystem boundaries, and performance invariants.

---

## 1. Executive Summary & Core Philosophy

Traditional edge proxies and API gateways (Envoy, NGINX, Traefik, Kong) struggle with architectural tension between **feature breadth** and **execution latency**:
- As they support more protocols (HTTP/1, HTTP/2, HTTP/3, gRPC, TCP, UDP), their request handling pipelines become heavily fragmented into deep inheritance trees, polymorphic interfaces (`Box<dyn Filter>`, `HandlerChain`), dynamic type downcasting, and runtime sniffing.
- As routing requirements grow, filter chains and transformation layers bloat the central routing path, turning routing into an expensive bottleneck ("The Fat Waist").

Velda Edge solves this by adopting **The Hourglass Architecture (Mô hình Đồng Hồ Cát)**, inspired by the foundational architectural philosophy of the Internet Protocol Suite (RFC 3439), fused with two non-negotiable principles:
1. **Flat Workflows**: Zero artificial function chopping, zero nested inheritance chains.
2. **Strict Protocol Isolation**: Declared protocol over dynamic sniffing; parallel, disjoint execution tracks.

```text
               DOWNSTREAM INGRESS (Top Cone)
   [ HTTP/1.1 | HTTP/2 | HTTP/3 | gRPC TCP | gRPC UDP | L4 TCP/UDP ]
   ↳ Dedicated Listener Runners (pre-bound at cold start)
   ↳ Wire Codecs, Binary/Text Framing, Downstream TLS Handshakes
                  \                           /
                   \                         /
                    \                       /
                     ▼                     ▼
               ┌─────────────────────────────────┐
               │      THE NARROW WAIST (Eo)      │
               │   Pure In-Memory Static Route   │
               │  (Path/Host/Method → UpstreamId)│
               └─────────────────────────────────┘
                     ▲                     ▲
                    /                       \
                   /                         \
                  /                           \
               UPSTREAM EGRESS (Bottom Cone)
   [ HTTP/1 Pool | H2 Multiplex | H3 QUIC | gRPC TCP | gRPC UDP | L4 Pipe ]
   ↳ Direct Protocol Stream Pipe
   ↳ Autonomous Pools & Specialized Socket Acceleration
```

---

## 2. The Three Planes of the Hourglass

### 2.1 The Top Cone: Downstream Ingress (Broad & Diverse)
The top cone represents client-facing diversity. Clients connect using various transports and protocols: HTTP/1.1 text streaming, HTTP/2 binary multiplexing, HTTP/3 QUIC datagrams, gRPC length-prefixed streaming, or raw L4 TCP/UDP.

**Architectural Invariants:**
- **Pre-Bound Execution**: Every listener (`ListenerId`) is bound at cold-start / reconcile time to a specialized runner closure (`DedicatedTcpRunner` or `DedicatedUdpRunner`).
- **Zero Dynamic Sniffing**: When a TCP socket accepts or a UDP datagram arrives, bytes enter their dedicated pipeline directly. There is **no inspection of payload bytes**, **no checking of `Content-Type: application/grpc`**, and **no runtime enum branching** to guess the protocol.
- **Protocol-Specific Framing**: Downstream codecs operate exclusively within their dedicated crates (`velda-http1`, `velda-http2`, `velda-http3`, `velda-grpc`, `velda-transport`), handling TLS handshakes, ALPN verification, and framing without cross-subsystem leakage.

### 2.2 The Narrow Waist: The Router (Razor-Thin & Pure RAM)
Like the IP (Internet Protocol) layer in the Internet Architecture, the Router is the narrowest point of Velda Edge. Everything from the top cone converges to it, and everything to the bottom cone diverges from it.

**Architectural Invariants:**
- **Pure Logic Invariant**: The Router answers exactly one question in RAM:
  $$\text{Router}(\text{listener\_id}, \text{request\_head}) \longrightarrow \text{upstream\_id}$$
- **Zero Bloat ("No Fat Waist")**:
  - **Never** touches network sockets or performs I/O.
  - **Never** inspects request body payloads.
  - **Never** parses JSON, YAML, or configuration files.
  - **Never** performs dynamic trait downcasting (`Box<dyn Any>`).
  - **Zero Heap Allocations**: Route matching against pre-compiled trie/radix tables operates with zero heap allocations on the hot path.

### 2.3 The Bottom Cone: Upstream Egress (Specialized & Autonomous)
Once `upstream_id` is resolved at the waist, execution fans out into the bottom cone. Here, each upstream backend is an autonomous, pre-compiled engine specialized for its target protocol (`Http1Upstream`, `Http2Upstream`, `Http3Upstream`, `GrpcTcpUpstream`, `GrpcUdpUpstream`, `TcpUpstream`, `UdpUpstream`).

**Architectural Invariants:**
- **Dedicated Pool Isolation**: Pools are **never** shared globally. Every Upstream instance owns its private, dedicated lock-sharded connection pool or multiplexed active session table.
- **Independent Load Balancing & Health Tracking**: Each upstream owns its private `LbAlgorithm` state (RoundRobin counter, PeakEWMA latency tracker, LeastConn counters) and inline Circuit Breaker.
- **Direct Wire Piping**: Pipeline strategies (`buffered`, `server_stream`, `client_stream`, `duplex`) execute directly between downstream and upstream stream handles without intermediate buffering layers.

---

## 3. Resolving the Dilemma: Connection-Level vs. Request-Level

A fundamental design question often arises:
> *"If the protocol, TLS state, and upstream backend are already declared and known at startup, why not bind the Listener directly to the Upstream in a single static plane, eliminating the Router completely?"*

Velda Edge resolves this by strictly distinguishing between **Connection-Level Mechanics** and **Request-Level Semantics**:

### 3.1 At Connection Level (Downstream Ingress): 100% Static & Pre-Bound
At the transport connection level, there is indeed **no dynamic guessing**.
- In traditional gateways, a generic `AcceptLoop` accepts a connection, reads raw bytes, and runs dynamic protocol detection algorithms (sniffing HTTP headers, checking TLS ALPN dynamically, or inspecting payload prefixes).
- In Velda Edge, the binding from Listener to Pipeline is **fully static**:
  $$\text{Listener}(\text{port}, \text{proto}) \xrightarrow{\text{AOT pre-bound}} \text{DedicatedRunner}$$
  A gRPC listener *only* runs `handle_grpc_tcp_stream`. An HTTP/2 listener *only* runs `handle_http2_stream`. Zero overhead, zero dynamic sniffing.

### 3.2 At Request Level (The Waist): Dynamic Multiplexing Requires Routing
While the connection is static, modern edge protocols (HTTP/1.1 keep-alive, HTTP/2 multiplexing, HTTP/3 QUIC streams, gRPC multiplexing) allow **many requests to share a single connection**:
- Over a single persistent HTTP/2 downstream connection, Client Stream 1 may request `GET /api/v1/auth`, while concurrent Stream 3 requests `POST /api/v1/orders`, and Stream 5 requests `GET /static/logo.png`.
- These three requests target three completely different backend services (`auth-service`, `orders-service`, `cdn-storage`), each with its own load-balancing policy, timeout configuration, and connection pool.
- Therefore, binding a Listener statically to a single Upstream would destroy multiplexing and multi-route hosting. The **Router Waist** is fundamentally necessary to decouple downstream connection multiplexing from upstream destination targets.

### 3.3 The Synthesis
1. Downstream Connection Ingress $\to$ **Static Pre-Bound Plane** (Top Cone).
2. Request Head Dispatch $\to$ **In-Memory Narrow Waist** (The Router: $O(1)$ in RAM).
3. Upstream Stream Lease & Pipe $\to$ **Pre-Compiled Specialized Plane** (Bottom Cone).

---

## 4. The Golden Combo: Hourglass + Flat Workflow + Protocol Isolation

The Hourglass model achieves its full potential only when combined with **Flat Workflows** and **Strict Protocol Isolation**:

| Dimension | Anti-Pattern (Traditional Gateways) | The Velda Edge "Golden Combo" |
|---|---|---|
| **Routing Waist** | **The Fat Waist**: Filter chains, payload transformation, JSON decoding, auth RPCs all stuffed into the route handler. | **The Razor-Thin Waist**: Router strictly maps `(listener_id, req_head) -> upstream_id` in RAM. Zero I/O, zero payload inspection. |
| **Workflow Structure** | **The Shattered Cone**: Chopping request handling into dozens of OOP micro-classes (`BaseHandler`, `FilterManager`, `StreamAdapter`). | **Flat Workflow**: Request execution is contiguous and readable from Step 1 to the end in a single function per pipeline strategy. |
| **Protocol Handling** | **Dynamic Sniffing & Polyglot Bleed**: One giant pipeline checking `if is_grpc()`, fallback paths, shared global connection pools. | **Strict Protocol Isolation**: HTTP is HTTP, gRPC is gRPC. Independent routing tables, separate pipelines, isolated connection pools. |

---

## 5. Architectural Anti-Patterns Eliminated

### 5.1 The "Fat Waist" Anti-Pattern
- **Problem**: In monolithic gateways, as features accumulate, developers inject custom logic, body parsing, dynamic logging, and remote authentication checks directly into the routing phase. The waist thickens, turning a pure routing lookup into an unpredictable, high-latency bottleneck.
- **Velda Edge Solution**: The Router is forbidden from performing I/O, touching payloads, or executing external RPCs. Complex policies belong to pre-compiled plugins with bounded authority (`Action::Continue`, `Action::Respond`, `Action::Reject`).

### 5.2 The "Shattered Cone" Anti-Pattern
- **Problem**: Over-abstracted OOP frameworks create a maze of interfaces (`IStreamPipeProvider`, `ConnectionLeaseManagerFactory`, `AbstractFilterContext`). Tracing a single byte requires jumping through 15 files and inspecting runtime vtables.
- **Velda Edge Solution**: Pipeline execution modules (`buffered.rs`, `server_stream.rs`, `client_stream.rs`, `duplex.rs`) are completely self-contained. The workflow reads top-to-bottom:
  $$\text{accept} \longrightarrow \text{route} \longrightarrow \text{acquire lease} \longrightarrow \text{stream pipe} \longrightarrow \text{drop guard release}$$

### 5.3 The "Dynamic Sniffing" Anti-Pattern
- **Problem**: Listening on generic ports and sniffing `Content-Type: application/grpc` or inspecting initial packet bytes on every connection adds branch mispredictions, security vulnerabilities, and pipeline cross-contamination.
- **Velda Edge Solution**: Ingress protocol is explicitly declared in configuration. Listeners bind directly to dedicated protocol runners at cold start.

### 5.4 The "Monolithic Shared Pool" Anti-Pattern
- **Problem**: A global connection pool storing `Box<dyn AnyConnection>` causes lock contention across unrelated backends, complex garbage collection, and head-of-line blocking.
- **Velda Edge Solution**: Autonomous upstreams own their private, sharded connection pools (`SequentialLease` for HTTP/1.1, `MultiplexedPool` for HTTP/2/3/gRPC). Backend A can never block Backend B.

---

## 6. Contributor & AI Agent Mental Checklist

Before writing or modifying any code in `crates/`, ask these five questions:

1. **Which Cone does this logic belong to?**
   - Ingress mechanics (framing, decoding) $\to$ Top Cone (`velda-http1`, `velda-http2`, `velda-grpc`, `velda-transport`).
   - Routing decision $\to$ Narrow Waist (`velda-router`).
   - Upstream leasing & forwarding $\to$ Bottom Cone (`velda-upstream`, `pre_compile/upstream/`).
2. **Does this change make the Waist "fat"?**
   - If a proposed change in `velda-router` touches network I/O, parses JSON, allocates dynamic memory, or inspects payload bodies $\to$ **REJECT**.
3. **Does this change introduce protocol cross-contamination?**
   - If an HTTP pipeline inspects gRPC fields or routes through gRPC tables $\to$ **REJECT**.
4. **Is the workflow contiguous and readable top-to-bottom?**
   - If a sequential 30-line pipeline workflow was broken into 4 micro-helpers across 3 files just to reduce line count $\to$ **REJECT** (Keep flat, readable, and localized).
5. **Are connection pools and upstream instances isolated?**
   - Pools must be owned per-upstream. Never introduce a global mutable connection singleton.
