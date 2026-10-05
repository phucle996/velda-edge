# velda-transport

Stage 0 — Edge Traffic Ingress, L4 Connection Lifecycle, Forwarding & L7 Handoff for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Invariant**: `Transport = Edge Traffic Ingress & L4 Lifecycle Engine`.
> Transport is neither an HTTP parser, TLS terminator, router, nor upstream load balancer.
> It orchestrates 5 core functions: **Declarative Listener Ingress & Reconcile, L4 Connection & Tracking, 2D Ingress Path Resolution, L4 Direct Forwarding, and Symmetrical L7 Protocol Handoff**.

---

## 1. Subsystem Architecture & Request Flow

```text
                             Client Ingress Traffic
                                (TCP / UDP)
                                     │
                                     ▼
                 ╔═══════════════════════════════════════╗
                 ║            velda-transport            ║
                 ║         (Edge Traffic Engine)         ║
                 ╚═══════════════════╤═══════════════════╝
                                     │
         ┌───────────────────────────┴───────────────────────────┐
         ▼                                                       ▼
  1. TCP Ingress Loop                                     1. UDP Ingress Loop
  TcpIngress (stateful, accept loop)                      UdpIngress (stateless, recv loop)
  reconcile_active_listeners                              reconcile_active_listeners
         │                                                       │
         ▼                                                       ▼
     Connection                                               Datagram
  (stream, peer, local, bytes)                             (payload, peer, local)
         │                                                       │
         ▼                                                       ▼
   tcp_handler(Connection)                    udp_handler(listener_id, socket, Datagram)
          │                                                       │
          ▼                                                       ▼
   velda-edge pipeline (L4 forward or L7 protocol engine, chosen per listener_id)
```

---

## 2. Five Core Functions

| # | Function | Subsystem Role | Delegated / Partner Subsystem |
|---|---|---|---|
| **1** | **Ingress & Reconcile** | Binds TCP listeners and UDP sockets dynamically matching `listeners.json`. Diffs topology changes with zero downtime. | Config parsed into RAM by [`velda-sync`](../velda-sync). Orchestrated by [`velda-edge`](../velda-edge). |
| **2** | **L4 Connection Tracking** | Wraps raw sockets in [`Connection`](src/connection.rs); generates monotonic `ConnectionId`; tracks bytes read/written with lock-free atomics. | Vocabulary types defined in [`velda-core`](../velda-core). |
| **3** | **Declared Ingress Binding** | `IngressBinding::from_transport` selects `Tcp` or `Udp` from `transport.protocol` at binding creation. L4 vs L7 is decided by `velda-edge`, not transport. Zero payload sniffing. | Protocol composition and backend mapping delegated to [`velda-composer`](../velda-composer). |
| **4** | **L4 Direct Forwarding** | High-throughput raw byte proxying via `copy_bidirectional` for TCP and stateless datagram/flow sessions for UDP. | Upstream backend discovery and connection leasing owned by [`velda-upstream`](../velda-upstream). |
| **5** | **Direct Handler Dispatch** | TCP handler receives `Connection` (tagged with `listener_id`); UDP handler receives `(listener_id, Arc<UdpSocket>, Datagram)`. No envelope types. | Protocol composition, TLS termination, and ALPN coordination owned by [`velda-composer`](../velda-composer). |

---

## 3. Invariants & Guarantees

1. **Zero-IO Hot Path**:
   Serving path never parses JSON, never reads disk, and never makes Control Plane RPCs. Ingress bindings, socket options, and forwarding paths execute entirely from pre-compiled RAM structures.
2. **No Handoff Layer**:
   Transport and the edge gateway share a process, so handlers are called directly with `Connection` or `(listener_id, socket, Datagram)`. Transport never makes application-layer assumptions; the edge pipeline resolves the engine solely via `listener_id`.
3. **Zero Payload Sniffing**:
   The protocol is declared per listener and resolved by the edge pipeline table (no pipeline $\to$ L4 forward). Zero byte sniffing or speculative prefetching on ingress.
4. **Declarative Listener Reconciliation**:
   Cold-start and live runtime updates use the exact same diffing engine:
   - **New listeners**: Bound and added to the Tokio `JoinSet`.
   - **Removed listeners**: Signaled via `watch::Sender<bool>`, shutting down accept loops and releasing ports cleanly.
   - **Unchanged listeners**: Kept running continuously with **zero dropped connections and zero downtime**.
5. **Lock-Free Atomic Accounting**:
   Transfer statistics and byte counters utilize atomic 64-bit counters, eliminating lock contention across multicore workers.

---

## 4. Benchmark Highlights

Benchmarked on bare-metal Linux (`x86_64`):

| Operation / Component | Latency / op | Throughput | Allocations | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Ingress Path Resolution (`from_transport`)** | **~1.2 ns** | **>800M ops/s** | **0 B (Zero-Alloc)** | $O(1)$ static branch resolution |
| **Monotonic ID Generator** | **1.22 ns** | **819M IDs/s** | **0 B (Zero-Alloc)** | Atomic fetch-add sequence |
| **TCP Ingress & Streaming** | **Streaming** | **1.89 GB/s** | **0 B per transfer** | Direct `tokio::io::copy_bidirectional` |
| **UDP Ingest & Datagram Flow** | **2.87 µs** | **348,000 pps** | **0 B per packet** | Lock-free atomic datagram relay |

---

## 5. Verification Commands

Run standard quality gate checks from the repository root:

```bash
# Code Style & Lints
cargo fmt --check -p velda-transport
cargo clippy -p velda-transport --all-targets --all-features -- -D warnings

# Unit & Integration Tests (10 unit tests, 5 integration suites)
cargo test -p velda-transport

# Performance Benchmarks
cargo bench -p velda-transport
```
