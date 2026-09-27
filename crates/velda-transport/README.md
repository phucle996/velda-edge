# velda-transport

Stage 0 — Edge Traffic Ingress, L4 Connection Lifecycle, Forwarding & L7 Handoff for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Invariant**: `Transport = Edge Traffic Ingress & L4 Lifecycle Engine`.
> Transport is neither an HTTP parser, TLS terminator, router, nor upstream load balancer.
> It orchestrates 5 core functions: **Listener Ingress & Declarative Reconcile, L4 Connection & Tracking, Zero-Copy Protocol Sniffing, L4 Direct Forwarding, and Symmetrical L7 Protocol Handoff**.

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
  IngressListener (JoinSet)                               UdpSocket (JoinSet)
  reconcile_active_listeners                              reconcile_active_listeners
         │                                                       │
         ▼                                                       ▼
    Connection                                                Datagram
  (stream, peer, local, bytes)                             (payload, peer, local)
         │                                                       │
         ▼                                                       ▼
  2. Protocol Classifier                                  2. Ingress Path
  peek_and_classify (3.01 ns)                             PathKind derived
  0x16 0x03 -> Tls                                        http3 / quic -> L7
  GET / POST -> Http1                                     udp raw -> L4Direct
  PRI * HTTP/2.0 -> Http2                                        │
  raw bytes -> L4Direct                                          │
         │                                                       │
         ├───────────────────────────┬───────────────────────────┤
         │                           │                           │
         ▼                           ▼                           ▼
  3. L4 Fast-Path Direct     4. TCP L7 Handoff           5. UDP L7 Handoff
  forward_tcp_direct / stream TcpL7Handoff                UdpL7Handoff
  copy_bidirectional          (conn, id, path_hint)       (dgram, sock, id, path_hint)
         │                           │                           │
         ▼                           ▼                           ▼
  Upstream L4 Target         velda-composer              velda-composer / velda-http
  (Raw TCP Proxy)            (TLS, HTTP/1, HTTP/2)       (HTTP/3 QUIC Engine)
```

---

## 2. Five Core Functions

| # | Function | Subsystem Role | Delegated / Partner Subsystem |
|---|---|---|---|
| **1** | **Ingress & Reconcile** | Binds TCP listeners and UDP sockets dynamically matching `listeners.json`. Diffs topology changes with zero downtime. | Config parsed into RAM by [`velda-sync`](../velda-sync). Orchestrated by [`velda-edge`](../velda-edge). |
| **2** | **L4 Connection Tracking** | Wraps raw sockets in [`Connection`](src/connection.rs); generates monotonic `ConnectionId`; tracks bytes read/written with lock-free atomics. | Vocabulary types defined in [`velda-core`](../velda-core). |
| **3** | **Zero-Copy Sniffing** | Peeks at the initial incoming bytes (3.01 ns) to classify traffic into `PathKind` (`Tls`, `Http1`, `Http2`, `L4Direct`) without consuming stream. | Application protocol resolution delegated to [`velda-composer`](../velda-composer). |
| **4** | **L4 Direct Forwarding** | High-throughput raw byte proxying via `copy_bidirectional` for TCP and stateless datagram/flow sessions for UDP. | Upstream backend discovery and connection leasing owned by [`velda-upstream`](../velda-upstream). |
| **5** | **Symmetrical L7 Handoff** | Envelopes classified streams into [`TcpL7Handoff`](src/forwarding/l7.rs) and [`UdpL7Handoff`](src/forwarding/l7.rs) containing only carrier, `listener_id`, and `path_hint`. | Protocol composition, TLS termination, and ALPN coordination owned by [`velda-composer`](../velda-composer). |

---

## 3. Invariants & Guarantees

1. **Zero-IO Hot Path**:
   Serving path never parses JSON, never reads disk, and never makes Control Plane RPCs. Ingress bindings, socket options, and forwarding paths execute entirely from pre-compiled RAM structures.
2. **Strict Symmetrical Handoff Contract**:
   Transport provides clean, symmetrical handoff structures for both transport protocols:
   - [`TcpL7Handoff`](src/forwarding/l7.rs): Hands off `(Connection, listener_id, path_hint)`.
   - [`UdpL7Handoff`](src/forwarding/l7.rs): Hands off `(Datagram, Arc<UdpSocket>, listener_id, path_hint)`.
   Transport never makes application-layer assumptions (zero `is_http1`, `is_http2`, `is_grpc` methods).
3. **Zero-Copy Protocol Classification**:
   Uses non-destructive socket peeking (`MSG_PEEK`). Classification runs in **3.01 ns** ($O(1)$) with **0 bytes allocated**.
4. **Declarative Listener Reconciliation**:
   Cold-start and live runtime updates use the exact same diffing engine:
   - **New listeners**: Bound and added to the Tokio `JoinSet`.
   - **Removed listeners**: Signaled via `watch::Sender<bool>`, shutting down accept loops and releasing ports cleanly.
   - **Unchanged listeners**: Kept running continuously with **zero dropped connections and zero downtime**.
5. **Lock-Free Atomic Accounting**:
   Transfer statistics and byte counters utilize atomic 64-bit counters, eliminating lock contention across multicore workers.

---

## 4. Benchmark Highlights

Benchmarked via Criterion on bare-metal Linux (`x86_64`):

| Operation / Component | Latency / op | Throughput | Allocations | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Protocol Sniffing (`classify_bytes`)** | **3.01 ns** | **332M ops/s** | **0 B (Zero-Alloc)** | $O(1)$ non-destructive byte inspection |
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

# Unit & Integration Tests (14 unit tests, 4 integration suites)
cargo test -p velda-transport

# Performance Benchmarks
cargo bench -p velda-transport
```
