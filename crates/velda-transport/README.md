# velda-transport

`velda-transport` is the high-performance **Edge Traffic Engine** for Velda Edge, responsible for **traffic ingress, L4 connection lifecycle, TCP/UDP sockets, accept loops, zero-copy protocol classification, bidirectional byte streaming, and L7 protocol handoff**.

> [!IMPORTANT]
> **Hot-Path Invariant**: `velda-transport` operates strictly within the Data Plane serving hot path. It performs **zero JSON parsing, zero disk I/O, and zero Control Plane RPCs**. Ingress ports, load paths, and forwarding behaviors are driven entirely by compiled in-memory declarations passed from the composition root (`velda-edge`).

---

## 1. Core Architecture & Request Flow

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                              velda-edge                                │
 │   (Bootstrap / Composition Root / LKG Artifact Loader / IPC Receiver)  │
 └──────────────────────────────────┬─────────────────────────────────────┘
                                    │ EngineHandle::reconcile(desired_bindings)
                                    ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │                           velda-transport                              │
 │                                                                        │
 │  ┌──────────────────────────────────────────────────────────────────┐  │
 │  │                  engine::runner (TrafficEngine)                  │  │
 │  │  • JoinSet-managed concurrent TCP listeners & UDP sockets        │  │
 │  │  • Graceful shutdown propagation via watch::Receiver<bool>       │  │
 │  │  • Declarative topology reconciliation via engine::reconcile     │  │
 │  └───────────────────────────────┬──────────────────────────────────┘  │
 │                                  │                                     │
 │        ┌─────────────────────────┴─────────────────────────┐           │
 │        ▼                                                   ▼           │
 │  ┌─────────────┐                                     ┌─────────────┐   │
 │  │ TCP Ingress │                                     │ UDP Ingress │   │
 │  │ (listeners) │                                     │  (sockets)  │   │
 │  └──────┬──────┘                                     └──────┬──────┘   │
 │         │ Connection                                        │ Datagram │
 │         ▼                                                   ▼          │
 │  ┌─────────────────────────────────┐                 ┌─────────────┐   │
 │  │   Protocol Classifier (peek)    │                 │ Path Decider│   │
 │  │  • HTTP/1.1  (GET, POST, etc.)  │                 │ • Raw UDP   │   │
 │  │  • HTTP/2    (PRI * HTTP/2.0)   │                 │ • HTTP/3    │   │
 │  │  • TLS       (0x16 0x03 Client) │                 │   (QUIC)    │   │
 │  │  • L4Direct  (Raw TCP stream)   │                 │             │   │
 │  └──────┬──────────────────┬───────┘                 └─┬─────────┬─┘   │
 └─────────┼──────────────────┼───────────────────────────┼─────────┼─────┘
           │                  │                           │         │
           ▼                  │                           ▼         │
 ┌───────────────────┐        │                 ┌───────────────────┐
 │   L4 TCP Stream   │        │                 │    L4 UDP Flow    │
 │   Fast-Path       │        │                 │    Fast-Path      │
 │ (Byte Forwarding) │        │                 │(Datagram Sessions)│
 └───────────────────┘        ▼                 └───────────────────┘
                    ┌───────────────────┐                 ▼
                    │    TCP L7Handoff  │       ┌───────────────────┐
                    │   (velda-tls /    │       │   UdpL7Handoff    │
                    │    velda-http)    │       │   (velda-http /   │
                    └───────────────────┘       │    HTTP/3 QUIC)   │
                                                └───────────────────┘
```

---

## 2. Subsystem Structure

`velda-transport` is organized into clean, single-responsibility modules:

```text
crates/velda-transport/src/
├── connection.rs       # Connection lifecycle, monotonic IDs, lock-free split halves, atomic counters
├── error.rs            # Concrete transport error taxonomy and Result alias
├── forwarding/         # Fast-path byte forwarding and protocol handoff
│   ├── l4.rs           # Zero-copy L4 TCP bidirectional stream proxying
│   ├── l7.rs           # L7 handoff container (Connection + PathKind + metadata)
│   └── mod.rs          # Re-exports
├── ingress/            # Ingress bindings, configuration models, and classification
│   ├── binding.rs      # Declarative IngressBinding representation matching listeners.json
│   ├── classifier.rs   # Zero-copy protocol sniffing (HTTP/1, HTTP/2, TLS, L4 raw)
│   ├── listener.rs     # IngressListener and dedicated TCP accept loop
│   └── mod.rs          # Re-exports
├── tcp/                # TCP transport implementation
│   ├── config.rs       # Socket options (backlog, buffer sizes, nodelay, keepalive)
│   ├── forward.rs      # Bidirectional streaming and transfer statistics
│   ├── listener.rs     # Low-level TCP listener wrapping Tokio TcpListener
│   └── mod.rs          # Re-exports
├── udp/                # UDP datagram implementation
│   ├── config.rs       # UDP buffer and socket options
│   ├── datagram.rs     # Datagram wrapper with peer, local address, and payload
│   ├── forward.rs      # Datagram proxying and bidirectional UDP flow sessions
│   ├── socket.rs       # Shared UdpSocket with atomic byte counters and receive loop
│   └── mod.rs          # Re-exports
└── engine/             # Multi-port lifecycle coordination and reconciliation
    ├── handle.rs       # EngineHandle providing non-blocking asynchronous reconcile channel
    ├── reconcile.rs    # Topology diffing: opens new ports, closes obsolete ones, keeps identical live
    ├── runner.rs       # TrafficEngine: JoinSet orchestrator and event loop
    └── mod.rs          # Module declarations and re-exports (pure export layer)
```

---

## 3. Declarative Port Management & Reconciliation

Ingress configuration strictly adheres to a **declarative reconciliation pattern**:

- **No Hardcoded Defaults**: There are no implicit ports (e.g. 80 or 443 are only bound if explicitly declared in `listeners.json`).
- **Unified Cold-Start & Hot-Reload**: Cold-start initialization and live runtime reloads execute through the exact same declarative reconciliation pipeline.
- **Topology Diffing**:
  - **Removed / Modified Listeners**: The engine signals the running loop via `watch::Sender<bool>`, gracefully shutting down the accept/receive loop and releasing the OS port.
  - **New / Modified Listeners**: The engine binds the new TCP socket or UDP port and spawns its accept/receive loop into the active `JoinSet`.
  - **Identical Listeners**: Untouched and kept running with **zero connection drops and zero downtime**.

---

## 4. Zero-Copy Protocol Classification

Incoming TCP connections and UDP datagrams undergo non-destructive inspection via `peek_and_classify` / `classify_bytes` before dispatching:

| Protocol | Transport | Signature / Identification | Resolved Path | Dispatched Handling |
|---|---|---|---|---|
| **TCP** | TCP | Raw byte stream without TLS/HTTP headers | `PathKind::L4Direct` | L4 bidirectional byte proxy |
| **UDP** | UDP | Raw UDP datagrams (DNS, Syslog, custom) | `PathKind::L4Direct` | L4 datagram flow session proxy |
| **HTTP/1** | TCP | `GET `, `POST `, `PUT `, `DELETE `, `HEAD `, etc. | `PathKind::Http1` | `L7Handoff` -> `velda-http` (HTTP/1.1) |
| **HTTP/2** | TCP | `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` (H2C Preface) | `PathKind::Http2` | `L7Handoff` -> `velda-http` (HTTP/2) |
| **HTTP/3** | UDP | QUIC Initial / 0-RTT / 1-RTT datagrams | `PathKind::Http3` | `UdpL7Handoff` -> `velda-http` (HTTP/3) |
| **TLS** | TCP | `0x16 0x03` (TLS 1.0 - 1.3 ClientHello) | `PathKind::Tls` | `L7Handoff` -> `velda-tls` (TLS Engine) |

Classification completes in **3.01 ns** ($O(1)$) with zero heap allocations.

---

## 5. Forwarding & Handoff Models

### 1. L4 TCP Streaming (`forwarding::l4`, `tcp::forward`)
- Streams bytes directly between downstream client and upstream server using `tokio::io::copy_bidirectional`.
- Propagates TCP half-closes (`shutdown(Write)`) in both directions.
- Automatically records cumulative transfer metrics into `TransferStats` and connection atomic counters without locks.

### 2. L4 UDP Forwarding (`udp::forward`)
- **Direct Datagram Forwarding**: Statelessly relays single datagrams to target addresses.
- **Bidirectional UDP Flow Session**: Manages a stateful proxy session between client and upstream backend, routing responses back through downstream sockets with atomic packet and byte counters.

### 3. TCP L7 Handoff (`forwarding::l7::L7Handoff`)
- Hands off accepted connections to `velda-tls` (TLS termination) or `velda-http` (HTTP/1.1 or HTTP/2 codec and multiplexing) along with listener metadata and TLS profile.

### 4. UDP L7 Handoff (`forwarding::l7::UdpL7Handoff`)
- Hands off UDP datagrams and shared sockets to `velda-http` (HTTP/3 QUIC connection state engine) for zero-copy packet processing and bidirectional response dispatch.

---

## 6. Performance Characteristics

Benchmarked via Criterion on bare-metal Linux (`x86_64`):

| Component / Workflow | Complexity | Throughput / Latency | Allocation Overhead |
|---|---|---|---|
| **Protocol Sniffing (`classify_bytes`)** | $O(1)$ | **3.01 ns** / probe | 0 bytes (zero-copy) |
| **TCP Full Ingress & Forwarding** | $O(1)$ | **1.89 GB/s** (1,894.2 MB/s) | 0 bytes per transfer |
| **UDP Ingest & Datagram Flow** | $O(1)$ | **348,000 pps** (2.87 µs / pkt) | 0 bytes per packet |
| **Monotonic ID Generator** | $O(1)$ | **1.22 ns** / ID | 0 bytes |

---

## 7. Verification & Testing

```bash
# Code formatting
cargo fmt --check

# Strict Clippy check with zero warnings
cargo clippy -p velda-transport --all-targets --all-features -- -D warnings

# Run all unit and integration tests
cargo test -p velda-transport

# Run Criterion micro-benchmarks
cargo bench -p velda-transport
```
