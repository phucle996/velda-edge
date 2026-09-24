# velda-transport

`velda-transport` is the high-performance **Edge Traffic Engine** for Velda Edge, responsible for **traffic ingress, L4 connection lifecycle, TCP/UDP sockets, accept loops, zero-copy protocol classification, bidirectional byte streaming, and L7 protocol handoff**.

> [!IMPORTANT]
> **Hot-Path Invariant**: `velda-transport` operates strictly within the Data Plane serving hot path. It performs **zero JSON parsing, zero disk I/O, and zero Control Plane RPCs**. Ingress ports and protocol behaviors are driven entirely by compiled in-memory declarations passed from the composition root (`velda-edge`).

---

## 1. Core Architecture & Responsibilities

`velda-transport` serves as the foundational network transport layer in the monorepo:

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                              velda-edge                                │
 │   (Bootstrap / Composition Root / LKG Artifact Loader / IPC Receiver)  │
 └──────────────────────────────────┬─────────────────────────────────────┘
                                    │ IngressBinding (id, addr, proto, tls)
                                    ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │                           velda-transport                              │
 │                                                                        │
 │  ┌──────────────────────────────────────────────────────────────────┐  │
 │  │                      TrafficEngine (Multi-Port)                  │  │
 │  │  • JoinSet-managed concurrent TCP listeners & UDP sockets        │  │
 │  │  • Graceful shutdown via watch::Receiver<bool>                   │  │
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
 │  │   Protocol Classifier (peek)    │                 │  UDP Flow   │   │
 │  │  • HTTP/1.1  (GET, POST, etc.)  │                 │  Pipeline   │   │
 │  │  • HTTP/2    (PRI * HTTP/2.0)   │                 └─────────────┘   │
 │  │  • TLS       (0x16 0x03 Client) │                                   │
 │  │  • L4Direct  (Raw TCP stream)   │                                   │
 │  └──────────────┬──────────────────┘                                   │
 └─────────────────┼──────────────────────────────────────────────────────┘
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
 ┌───────────────┐   ┌───────────────┐
 │   L4 Direct   │   │   L7 Handoff  │
 │ Fast-Path     │   │ (velda-tls /  │
 │ (Byte Stream) │   │  velda-http)  │
 └───────────────┘   └───────────────┘
```

### Module Breakdown

| Module | Primary Responsibility |
|---|---|
| [`connection`](src/connection.rs) | L4 TCP connection lifecycle, monotonic ID generator, lock-free split halves (`ConnectionReader`, `ConnectionWriter`), and atomic byte counters (`bytes_read`, `bytes_written`). |
| [`tcp`](src/tcp/) | TCP listener abstractions, builder-based configuration (`TcpListenerConfig`), and zero-copy bidirectional byte forwarding (`forward_bidirectional`, `connect_and_forward`). |
| [`udp`](src/udp/) | High-throughput UDP datagram engine, atomic packet/byte accounting, lock-free concurrent socket sharing, and datagram buffer wrapping (`Datagram`). |
| [`ingress`](src/ingress/) | User configuration-driven bindings (`IngressBinding`, `IngressListener`) aligned with `listeners.json`, plus zero-copy protocol classification (`classify_bytes`, `peek_and_classify`). |
| [`forwarding`](src/forwarding/) | L4 fast-path direct stream forwarder (`l4.rs`) and L7 protocol handoff (`L7Handoff`) packaging connection state for `velda-tls` and `velda-http`. |
| [`engine`](src/engine.rs) | Central multi-port coordination engine (`TrafficEngine`) executing arbitrary TCP and UDP listeners concurrently with graceful shutdown propagation. |

---

## 2. Ingress Binding & Declarative Ports

`velda-transport` strictly enforces **declarative listener management**:
- Ports are configured purely by the user in `listeners.json`.
- There are **no hardcoded gateway default ports** (e.g. 80 or 443 are only bound if explicitly configured).
- Every declared `IngressBinding` is bound and managed directly without unnecessary enable/disable switches:

```rust
use std::net::SocketAddr;
use velda_transport::{IngressBinding, TrafficEngine};

let mut engine = TrafficEngine::new();

// 1. Declarative HTTP binding
let http_addr: SocketAddr = "0.0.0.0:80".parse().unwrap();
let http_binding = IngressBinding::new("http", http_addr, "http", false, None)?;
engine.add_binding(http_binding)?;

// 2. Declarative HTTPS binding with TLS profile
let https_addr: SocketAddr = "0.0.0.0:443".parse().unwrap();
let https_binding = IngressBinding::new(
    "https",
    https_addr,
    "http",
    true,
    Some("production_tls".into()),
)?;
engine.add_binding(https_binding)?;

// 3. Declarative UDP binding (e.g. DNS or Metrics)
let udp_addr: SocketAddr = "0.0.0.0:53".parse().unwrap();
let udp_binding = IngressBinding::new("dns", udp_addr, "udp", false, None)?;
engine.add_binding(udp_binding)?;
```

---

## 3. Protocol Sniffing & Classification

Before committing a connection to a specific subsystem, the ingress layer inspects initial bytes without consuming or allocating memory via `classifier::classify_bytes`:

| Traffic Signature | Detected Protocol | Dispatched Path |
|---|---|---|
| `GET `, `POST `, `HEAD `, `PUT `, `DELETE `, `OPTIONS `, `CONNECT `, `TRACE `, `PATCH ` | HTTP/1.x | `PathKind::Http` |
| `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` | HTTP/2 Connection Preface | `PathKind::Http` |
| `0x16 0x03` | TLS Handshake (SSL 3.0 / TLS 1.0 - 1.3) | `PathKind::Tls` |
| Any other byte pattern | Opaque Raw TCP Traffic | `PathKind::L4Direct` |

Classification executes in **3.01 ns** ($O(1)$) with zero heap allocations.

---

## 4. Zero-Allocation Byte Accounting & Splitting

The [`Connection`](src/connection.rs) struct wraps a `tokio::net::TcpStream` with transparent atomic byte tracking:

```rust
use velda_transport::Connection;

let (mut reader, mut writer) = connection.into_split();

// Reader and Writer operate concurrently across tasks without Mutex locks
tokio::spawn(async move {
    // reader records bytes_read into shared AtomicU64
});

tokio::spawn(async move {
    // writer records bytes_written into shared AtomicU64
});
```

Both halves share two `Arc<AtomicU64>` counters, providing exact I/O observability with zero lock contention.

---

## 5. Bidirectional Forwarding Engine (L4 TCP & UDP)

`velda-transport` provides dedicated, zero-copy L4 fast-path forwarding pipelines for both TCP streams and UDP datagram flows:

### 5.1 L4 TCP Fast-Path Streaming

For raw TCP proxying, [`forward_tcp_direct`](src/forwarding/l4.rs) and [`forward_bidirectional`](src/tcp/forward.rs) stream bytes directly between client and upstream sockets:

```rust
use velda_transport::{forward_tcp_direct, forward_bidirectional};

// Direct connection to target upstream address
let stats = forward_tcp_direct(client_conn, upstream_addr).await?;

println!(
    "TCP Stream transferred client->upstream: {} bytes, upstream->client: {} bytes",
    stats.client_to_upstream, stats.upstream_to_client
);
```

- Employs zero-copy buffer transfer (`tokio::io::copy_bidirectional`) with clean connection teardown.
- Propagates TCP half-closes (`shutdown(Write)`) correctly in both directions.
- Automatically tracks transferred bytes into the connection's atomic counters.

### 5.2 L4 UDP Datagram Forwarding & Session Proxying

For UDP traffic (e.g. DNS, Syslog, Gaming, QUIC/HTTP/3), `velda-transport` provides two distinct forwarding modes:

1. **Direct Datagram Forwarding (`forward_datagram`)**:
   Forwards an individual UDP packet directly to a target destination socket:
   ```rust
   use velda_transport::forward_datagram;

   let sent_bytes = forward_datagram(&udp_socket, payload, target_addr).await?;
   ```

2. **Bidirectional UDP Flow Session (`forward_udp_direct` / `forward_udp_flow`)**:
   Maintains a full proxy session between downstream client and upstream backend:
   ```rust
   use velda_transport::forward_udp_direct;

   // Binds an ephemeral upstream UDP socket, proxies initial payload,
   // routes upstream responses back to client_addr through downstream socket,
   // and tracks transfer statistics until shutdown.
   let stats = forward_udp_direct(
       downstream_socket,
       client_addr,
       upstream_addr,
       initial_payload,
       shutdown_rx,
   ).await?;

   println!(
       "UDP Flow transferred client->server: {} bytes, server->client: {} bytes (total: {} bytes)",
       stats.client_to_server_bytes, stats.server_to_client_bytes, stats.total_bytes()
   );
   ```

- Fully asynchronous with lock-free atomic byte tracking (`bytes_received`, `bytes_sent`).
- Maximum datagram size (64 KB) supported without payload truncation.
- Verified in [`tests/udp_test.rs`](tests/udp_test.rs) and benchmarked at **348,000 pps** with zero allocation overhead.

---

## 6. Micro-Benchmark Performance

Benchmarked via Criterion on bare-metal Linux (`x86_64`):

| Component / Workflow | Complexity | Throughput / Latency | Allocation Overhead |
|---|---|---|---|
| **Protocol Sniffing (`classify_bytes`)** | $O(1)$ | **3.01 ns** / probe | 0 bytes (zero-copy) |
| **TCP Full Ingress & Forwarding** | $O(1)$ | **1.89 GB/s** (1,894.2 MB/s) | 0 bytes per transfer |
| **UDP Ingest & Datagram Flow** | $O(1)$ | **348,000 pps** (2.87 µs / pkt) | 0 bytes per packet |
| **Monotonic ID Generator** | $O(1)$ | **1.22 ns** / ID | 0 bytes |

*Detailed benchmark methodology and reproduction steps are documented in [`benchmark.md`](benchmark.md).*

---

## 7. Development & Verification

Ensure code adheres to the project rules defined in `AGENTS.md`:

```bash
# Code formatting
cargo fmt --check

# Strict Clippy check with zero warnings
cargo clippy --all-targets --all-features -- -D warnings

# Run all 24 unit and integration tests
cargo test -p velda-transport

# Run Criterion micro-benchmarks
cargo bench -p velda-transport
```
