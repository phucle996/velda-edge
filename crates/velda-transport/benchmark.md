# Velda Transport — Benchmark Report & Performance Registry

> **Last Updated**: 2026-09-25  
> **Environment**: Rust 1.98 / Edition 2024 / Linux x86_64 (`x86_64-unknown-linux-gnu`)  
> **Profile**: `release` (`[profile.bench]`, `-O3`, LTO enabled)  
> **Measurement Harness**: High-precision monotonic timer + thread-safe counting global allocator (`CountingAllocator`).  
> **Source Files**: [`src/ingress/classifier.rs`](./src/ingress/classifier.rs), [`src/tcp/forward.rs`](./src/tcp/forward.rs), [`src/udp/socket.rs`](./src/udp/socket.rs), [`src/connection.rs`](./src/connection.rs)

---

## Executive Summary & Big-O Complexity Overview

| Subsystem / Operation | Workload / Scale | Latency / Throughput | Heap Allocs | Big-O Complexity | Architectural Invariant |
|---|---|---|---|:---:|---|
| **Protocol Sniffing** | `classify_bytes` (TLS, HTTP/1, H2, L4) | **2.94 ns** (340M ops/s) | **0 allocs** | $\mathbf{\mathcal{O}(1)}$ | **Zero-allocation hot-path byte sniffing** |
| **TCP Fast-Path Stream** | `forward_bidirectional` (20 MB) | **1.89 GB/s** (10.36 ms) | $\le 2$ allocs | $\mathbf{\mathcal{O}(N)}$ | Direct kernel TCP buffer streaming |
| **UDP Datagram Stream** | `send_to` / `recv_from` + accounting | **2.86 µs** (349K pps) | 1 alloc (buffer) | $\mathbf{\mathcal{O}(N)}$ | Lock-free atomic counter tracking |
| **Context Projections** | `ConnectionId` + `ConnectionContext` | **6.71 ns** (149M ops/s) | **0 allocs** | $\mathbf{\mathcal{O}(1)}$ | Zero-copy registers/stack projection |

---

## 1. Ingress Protocol Sniffing & Classification

[src/ingress/classifier.rs](./src/ingress/classifier.rs) inspects incoming socket buffers without consuming stream bytes using `TcpStream::peek`. It detects TLS Handshake (`0x16 0x03`), HTTP/2 Preface (`PRI * HTTP/2.0`), HTTP/1.1 methods (`GET`, `POST`...), or falls back to raw `L4Direct`.

### 1.1 Multi-Scale Classification Performance

| Scale (N) | Total Time | Latency / Op | Throughput (Ops / Sec) | Heap Allocs | Allocated Bytes | Big-O Complexity |
|---|---|---|---|---|---|:---:|
| 10 | 302 ns | 30.20 ns | 33,112,582 ops/s | 0 | 0 B | $\mathcal{O}(1)$ |
| 100 | 378 ns | 3.78 ns | 264,550,264 ops/s | 0 | 0 B | $\mathcal{O}(1)$ |
| 1,000 | 2.98 µs | 2.98 ns | 335,795,836 ops/s | 0 | 0 B | $\mathcal{O}(1)$ |
| 10,000 | 29.39 µs | 2.94 ns | 340,240,209 ops/s | 0 | 0 B | $\mathcal{O}(1)$ |
| 100,000 | 320.61 µs | 3.21 ns | 311,909,321 ops/s | 0 | 0 B | $\mathcal{O}(1)$ |

> [!NOTE]
> **Zero-Allocation Invariant**: Sniffing runs in **3.0 nanoseconds** per check across mixed TLS/HTTP/L4 payloads, achieving **>310,000,000 classifications per second** with strictly **0 heap allocations**.

#### Classification Big-O Linearity Drift

$$\text{Linearity Drift } (\%) = \left( \frac{\text{Growth Ratio}}{\text{Scale Ratio}} - 1 \right) \times 100\%$$

| Transition | Scale Ratio | Observed Growth | Linearity Drift % | Big-O Status |
|---|---|---|---|:---:|
| $10 \rightarrow 100$ | 10.0x | 1.25x | -87.5% | $\mathcal{O}(1)$ Constant per op (warm cache) |
| $100 \rightarrow 1,000$ | 10.0x | 7.88x | -21.2% | $\mathcal{O}(1)$ Constant per op |
| $1,000 \rightarrow 10,000$ | 10.0x | 9.86x | **-1.4%** | **Strict $\mathcal{O}(1)$ Constant Time [OK]** |
| $10,000 \rightarrow 100,000$ | 10.0x | 10.90x | **+9.0%** | **Strict $\mathcal{O}(1)$ Constant Time [OK]** |

---

## 2. TCP Fast-Path Bidirectional Stream Forwarding

[src/tcp/forward.rs](./src/tcp/forward.rs) streams raw bytes between client and upstream backends using `tokio::io::copy_bidirectional`. When an L4 route is selected, protocol parsing is completely bypassed.

### 2.1 Multi-Scale Stream Forwarding Performance

| Payload Size | Duration | Throughput | Heap Allocs | Allocated Bytes | Big-O Complexity |
|---|---|---|---|---|:---:|
| 64 KB | 140.01 µs | 446.38 MB/s | 1 | 32 B | $\mathcal{O}(N)$ |
| 256 KB | 215.79 µs | 1.13 GB/s | 1 | 32 B | $\mathcal{O}(N)$ |
| 1 MB | 620.30 µs | 1.57 GB/s | 1 | 32 B | $\mathcal{O}(N)$ |
| 5 MB | 3.00 ms | 1.63 GB/s | 1 | 32 B | $\mathcal{O}(N)$ |
| 20 MB | 10.36 ms | 1.89 GB/s | 2 | 64 B | $\mathcal{O}(N)$ |

#### TCP Forwarding Big-O Linearity Drift

| Transition | Scale Ratio | Observed Growth | Linearity Drift % | Big-O Status |
|---|---|---|---|:---:|
| $64\text{ KB} \rightarrow 256\text{ KB}$ | 4.0x | 1.54x | -61.5% | Sub-linear $\mathcal{O}(N)$ (Socket buffer fill) |
| $256\text{ KB} \rightarrow 1\text{ MB}$ | 4.0x | 2.87x | -28.3% | Throughput ramps to 1.57 GB/s |
| $1\text{ MB} \rightarrow 5\text{ MB}$ | 5.0x | 4.84x | **-3.2%** | **Strict $\mathcal{O}(N)$ Linear Streaming [OK]** |
| $5\text{ MB} \rightarrow 20\text{ MB}$ | 4.0x | 3.45x | **-13.8%** | **Near-linear $\mathcal{O}(N)$ (1.89 GB/s peak) [OK]** |

---

## 3. UDP Datagram Transmission & Atomic Accounting

[src/udp/socket.rs](./src/udp/socket.rs) and [src/udp/forward.rs](./src/udp/forward.rs) handle non-blocking UDP transmission, datagram routing, and concurrent lock-free atomic byte tracking.

### 3.1 Multi-Scale Datagram Throughput

| Packets (N) | Total Time | Latency / Packet | Throughput (PPS) | Heap Allocs | Big-O Complexity |
|---|---|---|---|---|:---:|
| 100 | 821.00 µs | 8,210.03 ns | 121,802 pps | 1 | $\mathcal{O}(N)$ |
| 1,000 | 2.94 ms | 2,941.27 ns | 339,989 pps | 1 | $\mathcal{O}(N)$ |
| 5,000 | 14.31 ms | 2,862.33 ns | 349,366 pps | 1 | $\mathcal{O}(N)$ |
| 10,000 | 28.69 ms | 2,869.43 ns | 348,501 pps | 1 | $\mathcal{O}(N)$ |

#### UDP Throughput Big-O Linearity Drift

| Transition | Scale Ratio | Observed Growth | Linearity Drift % | Big-O Status |
|---|---|---|---|:---:|
| $100 \rightarrow 1,000$ | 10.0x | 3.58x | -64.2% | Sub-linear (Tokio event loop warmup) |
| $1,000 \rightarrow 5,000$ | 5.0x | 4.87x | **-2.6%** | **Strict $\mathcal{O}(N)$ Packet Stream [OK]** |
| $5,000 \rightarrow 10,000$ | 2.0x | 2.00x | **+0.0%** | **Perfect $\mathcal{O}(N)$ Linear Scaling [OK]** |

---

## 4. Connection ID & Context Projections

[src/connection.rs](./src/connection.rs) allocates unique monotonic connection IDs and projects active connections into lightweight context descriptors ([`L4Request`](../velda-core/src/l4/request.rs) and [`ConnectionContext`](../velda-core/src/context.rs)).

### 4.1 Multi-Scale Lifecycle Performance

| Scale (N) | Total Duration | Latency / Op | Operations / Sec | Heap Allocs | Big-O Complexity |
|---|---|---|---|---|:---:|
| 1,000 | 6.59 µs | 6.59 ns | 151,699,029 ops/s | 0 | $\mathcal{O}(1)$ |
| 10,000 | 67.10 µs | 6.71 ns | 149,029,075 ops/s | 0 | $\mathcal{O}(1)$ |
| 100,000 | 678.32 µs | 6.78 ns | 147,424,131 ops/s | 0 | $\mathcal{O}(1)$ |

> [!NOTE]
> **Hot-Path Invariant**: Creating connection IDs and projecting `ConnectionContext` takes only **6.7 nanoseconds** per connection (**>147,000,000 ops/second**) with strictly **0 heap allocations**.

#### Context Lifecycle Big-O Linearity Drift

| Transition | Scale Ratio | Observed Growth | Linearity Drift % | Big-O Status |
|---|---|---|---|:---:|
| $1,000 \rightarrow 10,000$ | 10.0x | 10.18x | **+1.8%** | **Strict $\mathcal{O}(1)$ Constant Per Op [OK]** |
| $10,000 \rightarrow 100,000$ | 10.0x | 10.11x | **+1.1%** | **Strict $\mathcal{O}(1)$ Constant Per Op [OK]** |

---

## Verification & Reproducibility

To re-run the benchmark suite and reproduce these exact measurements:

```bash
cargo bench -p velda-transport
```
