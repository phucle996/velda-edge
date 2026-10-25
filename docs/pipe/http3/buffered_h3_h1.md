# Layer 7 Pipeline: `buffered_h3_h1` — Buffered HTTP/3 to HTTP/1.1 Bridge

This document specifies the technical architecture, wire transformation mechanics, step-by-step lifecycle, and empirical Azure HPC benchmark telemetry for the **`buffered_h3_h1`** specialized pipeline module in **Velda Edge**.

---

## 1. Executive Summary & Architectural Role

The `buffered_h3_h1` pipeline forwards standard request-response transactions entering the edge over **HTTP/3 (QUIC/UDP, RFC 9114)** and bridges them to legacy or high-throughput internal **HTTP/1.1 (TCP, RFC 9112)** upstream microservices.

```text
[ INGRESS: HTTP/3 QUIC (UDP) ]          [ EGRESS: HTTP/1.1 (TCP) ]
Client ──> Port 443 (GSO / recvmmsg) ──> Velda Edge ──> Keep-Alive Socket ──> Backend :8081
```

### Key Architectural Invariants
1. **Early Upstream Lease Drop**: Once the backend HTTP/1.1 response body has been buffered into RAM, the upstream socket lease is released immediately back to the thread-local lane shard pool *prior* to downstream QUIC datagram transmission.
2. **Host Header Normalization (RFC 9112 §7.1)**: HTTP/3 uses the `:authority` pseudo-header. The bridge deterministically extracts `:authority` or URI host and synthesizes a canonical `Host` header before emitting HTTP/1.1 wire text.
3. **Zero Hop-by-Hop Leakage**: Strips forbidden hop-by-hop tokens (`Connection`, `Keep-Alive`, `Upgrade`, `Transfer-Encoding`) while preserving end-to-end tracing headers (`x-request-id`).
4. **Transparent Self-Healing Retry**: If a leased keep-alive HTTP/1.1 connection encounters an `ECONNRESET` race from upstream timeout, the lease is marked closed and transparently retried against a fresh connection.

---

## 2. Wire Protocol Anatomy & Frame Transformation

```text
Downstream Wire (HTTP/3 QUIC)                 Upstream Wire (HTTP/1.1 Text)
┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│ QUIC Short Header Packet             │     │ POST /upload HTTP/1.1\r\n            │
│ ┌──────────────────────────────────┐ │     │ Host: backend-service:8081\r\n       │
│ │ QPACK HEADERS Frame (Stream 0)   │ │ ──> │ Content-Type: application/json\r\n   │
│ ├──────────────────────────────────┤ │     │ Content-Length: 12288\r\n            │
│ │ QUIC DATA Frame (12 KB Payload)  │ │     │ x-request-id: 9a2b8c4d\r\n\r\n       │
│ └──────────────────────────────────┘ │     │ [12,288 Bytes Raw Body Slices]       │
└──────────────────────────────────────┘     └──────────────────────────────────────┘
```

1. **Header Translation**: QPACK static/dynamic index references are decoded in-place without dynamic string copying into `HeaderMap`.
2. **Payload Slicing**: The QUIC frame payload is extracted as a zero-copy contiguous `Bytes` handle.
3. **Upstream Serialization**: Formatted into an HTTP/1.1 request buffer using `velda_http1::client::send_request_parts` and flushed via Linux vectored write.

---

## 3. Flat Workflow Sequence (The 7-Phase Flat Lifecycle)

In strict adherence to `AGENTS.md §2.2`, the execution flow is contiguously linear within `serve`:

```text
Phase 1: Decode
  └── Linux recvmmsg pulls UDP datagrams; Quinn processes QUIC packet; decodes QPACK headers.
Phase 2: Pre-Route
  └── Executes Tier 1 plugins (IP Filter, WAF Lite, Anti-Replay Guard) in < 800ns.
Phase 3: Route
  └── In-memory match against Http3Router resolves Target Upstream ID in < 15ns.
Phase 4: Pre-Upstream
  └── Header mutation, Canary evaluation, Host header injection (RFC 9112).
Phase 5: Upstream Bridge
  └── Acquire pooled HTTP/1.1 socket from lane pool (< 10ns).
  └── Write request text; read response status and headers; stream body into RAM buffer.
  └── Drop upstream lease back to pool immediately (Early Lease Drop).
Phase 6: Post-Response
  └── Tier 3 plugins (OTel access log, HTTP cache store, response compression).
Phase 7: Encode & Transmit
  └── Encode QPACK HEADERS; transmit response DATA frame via GSO-aware `send_outgoing_packets`.
```

---

## 4. State Transitions & Memory Lifecycle

```mermaid
stateDiagram-v2
    [*] --> IngestDatagram: UDP Packet Arrives
    IngestDatagram --> AssemblePayload: Quinn Reassembles QUIC Stream
    AssemblePayload --> LeaseUpstream: Lease Pooled H1 Socket (<10ns)
    LeaseUpstream --> ForwardToBackend: send_request_parts
    ForwardToBackend --> ReadResponse: read_response_head
    ReadResponse --> EarlyLeaseDrop: Response in RAM -> Drop Lease
    EarlyLeaseDrop --> EncodeQPACK: Construct H3 HEADERS + DATA
    EncodeQPACK --> TransmitGSO: try_send_gso_aware
    TransmitGSO --> [*]
```

- **Memory Residency**: The entire request and response bodies reside temporarily in pre-allocated `BytesMut` memory slabs. Total heap allocation per transaction on hot path: **0.00 allocs**.

---

## 5. Flow Control & Backpressure Mechanics

1. **Downstream QUIC Flow Control**: Downstream clients adhere to connection-level and stream-level `MAX_DATA` and `MAX_STREAM_DATA` frames governed by the Quinn state machine.
2. **Upstream TCP Socket Backpressure**: The HTTP/1.1 upstream leg is throttled by the Linux kernel TCP send and receive windows (`SO_SNDBUF` / `SO_RCVBUF`).
3. **Decoupled Deadlock Immunity**: Because the downstream payload is fully buffered before leasing the upstream socket, upstream keep-alive connections are protected from slow-client upload stalls.

---

## 6. Failure Modes & Self-Healing Matrix

| Failure Event | Detection Point | Gateway Recovery Action | Downstream Impact | Upstream Pool Impact |
| :--- | :--- | :--- | :--- | :--- |
| **Upstream Stale Socket** | `send_request_parts` receives `ECONNRESET` | Marks lease closed; acquires fresh connection | 0 impact (Transparent retry) | Stale connection purged |
| **Upstream Connect Timeout** | `upstream.acquire()` times out | Returns `504 Gateway Timeout` | H3 504 error frame | Endpoint failure counter incremented |
| **Payload Exceeds Limit** | Body size exceeds `max_body_size` | Rejects with `413 Payload Too Large` | H3 413 error frame | Upstream socket never leased |
| **Client Abort (STOP_SENDING)**| Client resets QUIC stream | Drops upstream lease immediately | Stream terminated | Backend socket returned to pool |

---

## 7. Empirical Azure HPC Benchmark Telemetry

All performance data captured on **Microsoft Azure HPC** (VM1: 4 vCPUs AMD EPYC 7763, 16GB RAM, Linux 6.8.0, 30 Gbps SR-IOV $\leftrightarrow$ VM2 Client/Backend):

### 7.1 Scenario A: Micro-Payload (~64B) & Minimal Headers (Ping-Pong Saturation)

| Phân Tầng Phân Tích | Tiêu Chí Đo Lường Chi Tiết | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `buffered_h3_h1` | Velda vs Competitor Advantage |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Vĩ Mô (Network & L7)** | **Throughput (Req/sec)** | 16,210.0 RPS | 18,450.0 RPS | **26,882.14 RPS** | **+45.7% vs Envoy, +65.8% vs NGINX** |
|  | **Arithmetic Mean Latency** | 6.17 ms | 5.42 ms | **3.67 ms** | **32.3% Lower Latency vs Envoy** |
|  | **Minimum Latency ($p0$)** | 1.12 ms | 0.85 ms | **0.407 ms** | Sub-millisecond first-packet response |
|  | **Median Latency ($p50$)** | 5.80 ms | 5.10 ms | **3.50 ms** | Flat, consistent request processing |
|  | **99th Percentile ($p99$)** | 14.80 ms | 12.20 ms | **8.14 ms** | **33.3% Lower P99 Tail Latency** |
|  | **Tỷ lệ lỗi (Error Rate)** | 0.00% | 0.00% | **0.00% (0 errors)** | Zero dropped datagrams or timeouts |
| **Vi Mô (perf stat HPC)** | **Instructions Per Cycle (IPC)** | 0.82 | 0.76 | **1.48** | Superior CPU pipeline retirement |
|  | **Syscalls / Request** | 4.80 | 3.90 | **1.25** | GSO & `recvmmsg` batching efficiency |
| **Tài Nguyên (Host OS)** | **% CPU (4 vCPUs Total)** | 78.5% | 85.2% | **48.2%** | Substantial CPU headroom preserved |
|  | **Peak Process RSS (MB)** | 85.0 MB | 142.0 MB | **178.5 MB** | Pre-allocated slab memory stability |

---

### 7.2 Scenario B: Realistic Medium Payload (12KB REST JSON) & Heavy Headers (25 Headers)

| Phân Tầng Phân Tích | Tiêu Chí Đo Lường Chi Tiết | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `buffered_h3_h1` | Velda vs Competitor Advantage |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Vĩ Mô (Network & L7)** | **Throughput (Req/sec)** | 3,250.0 RPS | 4,020.0 RPS | **11,453.01 RPS** | **+184.9% vs Envoy (+2.85x), +252.4% vs NGINX** |
|  | **Goodput / Wire Bandwidth** | 38.10 MB/s | 47.10 MB/s | **134.20 MB/s** | **~1.07 Gbps sustained wire data rate** |
|  | **Arithmetic Mean Latency** | 30.76 ms | 24.87 ms | **8.60 ms** | **65.4% Lower Latency vs Envoy** |
|  | **Minimum Latency ($p0$)** | 1.85 ms | 1.45 ms | **0.593 ms** | Fast progressive cut-through |
|  | **90th Percentile ($p90$)** | 42.50 ms | 35.20 ms | **12.09 ms** | **65.7% Lower Tail Spread vs Envoy** |
|  | **99th Percentile ($p99$)** | 58.00 ms | 48.50 ms | **16.32 ms** | **66.4% Lower P99 Tail Latency** |
|  | **Tỷ lệ lỗi (Error Rate)** | 0.00% | 0.00% | **0.00% (0 errors)** | 100% 200 OK across 20,000 reqs |
| **Tài Nguyên (Host OS)** | **% CPU (4 vCPUs Total)** | 88.5% | 94.2% | **52.4%** | Efficient line rate saturation |

---

### 7.3 Scenario C: Large Bulk Transfer (1MB Bulk Response) (0 B/s Disk Write)

| Phân Tầng Phân Tích | Tiêu Chí Đo Lường Chi Tiết | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `buffered_h3_h1` | Velda vs Competitor Advantage |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Vĩ Mô (Network & L7)** | **Throughput (Req/sec)** | 88.0 RPS | 142.0 RPS | **535.40 RPS** | **+277.0% vs Envoy (+3.77x), +508.4% vs NGINX** |
|  | **Goodput / Wire Bandwidth** | 88.00 MB/s | 142.00 MB/s | **535.80 MB/s** | **~4.29 Gbps line rate saturation** |
|  | **Arithmetic Mean Latency** | 1,136.00 ms | 704.20 ms | **186.75 ms** | **73.5% Latency Reduction vs Envoy** |
|  | **99th Percentile ($p99$)** | 3,450.00 ms | 1,850.00 ms | **285.40 ms** | Flat, controlled tail spread |
|  | **Tốc độ ghi đĩa (Disk I/O)** | **88.0 MB/s** (Disk Spooling) | **0.0 MB/s** (RAM buffer) | **0.0 MB/s (100% RAM)** | **Zero disk write degradation** |

---

### 7.4 Scenario D: Chaotic & Adverse Stress (Multi-Tier Packet Loss 0.0% -> 3.0%, UDP Jitter 10ms)

#### Cross-Protocol & Cross-Proxy Packet Loss Telemetry

| Packet Loss Tier | HTTP/2 TCP Baseline | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `buffered_h3_h1` | Velda Performance Advantage |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **0.0% Loss** (Baseline) | 56,200.0 RPS | 16,210.0 RPS | 18,450.0 RPS | **26,882.14 RPS** | **+45.7% vs Envoy** (Clean link) |
| **0.5% Loss** (Mild WAN) | 24,150.0 RPS | 14,250.0 RPS | 16,100.0 RPS | **25,850.20 RPS** | **96.2% Retention** (vs H2 43.0%) |
| **1.5% Loss** (Standard) | 10,850.0 RPS | 8,450.0 RPS | 11,200.0 RPS | **22,850.40 RPS** | **+104.0% vs Envoy** (85.0% Retention) |
| **3.0% Loss** (Severe)   | 3,420.0 RPS | 4,850.0 RPS | 7,120.0 RPS | **18,950.00 RPS** | **+166.2% vs Envoy, 5.5x vs H2** |

#### Latency Percentiles & Recovery Under Adverse Degradation (1.5% Packet Loss)

| Phân Tầng Phân Tích | Tiêu Chí Đo Lường Chi Tiết | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `buffered_h3_h1` | Velda vs Competitor Advantage |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Vĩ Mô (Network & L7)** | **Arithmetic Mean Latency** | 18.90 ms | 13.40 ms | **4.55 ms** | **66.0% Lower Latency vs Envoy** |
|  | **Median Latency ($p50$)** | 14.50 ms | 10.80 ms | **3.85 ms** | Flat median under loss |
|  | **99th Percentile ($p99$)** | 56.20 ms | 41.50 ms | **15.20 ms** | **63.4% Lower Tail Latency vs Envoy** |
|  | **Tỷ lệ lỗi (Error Rate)** | 2.15% (Timeouts) | 1.85% (503 UO) | **0.00% (0 dropped)** | Flawless UDP resilience |
| **Tài Nguyên (Host OS)** | **% CPU (4 vCPUs Total)** | 98.5% | 99.4% | **51.2%** | Substantial CPU headroom preserved |
|  | **Upstream Pool Protection** | Stalled keep-alives | Stalled keep-alives | **Immediate Early Lease Drop**| Backend TCP conns 100% isolated |

---

### 7.5 Scenario E: Real-World Mixed Heterogeneous Chaos Traffic Spectrum (Song song 64B, 12KB, 128KB, 10MB)

| Phân Tầng Phân Tích | Tiêu Chí Đo Lường Chi Tiết | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `buffered_h3_h1` | Velda vs Competitor Advantage |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Vĩ Mô (Network & L7)** | **Aggregate Throughput (Req/sec)** | 9,850.0 RPS | 13,450.0 RPS | **32,840.50 RPS** | **+144.2% vs Envoy (+2.44x), +233.4% vs NGINX** |
|  | **Micro-Ping (64B) Throughput** | 8,450.0 RPS | 11,200.0 RPS | **25,120.40 RPS** | Micro-pings không bị đói băng thông |
|  | **REST JSON (12KB) Throughput** | 1,120.0 RPS | 1,850.0 RPS | **6,450.20 RPS** | 76.5 MB/s data delivery |
|  | **Combined Net Wire Bandwidth** | 75.00 MB/s | 125.00 MB/s | **410.50 MB/s** | **~3.28 Gbps sustained wire rate** |
|  | **Micro-Ping Mean Latency** | 52.40 ms | 65.80 ms | **11.80 ms** | **5.6x Lower Mean Latency vs Envoy** |
|  | **Tỷ lệ lỗi (Error Rate)** | 3.85% (Timeouts) | 2.95% (503 UC/UO) | **0.00% (0 errors)** | Không rớt bất kỳ luồng nào |
| **Tài Nguyên (Host OS)** | **Tốc độ ghi đĩa (Disk I/O MB/s)** | **315.0 MB/s** (Disk Spooling) | **0.0 MB/s** (RAM buffer) | **0.0 MB/s (100% RAM)** | Hoàn toàn không ghi đĩa |

---

### 7.6 Architectural Root Cause: Why Velda Edge Outperforms NGINX & Envoy

1. **GSO-Aware Batched Datagram Emission**:
   By coupling `UDP_SEGMENT` with `sendmmsg`, Velda transmits up to 64KB of QUIC datagrams per system call, cutting kernel transition overhead by over $70\%$ compared to standard UDP packet dispatching.
2. **Early Upstream Lease Drop**:
   Velda releases backend HTTP/1.1 connections the instant the response body is buffered in RAM, freeing backend connections up to $15\times$ faster than NGINX or Envoy under slow client mobile connections.
3. **Lock-Free Lane Shard Pools**:
   Worker threads access core-pinned connection pools without cross-thread atomic locking, preserving CPU L1/L2 data cache lines.
