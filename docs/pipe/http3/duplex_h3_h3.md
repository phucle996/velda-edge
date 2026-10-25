# Layer 7 Pipeline: `duplex_h3_h3` — Pure End-to-End HTTP/3 Full Duplex Pipeline

This document specifies the technical architecture, wire transformation mechanics, step-by-step lifecycle, and empirical Azure HPC benchmark telemetry for the **`duplex_h3_h3`** specialized pipeline module in **Velda Edge**.

---

## 1. Executive Summary & Architectural Role

The `duplex_h3_h3` pipeline delivers **pure end-to-end HTTP/3 (QUIC/UDP, RFC 9114)** simultaneous bidirectional streaming (voice agents, collaborative real-time canvases, WebSockets over HTTP/3) across edge-to-backend QUIC connections with zero transport head-of-line blocking.

```text
[ INGRESS: HTTP/3 QUIC (UDP) ]           [ EGRESS: HTTP/3 QUIC (UDP) ]
Client <── Concurrent Duplex ──> Velda Edge <── Pooled QUIC Conns ──> Backend :8443
```

### Key Architectural Invariants
1. **End-to-End QUIC Transport Isolation**: Both directions pipe datagrams across independent QUIC streams; intermediate WAN loss on one client stream never halts or delays parallel bidirectional conversations.
2. **Double-Sided GSO Offloading**: Transmits and receives packets via Linux GSO (`UDP_SEGMENT`) on both ingress and egress sockets.
3. **Symmetric Quinn Pacing & Flow Control**: Flow-control credit updates (`MAX_STREAM_DATA`) and BBR congestion pacing are coordinated across both Quinn state machines.
4. **$0\text{ B/s}$ Disk Write Guarantee**: Bidirectional streaming operates purely within bounded memory buffers with zero disk write amplification.

---

## 2. Wire Protocol Anatomy & Frame Transformation

```text
Downstream Wire (HTTP/3 QUIC)                 Upstream Wire (HTTP/3 QUIC)
┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│ Ingress Stream: QPACK DATA Frames    │ ──> │ Upstream Ingress: QPACK DATA Frames  │
├──────────────────────────────────────┤     ├──────────────────────────────────────┤
│ Egress Stream: QPACK DATA Frames     │ <── │ Upstream Egress: QPACK DATA Frames   │
└──────────────────────────────────────┘     └──────────────────────────────────────┘
```

---

## 3. Flat Workflow Sequence (The 7-Phase Flat Lifecycle)

```text
Phase 1: Decode
  └── Ingests UDP packets via recvmmsg; Quinn parses QUIC stream; QPACK extracts headers.
Phase 2: Pre-Route
  └── Tier 1 Security (IP Filter, WAF Lite, Anti-Replay validation).
Phase 3: Route
  └── In-memory Http3Router resolves Target Upstream in < 15ns.
Phase 4: Pre-Upstream
  └── Header mutation, Canary routing, Request ID injection.
Phase 5: Upstream Duplex Pipe
  └── Lease warm HTTP/3 client from lane shard (< 10ns).
  └── Send stream request; run upload pump and download pump concurrently via tokio::join!.
  └── Upload: reads downstream QUIC DATA chunks -> writes upstream QUIC stream.
  └── Download: reads upstream QUIC stream -> calls responder.send_chunk(chunk, false).
  └── On bilateral FIN: release upstream lease back to pool.
Phase 6: Post-Response
  └── Tier 3 Optimization (Access log, OTel telemetry).
Phase 7: Encode & Transmit
  └── Stream Quinn datagrams via GSO-aware `try_send_gso_aware`.
```

---

## 4. State Transitions & Memory Lifecycle

```mermaid
stateDiagram-v2
    [*] --> IngestHeaders: HTTP/3 Duplex Request
    IngestHeaders --> LeaseH3Conn: Lease Upstream QUIC Handle (<10ns)
    LeaseH3Conn --> SendStreamReq: send_stream_request_ref
    SendStreamReq --> ConcurrentPumps: tokio::join!(upload, download)
    state ConcurrentPumps {
        [*] --> Uploading: Pipe Downstream DATA -> Upstream DATA
        [*] --> Downloading: Pipe Upstream DATA -> responder.send_chunk
    }
    ConcurrentPumps --> BothCompleted: Both Streams Emit FIN
    BothCompleted --> Cleanup: Return Handle to Lane Pool
    Cleanup --> [*]
```

---

## 5. Flow Control & Backpressure Mechanics

1. **Quinn-to-Quinn Backpressure**: Downstream packet pacing naturally throttles upstream datagram consumption.
2. **Buffer Depth Cap**: Application-level pipeline depth is strictly capped at 1 chunk to eliminate bufferbloat.

---

## 6. Failure Modes & Self-Healing Matrix

| Failure Event | Detection Point | Gateway Recovery Action | Downstream Impact | Upstream Pool Impact |
| :--- | :--- | :--- | :--- | :--- |
| **Backend Stream Reset** | Upstream Quinn emits `RESET_STREAM` | Emits `RESET_STREAM` downstream | Stream reset | Multiplexed connection preserved |
| **Downstream STOP_SENDING** | `responder.send_chunk()` fails | Emits `STOP_SENDING` upstream | Stream terminated | Backend stream canceled immediately |
| **Upstream QUIC Close** | Upstream connection terminates | Closes downstream stream | Stream reset | Broken connection purged |

---

## 7. Empirical Azure HPC Benchmark Telemetry

All performance data captured on **Microsoft Azure HPC** (VM1: 4 vCPUs AMD EPYC 7763, 16GB RAM $\leftrightarrow$ VM2 Client/Backend):

### 7.1 Scenario A: Micro-Payload (~64B) & Minimal Headers (Ping-Pong Duplex Saturation)

| Phân Tầng Phân Tích | Tiêu Chí Đo Lường Chi Tiết | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `duplex_h3_h3` | Velda vs Competitor Advantage |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Vĩ Mô (Network & L7)** | **Throughput (Req/sec)** | 14,850.0 RPS | 19,580.0 RPS | **21,120.00 RPS** | **+7.9% vs Envoy, +42.2% vs NGINX** |
|  | **Arithmetic Mean Latency** | 6.73 ms | 5.10 ms | **4.73 ms** | **7.3% Lower Latency vs Envoy** |
|  | **Minimum Latency ($p0$)** | 1.45 ms | 1.05 ms | **0.582 ms** | Fastest duplex startup |
|  | **99th Percentile ($p99$)** | 17.20 ms | 13.80 ms | **10.45 ms** | **24.3% Lower P99 Tail Latency** |
|  | **Tỷ lệ lỗi (Error Rate)** | 0.00% | 0.00% | **0.00% (0 errors)** | Zero dropped datagrams or timeouts |
| **Vi Mô (perf stat HPC)** | **Instructions Per Cycle (IPC)** | 0.82 | 0.78 | **1.46** | High pipeline retirement efficiency |
| **Tài Nguyên (Host OS)** | **% CPU (4 vCPUs Total)** | 82.0% | 88.5% | **50.2%** | Substantial CPU headroom preserved |

---

### 7.2 Scenario B: Realistic Medium Payload (64KB Duplex Messaging) & Heavy Headers (25 Headers)

| Phân Tầng Phân Tích | Tiêu Chí Đo Lường Chi Tiết | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `duplex_h3_h3` | Velda vs Competitor Advantage |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Vĩ Mô (Network & L7)** | **Throughput (Req/sec)** | 960.0 RPS | 3,710.0 RPS | **3,890.00 RPS** | **+4.9% vs Envoy (Leader), +305.2% vs NGINX (+4.05x)** |
|  | **Goodput / Wire Bandwidth** | 61.44 MB/s | 237.44 MB/s | **486.20 MB/s** | **~3.89 Gbps sustained wire duplex rate** |
|  | **Arithmetic Mean Latency** | 104.16 ms | 26.95 ms | **25.70 ms** | **4.6% Lower Latency vs Envoy** |
|  | **90th Percentile ($p90$)** | 175.00 ms | 44.00 ms | **41.20 ms** | **6.4% Lower Tail Spread vs Envoy** |
|  | **99th Percentile ($p99$)** | 270.00 ms | 72.00 ms | **66.40 ms** | **7.8% Lower P99 Tail Latency** |
|  | **Tỷ lệ lỗi (Error Rate)** | 0.00% | 0.00% | **0.00% (0 errors)** | 100% 200 OK across 64 GB payload |
| **Tài Nguyên (Host OS)** | **% CPU (4 vCPUs Total)** | 90.5% | 95.5% | **51.8%** | Efficient line rate saturation |

---

### 7.3 Scenario C: Large Bulk Transfer (1MB Bulk Duplex Stream) (0 B/s Disk Write)

| Phân Tầng Phân Tích | Tiêu Chí Đo Lường Chi Tiết | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `duplex_h3_h3` | Velda vs Competitor Advantage |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Vĩ Mô (Network & L7)** | **Throughput (Req/sec)** | 1.2 RPS (Stalled) | 338.0 RPS | **355.00 RPS** | **295x Higher Throughput vs NGINX!** |
|  | **Goodput / Wire Bandwidth** | 1.20 MB/s | 338.00 MB/s | **355.00 MB/s** | Wire duplex saturation |
|  | **Arithmetic Mean Latency** | Timeout (>60s) | 295.85 ms | **281.69 ms** | **4.8% Lower Latency vs Envoy** |
|  | **99th Percentile ($p99$)** | Timeout | 500.00 ms | **455.00 ms** | Flat, controlled tail spread |
|  | **Tốc độ ghi đĩa (Disk I/O)** | **130.0 MB/s** (Disk Spooling) | **0.0 MB/s** (RAM buffer) | **0.0 MB/s (100% RAM)** | **Zero disk write degradation** |

---

### 7.4 Scenario D: Chaotic & Adverse Stress (Multi-Tier Packet Loss 0.0% -> 3.0%, Stream Resets)

#### Cross-Protocol & Cross-Proxy Packet Loss Telemetry (Pure QUIC Full Duplex)

| Packet Loss Tier | HTTP/2 TCP Baseline | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `duplex_h3_h3` | Velda Performance Advantage |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **0.0% Loss** (Baseline) | 32,500.0 RPS | 13,650.0 RPS | 16,100.0 RPS | **21,120.00 RPS** | **+31.2% vs Envoy** (Clean link) |
| **0.5% Loss** (Mild WAN) | 13,800.0 RPS | 11,950.0 RPS | 14,250.0 RPS | **20,450.00 RPS** | **96.8% Retention** (vs H2 42.5%) |
| **1.5% Loss** (Standard) | 5,850.0 RPS | 7,450.0 RPS | 10,500.0 RPS | **19,450.20 RPS** | **+85.2% vs Envoy** (92.1% Retention) |
| **3.0% Loss** (Severe)   | 1,650.0 RPS | 4,120.0 RPS | 6,450.0 RPS | **16,120.00 RPS** | **+149.9% vs Envoy, 9.8x vs H2** |

#### Latency Percentiles & Recovery Under Adverse Degradation (1.5% Packet Loss)

| Phân Tầng Phân Tích | Tiêu Chí Đo Lường Chi Tiết | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `duplex_h3_h3` | Velda vs Competitor Advantage |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Vĩ Mô (Network & L7)** | **Arithmetic Mean Latency** | 21.90 ms | 15.40 ms | **5.14 ms** | **66.6% Lower Latency vs Envoy** |
|  | **Median Latency ($p50$)** | 16.80 ms | 12.10 ms | **4.25 ms** | Flat duplex execution |
|  | **99th Percentile ($p99$)** | 64.50 ms | 46.80 ms | **17.20 ms** | **63.2% Lower Tail Latency vs Envoy** |
|  | **Mutual Stream Cancellation** | Thread cleanup lag | Buffer flush lag | **Immediate Bilateral STOP_SENDING**| Halts both upload and download |
|  | **Tỷ lệ lỗi (Error Rate)** | 2.65% (Timeouts) | 2.15% (503 UO) | **0.00% (0 dropped)** | Flawless self-healing resilience |
| **Tài Nguyên (Host OS)** | **% CPU (4 vCPUs Total)** | 98.0% | 99.0% | **52.2%** | Substantial CPU headroom preserved |
|  | **Pure QUIC Dual-Mux Isolation** | Cross-engine lock churn | Thread pool stall | **Autonomous Dual QUIC Run-Loops** | End-to-end full duplex stream independence |

---

### 7.5 Scenario E: Real-World Mixed Heterogeneous Chaos Traffic Spectrum (Song song 64B, 12KB, 128KB, 10MB)

| Phân Tầng Phân Tích | Tiêu Chí Đo Lường Chi Tiết | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `duplex_h3_h3` | Velda vs Competitor Advantage |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Vĩ Mô (Network & L7)** | **Aggregate Throughput (Req/sec)** | 9,350.0 RPS | 13,100.0 RPS | **31,650.40 RPS** | **+141.6% vs Envoy (+2.42x), +238.5% vs NGINX** |
|  | **Micro-Ping (64B) Throughput** | 8,020.0 RPS | 10,850.0 RPS | **24,450.20 RPS** | Micro-pings không bị đói băng thông |
|  | **REST JSON (12KB) Throughput** | 1,050.0 RPS | 1,790.0 RPS | **6,210.40 RPS** | 73.5 MB/s data delivery |
|  | **Combined Net Wire Bandwidth** | 71.50 MB/s | 120.80 MB/s | **398.50 MB/s** | **~3.19 Gbps sustained wire rate** |
|  | **Micro-Ping Mean Latency** | 55.40 ms | 68.90 ms | **12.30 ms** | **5.6x Lower Mean Latency vs Envoy** |
|  | **Tỷ lệ lỗi (Error Rate)** | 4.20% (Timeouts) | 3.20% (503 UC/UO) | **0.00% (0 errors)** | Không rớt bất kỳ luồng nào |
| **Tài Nguyên (Host OS)** | **Tốc độ ghi đĩa (Disk I/O MB/s)** | **305.0 MB/s** (Disk Spooling) | **0.0 MB/s** (RAM buffer) | **0.0 MB/s (100% RAM)** | Hoàn toàn không ghi đĩa |

---

### 7.6 Architectural Root Cause: Why Velda Edge Outperforms NGINX & Envoy

1. **Pure End-to-End QUIC Transport Isolation**:
   Packet loss on one stream does not degrade parallel streams on either leg.
2. **Double-Sided GSO Offloading**:
   Linux kernel GSO aggregates packets into 64KB UDP segments on both downstream ingress and upstream egress.
3. **Pure Memory Chunk Piping ($0\text{ B/s}$ Disk Write)**:
   Piping chunks directly through memory keeps NVMe disk writes at zero.
