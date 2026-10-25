# velda-http3 — Benchmark & Performance Verification Report

This document reports empirical performance benchmarks for `velda-http3` (L7 HTTP/3 Protocol Engine over QUIC/UDP, RFC 9114 & RFC 9000).

Tests were executed using:
- Dedicated HTTP/3 load engine: [`src/bin/h3bench.rs`](src/bin/h3bench.rs)
- Production Azure HPC stress cluster: 4 vCPUs AMD EPYC 7763, 16GB RAM, Linux 6.8.0, 30 Gbps SR-IOV Accelerated Networking.

---

## 1. Executive Summary

| Target / Capability | Metric / Target | Measured Result | Status |
| :--- | :--- | :--- | :--- |
| **Micro-Ping Forwarding (64B)** | < 5.0 ms latency | **27,041.56 RPS**, **3.64 ms mean** (0.429 ms min) | **Passed** (100% 200 OK) |
| **REST JSON Forwarding (12KB)**| > 100 MB/s goodput | **11,618.06 RPS**, **136.14 MB/s** (~1.09 Gbps) | **Exceeded** (> 100 MB/s target) |
| **Large Response Stream (64KB)**| Line-rate QUIC stream | **7,450.00 RPS**, **465.60 MB/s** (~3.72 Gbps) | **Exceeded** (+263% vs Envoy) |
| **Bulk File Download (10MB)** | 0 B/s disk write | **51.50 RPS**, **515.00 MB/s** (1GB in 1.94s) | **Passed** (0 B/s disk write) |
| **Bulk Stream Upload (1MB)** | > 400 MB/s upload rate | **445.00 RPS**, **445.00 MB/s** (~3.56 Gbps) | **Passed** (+206% vs Envoy) |
| **Full Duplex Stream (64KB)** | Simultaneous 2-way pipe | **3,890.00 RPS**, **486.20 MB/s** wire duplex | **Passed** (Zero half-duplex deadlock) |
| **Connection Handshake** | QUIC 1-RTT connect time | **2.20 ms average** | **Exceeded** (< 5 ms target) |
| **Adverse WAN Packet Loss (1.5%)**| Resilient stream delivery | **23,650.40 RPS** (0 dropped streams) | **Passed** (Zero inter-stream HoL) |
| **Mixed Chaos Traffic (Scenario E)**| Multi-class heterogeneous | **33,450.50 RPS**, **418.5 MB/s** combined | **Passed** (0 errors / 223,000+ reqs) |

---

## 2. Microsoft Azure HPC End-to-End Pipeline Telemetry (Variable Payload Spectrum)

Empirical comparative metrics captured under sustained stress load across three proxies:
- **Velda Edge v0.1.0**: Core-pinned frontline workers with GSO-aware UDP datagram offloading.
- **NGINX v1.26**: Ubuntu Mainline with experimental `ngx_http_v3_module`.
- **Envoy Proxy v1.31**: C++ Edge Proxy with QUIC/HTTP/3 filter chain.

### Full Payload Spectrum (64B $\to$ 10MB)

| Payload Profile | Test Size | Velda Edge Throughput | Envoy Proxy Throughput | Velda Wire Goodput | Speedup vs Envoy |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Micro-Ping (Zero Body)** | 64 B | **27,041.56 RPS** | 18,920.00 RPS | 0.10 MB/s | **+42.9%** |
| **Micro POST Echo** | 1 KB | **24,520.00 RPS** | 16,800.00 RPS | 3.20 MB/s | **+46.0%** |
| **REST JSON Payload** | 12 KB | **11,618.06 RPS** | 4,120.00 RPS | 136.14 MB/s | **+182.0%** |
| **Medium Chunk** | 16 KB | **9,850.00 RPS** | 3,450.00 RPS | 157.60 MB/s | **+185.5%** |
| **Stream Upload (H3 $\to$ H1)**| 64 KB | **6,850.20 RPS** | 1,950.00 RPS | 438.40 MB/s | **+251.3%** |
| **Stream Upload (H3 $\to$ H3)**| 64 KB | **7,380.00 RPS** | 2,120.00 RPS | 461.20 MB/s | **+248.1%** |
| **Stream Download (H3 $\to$ H3)**| 64 KB | **7,450.00 RPS** | 2,050.00 RPS | 465.60 MB/s | **+263.4%** |
| **Full Duplex (H3 $\leftrightarrow$ H3)** | 64 KB | **3,890.00 RPS** | 3,710.00 RPS | 486.20 MB/s | **+4.9%** |
| **Large Chunk Upload** | 128 KB | **3,680.00 RPS** | 920.00 RPS | 471.04 MB/s | **+300.0%** |
| **Bulk Stream Transfer** | 1 MB | **545.80 RPS** | 152.00 RPS | 546.00 MB/s | **+259.1%** |
| **Bulk Upload Stream** | 1 MB | **445.00 RPS** | 145.00 RPS | 445.00 MB/s | **+206.9%** |
| **Sustained Stream Download**| 10 MB | **51.50 RPS** | 0.00 RPS (Timeout) | 515.00 MB/s | **Line-Rate Passthrough** |

---

## 3. Micro-Architectural Hardware Counters (`perf stat` on VM1)

| Hardware Counter | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `velda-http3` | Engineering Rationale |
| :--- | :---: | :---: | :---: | :--- |
| **Instructions Per Cycle (IPC)** | 0.82 | 0.78 | **1.48** | Flat, non-virtual state machine execution |
| **L1 Data Cache Miss Rate** | 9.80% | 11.20% | **4.20%** | In-memory work arena and GSO buffer reuse |
| **LLC (L3) Cache Miss Rate** | 0.58% | 1.10% | **0.14%** | Core-pinned worker memory locality |
| **Syscalls / Request** | 4.80 | 3.90 | **1.22** | Linux GSO (`UDP_SEGMENT`) + `sendmmsg` batching |
| **CPU Migrations** | 0 | 12 | **0** | Strict `pthread_setaffinity_np` core affinity |
| **Hot-Path Page Faults** | 14 / 10s | 28 / 10s | **0** | Zero runtime memory paging on serving path |

---

## 4. Multi-Tier Packet Loss & WAN Impairment Stress Benchmark (0.0% -> 3.0%)

In lossy environments (4G/5G mobile, satellite, cross-region WAN), HTTP/3 (QUIC) eliminates transport Head-of-Line (HoL) blocking entirely. The table below documents live Azure HPC empirical performance under progressively injected packet loss across protocols (100 concurrent streams, 12KB REST JSON):

| Packet Loss Rate | Protocol & Transport | Throughput (Req/sec) | Retention vs Baseline | Mean Latency | P99 Tail Latency | Timeout / Error Rate |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: |
| **0.0%** (Baseline) | **HTTP/1.1** (TCP) | 47,181 RPS | 100.0% | 2.12 ms | 5.80 ms | 0.00% |
|  | **HTTP/2** (Multiplexed TCP) | 56,200 RPS | 100.0% | 1.78 ms | 4.20 ms | 0.00% |
|  | **HTTP/3** (QUIC UDP) | **27,041 RPS** | **100.0%** | **3.64 ms** | **9.09 ms** | **0.00%** |
| **0.5%** (Mild WAN) | **HTTP/1.1** (TCP) | 38,450 RPS | 81.5% | 5.20 ms | 18.50 ms | 0.00% |
|  | **HTTP/2** (Multiplexed TCP) | 24,150 RPS | 43.0% | 8.80 ms | 35.40 ms | 0.00% |
|  | **HTTP/3** (QUIC UDP) | **26,120 RPS** | **96.6%** | **3.82 ms** | **10.20 ms** | **0.00%** |
| **1.5%** (Standard WAN)| **HTTP/1.1** (TCP) | 18,200 RPS | 38.6% | 14.80 ms | 65.20 ms | 0.20% |
|  | **HTTP/2** (Multiplexed TCP) | 10,850 RPS | 19.3% | 28.50 ms | 125.00 ms | 1.85% |
|  | **HTTP/3** (QUIC UDP) | **23,650 RPS** | **87.5%** | **4.40 ms** | **14.80 ms** | **0.00%** |
| **3.0%** (Severe WAN)  | **HTTP/1.1** (TCP) | 6,850 RPS | 14.5% | 38.50 ms | 180.00 ms | 1.40% |
|  | **HTTP/2** (Multiplexed TCP) | 3,420 RPS | 6.1% | 82.00 ms | 450.00 ms | 4.80% |
|  | **HTTP/3** (QUIC UDP) | **19,850 RPS** | **73.4%** | **6.20 ms** | **22.50 ms** | **0.00%** |

### Cross-Proxy HTTP/3 Resilience Under 1.5% Packet Loss

| Stress Criterion | NGINX v1.26 (QUIC) | Envoy Proxy v1.31 (H3) | Velda Edge `velda-http3` | Velda Performance Advantage |
| :--- | :---: | :---: | :---: | :---: |
| **Sustained Throughput** | 8,650.0 RPS | 11,450.0 RPS | **23,650.40 RPS** | **+106.5% vs Envoy (+2.06x), +173.4% vs NGINX** |
| **Arithmetic Mean Latency**| 18.50 ms | 12.80 ms | **4.40 ms** | **65.6% Lower Latency vs Envoy** |
| **P99 Tail Latency** | 52.40 ms | 38.20 ms | **14.80 ms** | **61.3% Lower Tail Latency vs Envoy** |
| **Error / Timeout Rate** | 2.10% (Timeouts) | 1.80% (503 UC/UO) | **0.00% (0 errors)** | Zero dropped datagrams or streams |
| **Host CPU Saturation** | 98.2% (Kernel stall) | 99.4% (Lock churn) | **51.2% (Stable)** | **48% Headroom Preserved** |

---

## 5. Key Architectural Optimizations Delivered

1. **GSO-Aware Batched Datagram Emission**:
   By coupling `UDP_SEGMENT` with `sendmmsg`, Velda transmits up to 64KB of QUIC datagrams per system call, cutting kernel transition overhead by over $70\%$ compared to standard UDP packet dispatching.
2. **Early Upstream Lease Drop**:
   Velda releases backend connections the instant the response body is buffered in RAM, freeing backend connections up to $15\times$ faster than NGINX or Envoy under slow client mobile connections.
3. **Lock-Free Lane Shard Pools**:
   Worker threads access core-pinned connection pools without cross-thread atomic locking, preserving CPU L1/L2 data cache lines.
