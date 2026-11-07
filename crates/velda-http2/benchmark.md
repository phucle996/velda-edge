# velda-http2 — Benchmark & Performance Verification Report

This document reports empirical performance benchmarks, hardware contention audits, and memory safety invariants for `velda-http2` (RFC 9113 HTTP/2 Binary Protocol Engine).

Benchmarks were executed using the custom counting allocator and high-resolution timing harness across the 4 modular suites:
- Single-thread suite: [`benches/single_thread_bench.rs`](benches/single_thread_bench.rs)
- Multi-thread concurrency suite: [`benches/multi_thread_bench.rs`](benches/multi_thread_bench.rs)
- Adversarial & stress suite: [`benches/adversarial_bench.rs`](benches/adversarial_bench.rs)
- Memory leak & resource regression suite: [`benches/memory_leak_bench.rs`](benches/memory_leak_bench.rs)

---

## 1. Executive Summary

| Target / Capability | Invariant / Target Metric | Measured Result | Status |
| :--- | :--- | :--- | :--- |
| **In-Place Header Sanitization** | < 150 ns, 0 allocs | **98.56 ns**, **0.00 allocs** | **Exceeded** (10.15 Million ops/s) |
| **H2 Full Roundtrip Turnaround** | < 25 µs, full duplex loop | **11.34 - 12.45 µs** | **Exceeded** (~88,200 roundtrips/s) |
| **Server Streaming (SSE / Chunks)** | > 200,000 chunks/s | **13.11 µs / stream** | **Exceeded** (381,390 chunks/s) |
| **E2E Proxy Pipe Turnaround** | Full client <-> proxy <-> upstream | **12.62 µs / op** | **Passed** (~79,260 ops/s) |
| **Multiplexed Concurrency (16)** | Sub-10 µs per stream | **6.17 µs / op**, **161.95K ops/s** | **Passed** (Low stream contention) |
| **Multiplexed Concurrency (128)** | Zero lock degradation | **6.68 µs / op**, **149.78K ops/s** | **Passed** (High multiplexing scaling) |
| **Rapid Reset Attack Resilience** | CVE-2023-44487 mitigation | **5,000 resets in 5.23 ms** | **Passed** (956,470 resets/s, 0 bloat) |
| **Hop-by-Hop Header Stripping** | Malicious header sanitation | **219.03 ns / op** | **Passed** (4.57 Million ops/s) |
| **Zero Memory Leak (20,000 Reqs)** | Net Heap Growth | **0 B (Zero Leak)** | **Passed** (137.7 MB allocated / freed) |

---

## 2. Suite 1: Single-Thread Latency, Framing & Primitives

### RFC 9113 Header Sanitization
Evaluates in-place stripping of forbidden hop-by-hop headers (`Connection`, `Keep-Alive`, `Proxy-Connection`, `Transfer-Encoding`, `Upgrade`) versus allocation-based filtering:
- **`sanitize_h2_headers` (In-Place)**: **98.56 ns / op**, **0.00 allocs** (**10,146,103 ops/s**).
- **`filter_h2_headers` (Copy)**: **180.38 ns / op**, **2.00 allocs** (**5,543,851 ops/s**).

### Roundtrip Turnaround & Streaming Delivery
- **Empty Body (200 OK)**: **11.34 µs / op** (**88,200 ops/s**).
- **1 KB Body Echo**: **12.45 µs / op** (**80,350 ops/s**).
- **Server Streaming (5 chunks of 128B)**: **13.11 µs / stream** (**381,390 chunks/s**).
- **E2E Buffered Proxy Pipeline**: **12.62 µs / op** (**79,260 ops/s**).

---

## 3. Suite 2: Multi-Thread & Multiplexed Concurrency

Evaluates stream multiplexing over concurrent streams on shared HTTP/2 connections:

| Concurrency Level | Total Completed Requests | Latency / op | Aggregate Throughput |
| :--- | :--- | :--- | :--- |
| **4 Streams** | 200 requests | **7.00 µs** | 142,770 ops/s |
| **16 Streams** | 800 requests | **6.17 µs** | 161,950 ops/s |
| **64 Streams** | 3,200 requests | **6.48 µs** | 154,300 ops/s |
| **128 Streams** | 6,400 requests | **6.68 µs** | 149,780 ops/s |

---

## 4. Suite 3: Adversarial, Rapid Reset & Safety Limits

### Rapid Reset Flood Attack (CVE-2023-44487)
Evaluates resilience against malicious clients opening streams and immediately issuing `RST_STREAM` frames:
- **Processed 5,000 rapid resets** in **5.23 ms** (**956,470 resets/s**).
- Zero memory bloat, zero worker task panics, connection state remains completely healthy.

### Oversized Payload & Malicious Header Stripping
- **Oversized Payload Early Rejection**: 1,000 / 1,000 rejected at **11.80 µs / op** (**84,740 ops/s**).
- **Hop-by-Hop Header Stripping**: 5 illegal headers stripped in **219.03 ns / op** (**4,570,000 ops/s**).

---

## 5. Suite 4: Zero-Leak & Heap Stability

Audits memory consumption across sustained high-throughput request cycles:
- **5,000 Completed Stream Iterations**: 34.45 MB allocated, **0 B Net Heap Growth** (**Zero Leak**).
- **20,000 Completed Stream Iterations**: 137.77 MB allocated, **0 B Net Heap Growth** (**Zero Leak**).

---

## 6. How to Reproduce

```bash
# 1. Single-thread latency & header sanitization
cargo bench -p velda-http2 --bench single_thread_bench

# 2. Multi-thread & multiplexed stream concurrency
cargo bench -p velda-http2 --bench multi_thread_bench

# 3. Adversarial, rapid reset & safety limits
cargo bench -p velda-http2 --bench adversarial_bench

# 4. Zero-leak & heap stability audit
cargo bench -p velda-http2 --bench memory_leak_bench
```
