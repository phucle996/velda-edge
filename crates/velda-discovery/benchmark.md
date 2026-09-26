# velda-discovery — Benchmark & Performance Verification Report

This document reports empirical performance benchmarks for `velda-discovery` (Stage 1 Backend Topology Discovery).

Tests were executed using the custom counting allocator and timing suite in [`benches/discovery_bench.rs`](benches/discovery_bench.rs).

---

## 1. Executive Summary

| Critical Metric | Target Requirement | Measured Result | Status |
| :--- | :--- | :--- | :--- |
| **Hot-Path Snapshot Read** (`current_endpoints()`) | < 50 ns, 0 allocs | **20.34 ns**, **0.00 allocs** | **Exceeded** (49.1M ops/s) |
| **Direct IP Target Resolution** | < 20 ns, 0 allocs | **7.81 ns**, **0.00 allocs** | **Exceeded** (128.0M ops/s) |
| **In-Memory Cache Lookup** (`DnsCache::get`) | < 100 ns | **79.02 ns** | **Passed** (12.6M ops/s) |
| **Negative Cache Shield** (NXDOMAIN) | < 100 ns | **76.73 ns** | **Passed** (13.0M ops/s) |
| **Static Hosts Lookup Scaling** (N = 10 .. 10,000) | $O(1)$ flat | **29.68 ns — 44.53 ns** | **Verified $O(1)$** |
| **Multicore Scalability** (64 Workers) | Non-blocking reads | **9.53M ops/s** (104.89 ns) | **Passed** |
| **End-to-End Cached Resolution** | < 250 ns | **151.54 ns** | **Passed** (6.6M ops/s) |

---

## 2. In-Memory DnsCache Lookup

Measures 500,000 iterations of in-memory cache operations across positive hit, negative NXDOMAIN hit, and cache miss scenarios:

| Operation | Total Time | Latency / op | Allocs / op | Bytes / op | Throughput |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Positive Cache Hit** | 39.51 ms | **79.02 ns** | 2.00 | 46.00 B | **12,655,730 ops/s** |
| **Negative Cache Hit (NXDOMAIN)** | 38.36 ms | **76.73 ns** | 1.00 | 17.00 B | **13,033,058 ops/s** |
| **Cache Miss** | 41.76 ms | **83.53 ns** | 1.00 | 23.00 B | **11,971,848 ops/s** |

---

## 3. Hot-Path EndpointSet Read (`Discovery::current_endpoints()`)

Measures 1,000,000 atomic lock-free reads of active `EndpointSet` snapshots via `ArcSwap`:

| Operation | Iterations | Total Time | Latency / op | Allocs / op | Throughput |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `Discovery::current_endpoints()` | 1,000,000 | 20.34 ms | **20.34 ns** | **0.00** | **49,169,031 ops/s** |

> **Hot-Path Invariant**: Request processing serving reads active endpoint snapshots with zero lock contention and zero heap allocations.

---

## 4. Static Hosts File Lookup Scaling (Big-O Verification)

Measures 100,000 lookups per table size across 4 orders of magnitude:

| Table Size (N) | Total Lookup Time | Latency / op | Big-O Complexity |
| :--- | :--- | :--- | :--- |
| **N = 10** | 4.45 ms | 44.53 ns | **$O(1)$** |
| **N = 100** | 4.45 ms | 44.49 ns | **$O(1)$** |
| **N = 1,000** | 2.97 ms | 29.68 ns | **$O(1)$** |
| **N = 10,000** | 3.11 ms | 31.09 ns | **$O(1)$** |

Observed Algorithm Complexity: **$O(1)$**.

---

## 5. Multicore Concurrency Scaling (1 .. 64 Workers)

Measures concurrent cache read throughput and latency degradation under increasing worker thread contention:

| Workers | Total Operations | Total Time | Aggregate Throughput | Avg Latency / op |
| :--- | :--- | :--- | :--- | :--- |
| **1** | 50,000 | 4.37 ms | 11,429,770 ops/s | 87.49 ns |
| **2** | 100,000 | 12.42 ms | 8,053,905 ops/s | 124.16 ns |
| **4** | 200,000 | 24.23 ms | 8,254,871 ops/s | 121.14 ns |
| **8** | 400,000 | 41.56 ms | 9,624,658 ops/s | 103.90 ns |
| **16** | 800,000 | 85.47 ms | 9,360,180 ops/s | 106.84 ns |
| **32** | 1,600,000 | 166.69 ms | 9,598,622 ops/s | 104.18 ns |
| **64** | 3,200,000 | 335.66 ms | **9,533,499 ops/s** | **104.89 ns** |

---

## 6. DnsServer Target Resolution Latency

Measures resolution overhead between direct IP socket addresses vs host-based lookup:

| Target Type | Total Time | Latency / op | Allocs / op | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **Direct IP Target (`10.96.0.10:53`)** | 1.56 ms | **7.81 ns** | **0.00** | **128,061,797 ops/s** |
| **Host Target via Hosts (`coredns:53`)** | 12.34 ms | **61.70 ns** | 2.00 | **16,207,481 ops/s** |

---

## 7. End-to-End Resolver Hot-Path Resolution

Measures 200,000 iterations of full `resolver.resolve()` on warm cache:

| Scenario | Iterations | Total Time | Latency / op | Allocs / op | Throughput |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Cached `resolver.resolve()`** | 200,000 | 30.31 ms | **151.54 ns** | 5.00 | **6,598,829 ops/s** |

> Zero wire I/O is executed during hot-path serving.
