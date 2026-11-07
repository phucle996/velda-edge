# velda-discovery — Benchmark & Performance Verification Report

This document reports empirical performance benchmarks for `velda-discovery` (Stage 1 Backend Topology Discovery).

Tests were executed using the custom counting allocator and timing suite in:
- Single-thread suite: [`benches/single_thread_bench.rs`](benches/single_thread_bench.rs)
- Multi-thread concurrency suite: [`benches/multi_thread_bench.rs`](benches/multi_thread_bench.rs)
- Adversarial & fault-tolerance suite: [`benches/adversarial_bench.rs`](benches/adversarial_bench.rs)
- Memory leak & resource regression suite: [`benches/memory_leak_bench.rs`](benches/memory_leak_bench.rs)
- RFC 1035 wire codec suite: [`benches/wire_bench.rs`](benches/wire_bench.rs)

---

## 1. Executive Summary

| Critical Metric | Target Requirement | Measured Result | Status |
| :--- | :--- | :--- | :--- |
| **Hot-Path Snapshot Read** (`current_endpoints()`) | < 50 ns, 0 allocs | **20.73 ns**, **0.00 allocs** | **Exceeded** (48.2M ops/s) |
| **Direct IP Target Resolution** | < 20 ns, 0 allocs | **7.67 ns**, **0.00 allocs** | **Exceeded** (130.4M ops/s) |
| **In-Memory Cache Lookup** (`DnsCache::get`) | < 100 ns, 0 allocs | **54.62 ns**, **0.00 allocs** | **Exceeded** (18.3M ops/s) |
| **Negative Cache Shield** (NXDOMAIN) | < 100 ns, 0 allocs | **66.87 ns**, **0.00 allocs** | **Passed** (14.9M ops/s) |
| **Static Hosts Lookup Scaling** ($N = 10 \dots 10,000$) | $\mathcal{O}(1)$ flat | **20.22 ns — 22.84 ns** | **Verified $\mathcal{O}(1)$** |
| **Lock-Free Read Under Background Refresh** | Zero lock contention | **26.73M ops/s** | **Passed** (8 readers, 40M reads) |
| **Singleflight Stampede Coalescing** | Maximize wire deduplication | **99.5% coalesced** (200 tasks $\to$ 1 query) | **Exceeded** |
| **Total Nameserver Outage (LKG Fallback)** | Zero traffic drop | **118.48 ns** (8.44M ops/s) | **Passed** (Zero downtime) |
| **RFC 1035 Response Decoding (16 A Records)**| Fast wire unpack | **233.19 ns** (68.6M records/s) | **Exceeded** |
| **Steady-State Memory Leak** | 5,000,000 lookups net heap growth | **0 B (Zero Leak)** | **Passed** (100% RAII reclamation) |

---

## 2. In-Memory DnsCache Lookup (`single_thread_bench`)

Measures 500,000 iterations of in-memory cache operations across positive hit, negative NXDOMAIN hit, and cache miss scenarios:

| Operation | Total Time | Latency / op | Allocs / op | Bytes / op | Throughput |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Positive Cache Hit** | 27.31 ms | **54.62 ns** | **0.00** | **0.00 B** | **18,307,997 ops/s** |
| **Negative Cache Hit (NXDOMAIN)** | 33.44 ms | **66.87 ns** | **0.00** | **0.00 B** | **14,953,940 ops/s** |
| **Cache Miss** | 25.37 ms | **50.74 ns** | **0.00** | **0.00 B** | **19,709,679 ops/s** |

---

## 3. Hot-Path EndpointSet Read (`Discovery::current_endpoints()`)

Measures 1,000,000 atomic lock-free reads of active `EndpointSet` snapshots via `ArcSwap`:

| Operation | Iterations | Total Time | Latency / op | Allocs / op | Throughput |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `Discovery::current_endpoints()` | 1,000,000 | 20.73 ms | **20.73 ns** | **0.00** | **48,230,718 ops/s** |

> **Hot-Path Invariant**: Request processing hot path reads active endpoint snapshots with zero lock contention and zero heap allocations.

---

## 4. Static Hosts File Lookup Scaling ($\mathcal{O}(1)$ Verification)

Measures 100,000 lookups per table size across 4 orders of magnitude:

| Table Size ($N$) | Total Lookup Time | Latency / op | Big-O Complexity |
| :--- | :--- | :--- | :--- |
| **$N = 10$** | 2.02 ms | **20.22 ns** | **$\mathcal{O}(1)$** |
| **$N = 100$** | 2.28 ms | **22.84 ns** | **$\mathcal{O}(1)$** |
| **$N = 1,000$** | 2.11 ms | **21.09 ns** | **$\mathcal{O}(1)$** |
| **$N = 10,000$** | 2.24 ms | **22.41 ns** | **$\mathcal{O}(1)$** |

Observed Algorithm Complexity: **$\mathcal{O}(1)$ flat 20 - 23 ns**.

---

## 5. DnsServer Target & End-to-End Resolution

| Target / Scenario | Total Time | Latency / op | Allocs / op | Throughput | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Direct IP Target (`10.96.0.10:53`)** | 1.53 ms | **7.67 ns** | **0.00** | **130,436,172 ops/s** | Direct socket address fast-path |
| **Host Target via Hosts (`coredns:53`)** | 11.50 ms | **57.52 ns** | 1.00 | **17,386,141 ops/s** | In-memory hosts map lookup |
| **Cached `resolver.resolve()`** | 21.77 ms | **108.87 ns** | 2.00 | **9,185,114 ops/s** | Zero wire I/O hot-path resolution |

---

## 6. Multicore Concurrency & Singleflight Stampede Defense (`multi_thread_bench`)

### A. Multicore Cache Concurrency Scaling (1 .. 64 Workers)

| Workers | Total Operations | Total Time | Aggregate Throughput | Avg Latency / op |
| :--- | :--- | :--- | :--- | :--- |
| **1** | 100,000 | 5.14 ms | **19.46M ops/s** | 51.39 ns |
| **2** | 200,000 | 22.32 ms | **8.96M ops/s** | 111.60 ns |
| **4** | 400,000 | 49.07 ms | **8.15M ops/s** | 122.67 ns |
| **8** | 800,000 | 82.83 ms | **9.66M ops/s** | 103.54 ns |
| **16** | 1,600,000 | 168.38 ms | **9.50M ops/s** | 105.24 ns |
| **32** | 3,200,000 | 338.36 ms | **9.46M ops/s** | 105.74 ns |
| **64** | 6,400,000 | 654.66 ms | **9.78M ops/s** | 102.29 ns |

### B. Readers Under Background Topology Refresh

| Concurrency Scenario | Readers | Duration | Total Reads | Read Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **Lock-Free ArcSwap Under Refresh** | 8 | 1.50 s | 40,089,919 | **26.73M ops/s** |

### C. Concurrent Singleflight Query Deduplication (Stampede Defense)

| Concurrent Tasks | Wire Queries Issued | Queries Deduplicated | Coalescing Ratio | Latency |
| :--- | :--- | :--- | :--- | :--- |
| **10** | 1 | 9 | **90.0%** | 62.76 µs |
| **50** | 1 | 49 | **98.0%** | 57.57 µs |
| **100** | 1 | 99 | **99.0%** | 74.89 µs |
| **200** | 1 | 199 | **99.5%** | 140.18 µs |

---

## 7. Adversarial & Fault-Tolerance Suite (`adversarial_bench.rs`)

| Adversarial Scenario | Workload Scale | Key Outcome | Latency / op | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **100% NXDOMAIN Flooding Storm** | 500,000 requests | 100% absorbed in RAM | **85.66 ns** | **11.67M ops/s** |
| **Nameserver Failover Latency** | 1,000 failovers | Dead primary $\to$ healthy backup | **2.07 ms** | 100% Success |
| **Total Outage (LKG Active)** | 100,000 requests | Zero traffic drop via LKG | **118.48 ns** | **8.44M ops/s** |

---

## 8. RFC 1035 Binary DNS Wire Protocol Codec (`wire_bench.rs`)

### A. Query Serialization Latency

| Domain Hierarchy | Length | Total Time | Latency / op | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **Short (2 labels)** | 9 B | 26.01 ms | **52.01 ns** | **19.23M ops/s** |
| **Standard (5 labels)** | 27 B | 37.57 ms | **75.13 ns** | **13.31M ops/s** |
| **Deep FQDN (7 labels)** | 52 B | 38.51 ms | **77.02 ns** | **12.98M ops/s** |

### B. Response Parsing Scaling

| Answer Records | Packet Size | Total Time | Latency / op | Records / sec | Throughput |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1 record** | 56 B | 8.22 ms | **41.10 ns** | 24.33M rec/s | **24.33M pkts/s** |
| **4 records** | 104 B | 10.14 ms | **50.69 ns** | 78.91M rec/s | **19.73M pkts/s** |
| **16 records** | 296 B | 46.64 ms | **233.19 ns** | 68.61M rec/s | **4.29M pkts/s** |
| **64 records** | 1,064 B | 103.00 ms | **515.00 ns** | 124.27M rec/s | **1.94M pkts/s** |

### C. Name Compression Pointer (0xC0) & Security Invariant

- **Compression Pointer Bandwidth Savings**: **-67.1% wire bytes** (314 B vs 954 B), decoded in **164.84 ns**.
- **Malformed & Adversarial Defense**:
  - Truncated Header (< 12B): **47.39 ns** (100% Safe)
  - ID Mismatch: **104.71 ns** (100% Safe)
  - NXDOMAIN Fast-Path: **26.85 ns** (37.24M ops/s)
  - Corrupt Label / Overflow Defense: **48 - 56 ns** (100% Safe)

---

## 9. Memory Leak & Resource Regression Audit (`memory_leak_bench.rs`)

| Audit Stage | Workload Scale | Throughput | Net Lingering Allocs | Net Heap Growth | Result |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1. Steady-State DnsCache Lookups** | 5,000,000 lookups | **17.12M ops/s** | **0 allocs** | **0 B** | **ZERO LEAK [PASS]** |
| **2. Negative Cache Flood & Bound** | 500,000 inserts | **2.33M inserts/s** | Bounded (1000) | **102.7 KB (Bounded)** | **BOUNDED [PASS]** |
| **3. Singleflight Channel Lifecycle** | 100,000 resolves | **1.18M resolves/s** | **0 allocs** | **0 B** | **ZERO LEAK [PASS]** |
| **4. ArcSwap Generation Soak** | 10,000 generations | **2.73M gens/s** | **0 allocs** | **0 B** | **ZERO LEAK [PASS]** |

---

## 10. How to Reproduce

```bash
# 1. Single-thread cache lookup & hosts O(1) scaling
cargo bench -p velda-discovery --bench single_thread_bench

# 2. Multi-thread concurrency & singleflight deduplication
cargo bench -p velda-discovery --bench multi_thread_bench

# 3. Adversarial NXDOMAIN storms, failover, & LKG fallback
cargo bench -p velda-discovery --bench adversarial_bench

# 4. RFC 1035 wire protocol encoding/decoding & corruption defenses
cargo bench -p velda-discovery --bench wire_bench

# 5. Long-running memory leak and capacity bound audit
cargo bench -p velda-discovery --bench memory_leak_bench
```
