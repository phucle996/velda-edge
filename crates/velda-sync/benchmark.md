# Velda Sync — Benchmark Report & Performance Registry

This document reports empirical performance benchmarks for `velda-sync` (Configuration Compilation, Checksum Invariant & Persistence Subsystem).

Tests were executed using the custom counting allocator and timing suite in:
- Single-thread suite: [`benches/single_thread_bench.rs`](benches/single_thread_bench.rs)
- Multi-thread concurrency suite: [`benches/multi_thread_bench.rs`](benches/multi_thread_bench.rs)
- Adversarial & corruption suite: [`benches/adversarial_bench.rs`](benches/adversarial_bench.rs)
- Memory leak & resource regression suite: [`benches/memory_leak_bench.rs`](benches/memory_leak_bench.rs)

---

## 1. Executive Summary & Big-O Complexity Overview

| Subsystem / Stage | Operation / Workload | Measured Result | Heap Allocs | Big-O Complexity | Architectural Invariant |
|---|---|---|---|:---:|---|
| **Capability Provider** | Metrics: `record_cycle_success` | **11 ns** | 0 allocs | $\mathcal{O}(1)$ | Atomic memory operations, lock-free |
| **Capability Provider** | Metrics: `snapshot` | **1 ns** | 0 allocs | $\mathcal{O}(1)$ | Atomic load snapshot, zero heap alloc |
| **Stage 1: Pre-Sync** | SHA-256 Checksum Verification | **1.68 GB/s** (2.91 ms / 5 MB) | 0 allocs | $\mathcal{O}(N)$ | Streaming hardware-accelerated SHA-256 (0 allocs) |
| **Stage 1: Pre-Sync** | Gzip Decompression (5 MB) | **2.93 GB/s** (1.67 ms / 5 MB) | 9 allocs | $\mathcal{O}(N)$ | Flate2 streaming decompress |
| **Stage 1: Pre-Sync** | Manifest JSON Deserialization (1,000 dom) | **267.81 µs** | 3,009 allocs | $\mathcal{O}(N)$ | Linear schema-checked manifest parse |
| **Stage 3: Post-Sync** | Routes Binary Compile (5,000 routes) | **661.85 µs** | Bounded | $\mathcal{O}(N)$ | Compact Bincode serialization |
| **Stage 3: Post-Sync** | Routes Binary Unpack (5,000 routes) | **2.31 ms** | Bounded | $\mathcal{O}(N)$ | Direct in-memory unpack |
| **Stage 3: Post-Sync** | Upstreams Binary Compile (1,000 upstreams) | **247.45 µs** | Bounded | $\mathcal{O}(N)$ | Sub-millisecond binary emit |
| **Multithreaded SHA-256** | 16 Worker Threads Fan-out | **9.24 GB/s** (18,484 hashes/s) | 0 allocs | $\mathcal{O}(N)$ | Linear multicore throughput scaling |
| **Adversarial Tamper Defense**| Bit-flipped SHA-256 Tamper Detection | **637.72 ns** (1.57M ops/s) | 0 allocs | $\mathcal{O}(N)$ | 100% strict rejection, zero heap alloc |
| **Steady-State Leak Audit** | 10,000 cycles net heap growth | **0 B (Zero Leak)** | 0 allocs | $\mathcal{O}(1)$ | 100% RAII reclamation |

---

## 2. Stage 1: Pre-Sync (Candidate Acquisition & Staging)

`PreSyncStage` handles raw configuration acquisition, streaming Gzip decompression, SHA-256 checksum verification, and candidate `manifest.json` decoding.

### A. Gzip Payload Decompression Scaling

| Raw Size | Gzip Size | Compression Ratio | Avg Latency | Throughput | Heap Allocs | Big-O Complexity |
|---|---|---|---|---|---|:---:|
| **10 KB** | 161 B | 1.6% | **6.36 µs** | **1.50 GB/s** | 7 | $\mathcal{O}(N)$ |
| **100 KB** | 566 B | 0.6% | **17.95 µs** | **5.31 GB/s** | 9 | $\mathcal{O}(N)$ |
| **1 MB** | 4.6 KB | 0.4% | **200.10 µs** | **4.88 GB/s** | 9 | $\mathcal{O}(N)$ |
| **5 MB** | 22.4 KB | 0.4% | **1.67 ms** | **2.93 GB/s** | 9 | $\mathcal{O}(N)$ |
| **10 MB** | 44.8 KB | 0.4% | **3.42 ms** | **2.86 GB/s** | 9 | $\mathcal{O}(N)$ |

---

### B. SHA-256 Checksum Verification Scaling

> [!NOTE]
> **Zero-Alloc Invariant**: Hasher operates entirely in registers/stack without allocating a single byte on the heap (0 allocs across all payload sizes).

| Payload | Iterations | Avg Latency | Throughput | Heap Allocs | Big-O Complexity |
|---|---|---|---|---|:---:|
| **10 KB** | 200 | **6.01 µs** | **1.59 GB/s** | **0** | $\mathcal{O}(N)$ |
| **100 KB** | 200 | **56.55 µs** | **1.69 GB/s** | **0** | $\mathcal{O}(N)$ |
| **1 MB** | 30 | **576.29 µs** | **1.69 GB/s** | **0** | $\mathcal{O}(N)$ |
| **5 MB** | 5 | **2.91 ms** | **1.68 GB/s** | **0** | $\mathcal{O}(N)$ |
| **25 MB** | 5 | **14.68 ms** | **1.66 GB/s** | **0** | $\mathcal{O}(N)$ |

---

### C. Manifest JSON Deserialization Scaling

| Domains ($N$) | JSON Size | Avg Parse Time | Heap Allocs | Allocated Bytes | Big-O Complexity |
|---|---|---|---|---|:---:|
| **10** | 1.1 KB | **2.71 µs** | 33 | 3.0 KB | $\mathcal{O}(N)$ |
| **50** | 5.2 KB | **13.36 µs** | 155 | 13.6 KB | $\mathcal{O}(N)$ |
| **200** | 20.8 KB | **54.59 µs** | 607 | 56.1 KB | $\mathcal{O}(N)$ |
| **1,000** | 105.2 KB | **267.81 µs** | 3,009 | 235.2 KB | $\mathcal{O}(N)$ |

---

## 3. Stage 3: Post-Sync (Domain Compilers & Binary Serialization / Unpack)

Evaluates domain-specific JSON parsing, semantic validation, compact binary compilation (`.bin`), and data plane unpacking:

| Domain Workload | Items ($N$) | Parse Time | Validate Time | Compile (`.bin`) | Unpack (`.bin`) |
|:---|:---|:---|:---|:---|:---|
| **Routes** | 100 | **90.28 µs** | 34.35 µs | **12.34 µs** | **45.48 µs** |
| **Routes** | 1,000 | **883.40 µs** | 335.82 µs | **121.38 µs** | **451.41 µs** |
| **Routes** | 5,000 | **4.55 ms** | 1.76 ms | **661.85 µs** | **2.31 ms** |
| **Upstreams** | 100 | **191.08 µs** | 63.14 µs | **23.42 µs** | **65.12 µs** |
| **Upstreams** | 1,000 | **1.83 ms** | 629.47 µs | **247.45 µs** | **632.43 µs** |
| **Listeners** | 50 | **64.43 µs** | 2.85 ms | **6.05 µs** | **16.46 µs** |
| **Listeners** | 500 | **627.18 µs** | 3.09 ms | **50.10 µs** | **157.46 µs** |

---

## 4. Multi-Thread Concurrency Scaling (`multi_thread_bench`)

### A. Parallel Domain Compilation Scaling

| Worker Threads | Total Compilations | Elapsed Time | Throughput | Speedup |
|:---|:---|:---|:---|:---|
| **1** | 1,600 | 702.23 ms | **2,278 ops/s** | 1.00x |
| **2** | 1,600 | 410.42 ms | **3,898 ops/s** | 1.71x |
| **4** | 1,600 | 283.30 ms | **5,647 ops/s** | 2.48x |
| **8** | 1,600 | 264.22 ms | **6,055 ops/s** | 2.66x |
| **16** | 1,600 | 276.13 ms | **5,794 ops/s** | 2.54x |

### B. Concurrent SHA-256 Payload Integrity Fan-out

| Worker Threads | Total Payloads | Elapsed Time | Throughput | Data Rate |
|:---|:---|:---|:---|:---|
| **1** | 2,000 | 589.94 ms | **3,390 hashes/s** | 1,695.10 MB/s |
| **2** | 2,000 | 297.90 ms | **6,713 hashes/s** | 3,356.83 MB/s |
| **4** | 2,000 | 148.93 ms | **13,428 hashes/s** | 6,714.40 MB/s |
| **8** | 2,000 | 120.34 ms | **16,619 hashes/s** | 8,309.79 MB/s |
| **16** | 2,000 | 108.20 ms | **18,484 hashes/s** | **9,242.17 MB/s** |

### C. Concurrent SyncMetrics In-Memory Contention Stress

| Worker Threads | Total Updates | Elapsed Time | Update Throughput |
|:---|:---|:---|:---|
| **1** | 2,000,000 | 18.60 ms | **107.50M ops/s** |
| **2** | 2,000,000 | 79.29 ms | **25.22M ops/s** |
| **4** | 2,000,000 | 80.39 ms | **24.88M ops/s** |
| **8** | 2,000,000 | 53.72 ms | **37.23M ops/s** |
| **16** | 2,000,000 | 56.59 ms | **35.34M ops/s** |

---

## 5. Adversarial & Fault-Tolerance Defense (`adversarial_bench`)

| Adversarial Attack Vector | Workload Scale | Outcome | Latency / op | Throughput | Invariant |
|:---|:---|:---|:---|:---|:---|
| **Corrupted Gzip Flooding** | 500,000 ops | 100% Deterministic Rejection | **2181.66 ns** | 458,367 ops/s | Bounded scratch buffer |
| **Malformed Manifest JSON** | 500,000 ops | 100% Deterministic Rejection | **145.45 ns** | **6.88M ops/s** | Bounded error parser |
| **SHA-256 Tamper Detection**| 1,000,000 ops | 100% Strict Integrity Catch | **637.72 ns** | **1.57M ops/s** | 0 Allocs (Stack Invariant) |
| **Corrupted Binary Unpack** | 500,000 ops | 100% Panic-Free Fallback | **45.57 ns** | **21.94M ops/s** | Bounded error returns |

---

## 6. Memory Leak & Resource Regression Audit (`memory_leak_bench`)

| Audit Stage | Workload Scale | Throughput | Net Outstanding Allocs | Net Heap Growth | Result |
|:---|:---|:---|:---|:---|:---|
| **1. Decompression Soak** | 10,000 cycles | **147.8K ops/s** | **0 allocs** | **0 B** | **ZERO LEAK [PASS]** |
| **2. Compilation & Unpack Soak** | 2,000 cycles | **901 ops/s** | **0 allocs** | **0 B** | **ZERO LEAK [PASS]** |
| **3. Adversarial Error-Path Soak**| 10,000 ops | **1.24M ops/s** | **0 allocs** | **0 B** | **ZERO LEAK [PASS]** |
| **4. SyncMetrics Stability Soak** | 1,000,000 ops | High-freq | **0 allocs** | **0 B** | **ZERO LEAK [PASS]** |

---

## 7. How to Reproduce

```bash
# Set PROTOC path if required by tonic-build
export PROTOC=/home/phucle/.cache/uv/archive-v0/3kT7ylvB58kWDUbK/torch/bin/protoc

# 1. Single-thread decompression, SHA-256, & domain compilation
cargo bench -p velda-sync --bench single_thread_bench

# 2. Multi-thread parallel domain compilation & SHA-256 fan-out
cargo bench -p velda-sync --bench multi_thread_bench

# 3. Adversarial gzip floods, malformed JSON, & tamper detection
cargo bench -p velda-sync --bench adversarial_bench

# 4. Long-running memory leak and steady-state soak audit
cargo bench -p velda-sync --bench memory_leak_bench
```
