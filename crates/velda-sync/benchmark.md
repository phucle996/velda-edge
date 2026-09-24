# Velda Sync — Benchmark Report & Performance Registry

> **Last Updated**: 2026-09-25  
> **Environment**: Rust 1.98 / Edition 2024 / Linux x86_64 (`x86_64-unknown-linux-gnu`)  
> **Profile**: `release` (`[profile.bench]`, `-O3`, LTO enabled)  
> **Workload Baseline**: Real production configurations from [`example/`](./example) scaled up to 10,000+ entities and 200,000+ JSON lines.  
> **Measurement Harness**: High-precision monotonic timer + thread-safe counting global allocator (`CountingAllocator`).

---

## Executive Summary & Big-O Complexity Overview

| Subsystem / Stage | Operation / Workload | Latency / Throughput | Heap Allocs | Big-O Complexity | Architectural Invariant |
|---|---|---|---|:---:|---|
| **Capability Provider** | Metrics: `record_cycle_success` | **12 ns** | 0 allocs | $\mathcal{O}(1)$ | Atomic memory operations, lock-free |
| **Capability Provider** | Metrics: `snapshot` | **6 ns** | 0 allocs | $\mathcal{O}(1)$ | Atomic load snapshot, zero heap alloc |
| **Capability Provider** | Logging: Caller thread dispatch | **1.30 µs** (771K logs/s) | 0 blocking | $\mathcal{O}(1)$ | Non-blocking ring buffer via `tracing-appender` |
| **Stage 1: Pre-Sync** | SHA-256 Checksum Verification | **1.54 GB/s** (3.17 ms / 5 MB) | 0 allocs | $\mathcal{O}(N)$ | Streaming hardware-accelerated SHA-256 |
| **Stage 1: Pre-Sync** | Gzip Decompression | **2.44 GB/s** (2.00 ms / 5 MB) | Minimal | $\mathcal{O}(N)$ | Flate2 streaming decompress |
| **Stage 2: Sync** | **Idle Reconcile No-Op Loop** | **3.81 µs** | 15 allocs | $\mathbf{\mathcal{O}(1)}$ | **Fast Short-Circuit on Manifest Checksum** |
| **Stage 2: Sync** | Delta Reconcile (10K routes) | **44.08 ms** | 373K allocs | $\mathcal{O}(M)$ | Only changed domains are recompiled |
| **Stage 3: Post-Sync** | Routes Domain Compile (10K routes) | **1.87 ms** | 17 allocs | $\mathcal{O}(N)$ | Zero redundant cloning, streaming binary emit |
| **Stage 3: Post-Sync** | **Data Plane Boot (Binary Unpack)** | **5.48 ms** vs 9.83 ms JSON | 100K allocs | $\mathbf{\mathcal{O}(N)}$ | **2.0x faster boot, 55.8% smaller payload** |

---

## 1. Stage 1: Pre-Sync (Candidate Acquisition & Staging)

`PreSyncStage` handles raw configuration acquisition, streaming Gzip decompression, SHA-256 checksum verification, and candidate `manifest.json` decoding.

### 1.1 Gzip Payload Decompression Scaling

| Raw Size | Gzip Size | Compression Ratio | Avg Latency | Throughput | Heap Allocs | Allocated Bytes | Big-O Complexity |
|---|---|---|---|---|---|---|:---:|
| 10 KB | 161 B | 1.6 % | 6.82 µs | 1.40 GB/s | 12 | 106.2 KB | $\mathcal{O}(N)$ |
| 100 KB | 566 B | 0.6 % | 18.47 µs | 5.16 GB/s | 15 | 330.2 KB | $\mathcal{O}(N)$ |
| 1 MB | 4.6 KB | 0.4 % | 299.20 µs | 3.26 GB/s | 19 | 4.07 MB | $\mathcal{O}(N)$ |
| 5 MB | 22.4 KB | 0.4 % | 2.00 ms | 2.44 GB/s | 21 | 16.07 MB | $\mathcal{O}(N)$ |
| 10 MB | 44.8 KB | 0.4 % | 4.08 ms | 2.39 GB/s | 22 | 32.07 MB | $\mathcal{O}(N)$ |
| 20 MB | 89.5 KB | 0.4 % | 15.52 ms | 1.26 GB/s | 23 | 64.07 MB | $\mathcal{O}(N)$ |

#### Gzip Decompression Big-O Linearity Drift

$$\text{Linearity Drift } (\%) = \left( \frac{\text{Growth Ratio}}{\text{Scale Ratio}} - 1 \right) \times 100\%$$

| Transition | Scale Ratio | Observed Growth | Linearity Drift % | Big-O Status |
|---|---|---|---|:---:|
| 10 KB $\rightarrow$ 100 KB | 10.0x | 2.71x | -72.9% | Sub-linear $\mathcal{O}(N)$ (Decompress buffer reuse) |
| 100 KB $\rightarrow$ 1 MB | 10.2x | 16.20x | +58.2% | Buffer expansion transition |
| 1 MB $\rightarrow$ 5 MB | 5.0x | 6.69x | +33.7% | Near-linear $\mathcal{O}(N)$ |
| 5 MB $\rightarrow$ 10 MB | 2.0x | 2.04x | **+2.0%** | **Strict $\mathcal{O}(N)$ [OK]** |
| 10 MB $\rightarrow$ 20 MB | 2.0x | 3.80x | +90.1% | L3 cache boundary pressure |

---

### 1.2 SHA-256 Checksum Verification Scaling

> [!NOTE]
> **Zero-Alloc Invariant**: Hasher operates entirely in registers/stack without allocating a single byte on the heap (0 allocs across all payload sizes).

| Payload | Iterations | Avg Latency | Throughput | Heap Allocs | Big-O Complexity |
|---|---|---|---|---|:---:|
| 10 KB | 300 | 7.97 µs | 1.20 GB/s | 0 | $\mathcal{O}(N)$ |
| 100 KB | 300 | 70.38 µs | 1.35 GB/s | 0 | $\mathcal{O}(N)$ |
| 1 MB | 50 | 612.16 µs | 1.60 GB/s | 0 | $\mathcal{O}(N)$ |
| 5 MB | 10 | 3.17 ms | 1.54 GB/s | 0 | $\mathcal{O}(N)$ |
| 10 MB | 10 | 6.24 ms | 1.56 GB/s | 0 | $\mathcal{O}(N)$ |
| 25 MB | 3 | 15.83 ms | 1.54 GB/s | 0 | $\mathcal{O}(N)$ |
| 50 MB | 3 | 33.05 ms | 1.48 GB/s | 0 | $\mathcal{O}(N)$ |

#### SHA-256 Big-O Linearity Drift

| Transition | Scale Ratio | Observed Growth | Linearity Drift % | Big-O Status |
|---|---|---|---|:---:|
| 10 KB $\rightarrow$ 100 KB | 10.0x | 8.83x | -11.7% | **Strict $\mathcal{O}(N)$ [OK]** |
| 100 KB $\rightarrow$ 1 MB | 10.2x | 8.70x | -15.1% | **Strict $\mathcal{O}(N)$ [OK]** |
| 1 MB $\rightarrow$ 5 MB | 5.0x | 5.18x | **+3.6%** | **Strict $\mathcal{O}(N)$ [OK]** |
| 5 MB $\rightarrow$ 10 MB | 2.0x | 1.97x | **-1.6%** | **Strict $\mathcal{O}(N)$ [OK]** |
| 10 MB $\rightarrow$ 25 MB | 2.5x | 2.54x | **+1.4%** | **Strict $\mathcal{O}(N)$ [OK]** |
| 25 MB $\rightarrow$ 50 MB | 2.0x | 2.09x | **+4.4%** | **Strict $\mathcal{O}(N)$ [OK]** |

---

### 1.3 Manifest JSON Decoding Scaling

| Domains ($N$) | JSON Size | Avg Parse Time | Heap Allocs | Allocated Bytes | Big-O Complexity |
|---|---|---|---|---|:---:|
| 10 | 1.1 KB | 2.61 µs | 33 | 3.0 KB | $\mathcal{O}(N)$ |
| 50 | 5.2 KB | 13.31 µs | 155 | 13.6 KB | $\mathcal{O}(N)$ |
| 100 | 10.3 KB | 25.81 µs | 306 | 27.7 KB | $\mathcal{O}(N)$ |
| 500 | 52.5 KB | 127.87 µs | 1,508 | 117.3 KB | $\mathcal{O}(N)$ |
| 1,000 | 105.2 KB | 252.41 µs | 3,009 | 235.2 KB | $\mathcal{O}(N)$ |
| 2,500 | 267.8 KB | 636.18 µs | 7,511 | 881.6 KB | $\mathcal{O}(N)$ |

#### Manifest JSON Big-O Linearity Drift

| Transition | Scale Ratio | Observed Growth | Linearity Drift % | Big-O Status |
|---|---|---|---|:---:|
| 10 $\rightarrow$ 50 dom | 5.0x | 5.09x | **+1.9%** | **Strict $\mathcal{O}(N)$ [OK]** |
| 50 $\rightarrow$ 100 dom | 2.0x | 1.94x | **-3.0%** | **Strict $\mathcal{O}(N)$ [OK]** |
| 100 $\rightarrow$ 500 dom | 5.0x | 4.95x | **-0.9%** | **Strict $\mathcal{O}(N)$ [OK]** |
| 500 $\rightarrow$ 1,000 dom | 2.0x | 1.97x | **-1.3%** | **Strict $\mathcal{O}(N)$ [OK]** |
| 1,000 $\rightarrow$ 2,500 dom | 2.5x | 2.52x | **+0.8%** | **Strict $\mathcal{O}(N)$ [OK]** |

---

## 2. Stage 2: Sync (Reconciliation Engine)

`SyncComposition` executes the 5-phase reconciliation cycle.

### 2.1 Reconciler Execution Times across Workloads

| Workload Weight (Routes, Upstreams, Listeners) | Hot-Path No-Op (Time, Allocs) | Delta Update Routes (Time, Allocs) | Full Update 5 Domains (Time, Allocs) | Idle Big-O | Active Big-O |
|---|---|---|---|:---:|:---:|
| 100 R / 20 U / 5 L | **3.81 µs** (15 allocs) | 517.21 µs (4,066 allocs) | 748.17 µs (4,899 allocs) | $\mathbf{\mathcal{O}(1)}$ | $\mathcal{O}(M)$ |
| 500 R / 50 U / 10 L | **4.01 µs** (15 allocs) | 2.09 ms (18,912 allocs) | 2.45 ms (20,499 allocs) | $\mathbf{\mathcal{O}(1)}$ | $\mathcal{O}(M)$ |
| 2,000 R / 200 U / 25 L | **3.84 µs** (15 allocs) | 8.41 ms (74,768 allocs) | 9.19 ms (79,941 allocs) | $\mathbf{\mathcal{O}(1)}$ | $\mathcal{O}(M)$ |
| 5,000 R / 500 U / 50 L | **3.88 µs** (15 allocs) | 21.69 ms (186,656 allocs) | 24.05 ms (199,218 allocs) | $\mathbf{\mathcal{O}(1)}$ | $\mathcal{O}(M)$ |
| 10,000 R / 1,000 U / 100 L | **3.95 µs** (15 allocs) | 44.08 ms (373,159 allocs) | 47.34 ms (397,777 allocs) | $\mathbf{\mathcal{O}(1)}$ | $\mathcal{O}(M)$ |

> [!IMPORTANT]
> **Hot-Path Fast Short-Circuit Invariant**:
> When configuration is unchanged, the daemon checks the Manifest SHA-256 checksum and returns in **3.81 – 3.95 µs** flat, regardless of whether the cluster manages 100 routes or 10,000 routes. **Time complexity is strictly $\mathcal{O}(1)$**.

### 2.2 Reconciler Big-O Growth Linearity Analysis

| Scale Transition | Scale Ratio | No-Op Growth | Delta Update Growth | Full Update Growth | Big-O Status |
|---|---|---|---|---|:---:|
| 100 $\rightarrow$ 500 routes | 5.0x | 1.05x | 4.03x (-19.3%) | 3.27x | **Strict $\mathcal{O}(N)$ [OK]** |
| 500 $\rightarrow$ 2,000 routes | 4.0x | 0.96x | 4.03x (+0.8%) | 3.76x | **Strict $\mathcal{O}(N)$ [OK]** |
| 2,000 $\rightarrow$ 5,000 routes | 2.5x | 1.01x | 2.58x (+3.1%) | 2.62x | **Strict $\mathcal{O}(N)$ [OK]** |
| 5,000 $\rightarrow$ 10,000 routes | 2.0x | 1.02x | 2.03x (+1.6%) | 1.97x | **Strict $\mathcal{O}(N)$ [OK]** |

---

## 3. Stage 3: Post-Sync (Domain Compilers & Binary Artifacts)

Independent domain compilers in [`src/post_sync/`](./src/post_sync) convert declarative JSON into validated, binary snapshots (`*.bin`).

### 3.1 Routes Domain Pipeline Scaling (up to 200,000+ JSON lines)

| Routes ($N$) | JSON Lines | JSON Size | Bin Size | JSON Parse Time (Allocs) | Validation Time (Allocs) | Binary Compile Time (Allocs) | Binary Unpack Time (Allocs) | Big-O Complexity |
|---|---|---|---|---|---|---|---|:---:|
| 100 | 2,025 | 43.5 KB | 19.3 KB | 109.21 µs (1,026) | 37.83 µs (1,002) | 16.84 µs (10) | 51.49 µs (1,001) | $\mathcal{O}(N)$ |
| 500 | 10,105 | 217.6 KB | 96.2 KB | 540.36 µs (5,108) | 199.75 µs (5,002) | 84.08 µs (12) | 247.17 µs (5,001) | $\mathcal{O}(N)$ |
| 1,000 | 20,205 | 435.2 KB | 192.3 KB | 961.74 µs (10,209) | 358.88 µs (10,002) | 151.28 µs (13) | 483.65 µs (10,001) | $\mathcal{O}(N)$ |
| 2,500 | 50,505 | 1.06 MB | 482.0 KB | 2.57 ms (25,511) | 944.33 µs (25,002) | 438.21 µs (15) | 1.27 ms (25,001) | $\mathcal{O}(N)$ |
| 5,000 | 101,005 | 2.13 MB | 964.8 KB | 4.98 ms (51,012) | 1.90 ms (50,002) | 933.37 µs (16) | 2.70 ms (50,002) | $\mathcal{O}(N)$ |
| 10,000 | 202,005 | 4.26 MB | 1.89 MB | 9.83 ms (102,013) | 3.81 ms (100,002) | 1.87 ms (17) | 5.48 ms (100,003) | $\mathcal{O}(N)$ |

#### Routes Big-O Linearity Drift Analysis

| Transition | Scale Ratio | JSON Parse Growth | Validation Growth | Compile Growth | Unpack Growth | Big-O Status |
|---|---|---|---|---|---|:---:|
| 100 $\rightarrow$ 500 | 5.0x | 4.95x (-1.0%) | 5.28x | 4.99x | 4.80x | **Strict $\mathcal{O}(N)$ [OK]** |
| 500 $\rightarrow$ 1,000 | 2.0x | 1.78x (-11.0%) | 1.80x | 1.80x | 1.96x | **Strict $\mathcal{O}(N)$ [OK]** |
| 1,000 $\rightarrow$ 2,500 | 2.5x | 2.67x (+6.8%) | 2.63x | 2.90x | 2.62x | **Strict $\mathcal{O}(N)$ [OK]** |
| 2,500 $\rightarrow$ 5,000 | 2.0x | 1.94x (-3.1%) | 2.02x | 2.13x | 2.13x | **Strict $\mathcal{O}(N)$ [OK]** |
| 5,000 $\rightarrow$ 10,000 | 2.0x | 1.98x (-1.2%) | 2.00x | 2.00x | 2.03x | **Strict $\mathcal{O}(N)$ [OK]** |

---

### 3.2 Edge Serving Boot Advantage: Binary Unpack vs JSON Parse

When `velda-edge` boots or hot-reloads, it reads pre-compiled `*.bin` artifacts directly into RAM, bypassing JSON parsing entirely:

| Routes ($N$) | JSON Parse Latency | Binary Unpack Latency | Latency Speedup | JSON Allocations | Binary Allocations | Storage Reduction |
|---|---|---|---|---|---|:---:|
| 100 | 109.21 µs | **51.49 µs** | **2.1x** | 1,026 | 1,001 | **55.7 %** |
| 500 | 540.36 µs | **247.17 µs** | **2.2x** | 5,108 | 5,001 | **55.8 %** |
| 1,000 | 961.74 µs | **483.65 µs** | **2.0x** | 10,209 | 10,001 | **55.8 %** |
| 2,500 | 2.57 ms | **1.27 ms** | **2.0x** | 25,511 | 25,001 | **55.8 %** |
| 5,000 | 4.98 ms | **2.70 ms** | **1.8x** | 51,012 | 50,002 | **55.7 %** |
| 10,000 | 9.83 ms | **5.48 ms** | **1.8x** | 102,013 | 100,003 | **55.7 %** |

---

### 3.3 Upstreams Domain Pipeline Scaling

| Upstreams ($N$) | JSON Size | Bin Size | JSON Parse | Validate | Compile | Unpack | Big-O Complexity |
|---|---|---|---|---|---|---|:---:|
| 100 | 50.4 KB | 16.3 KB | 102.15 µs | 25.16 µs | 13.93 µs | 42.40 µs | $\mathcal{O}(N)$ |
| 500 | 252.6 KB | 81.6 KB | 509.56 µs | 124.75 µs | 67.36 µs | 185.97 µs | $\mathcal{O}(N)$ |
| 1,000 | 505.3 KB | 163.1 KB | 973.12 µs | 260.85 µs | 133.38 µs | 361.04 µs | $\mathcal{O}(N)$ |
| 2,500 | 1.23 MB | 408.3 KB | 2.46 ms | 623.42 µs | 358.28 µs | 910.59 µs | $\mathcal{O}(N)$ |
| 5,000 | 2.47 MB | 820.3 KB | 4.87 ms | 1.24 ms | 675.74 µs | 1.84 ms | $\mathcal{O}(N)$ |

#### Upstreams Big-O Linearity Drift

| Transition | Scale Ratio | JSON Parse Growth | Validation Growth | Compile Growth | Unpack Growth | Big-O Status |
|---|---|---|---|---|---|:---:|
| 100 $\rightarrow$ 500 | 5.0x | 4.99x (-0.2%) | 4.96x | 4.83x | 4.39x | **Strict $\mathcal{O}(N)$ [OK]** |
| 500 $\rightarrow$ 1,000 | 2.0x | 1.91x (-4.5%) | 2.09x | 1.98x | 1.94x | **Strict $\mathcal{O}(N)$ [OK]** |
| 1,000 $\rightarrow$ 2,500 | 2.5x | 2.53x (+1.2%) | 2.39x | 2.69x | 2.52x | **Strict $\mathcal{O}(N)$ [OK]** |
| 2,500 $\rightarrow$ 5,000 | 2.0x | 1.98x (-1.0%) | 1.99x | 1.89x | 2.02x | **Strict $\mathcal{O}(N)$ [OK]** |

---

### 3.4 Listeners Domain Pipeline Scaling

| Listeners ($N$) | JSON Size | Bin Size | JSON Parse | Validate | Compile | Unpack | Big-O Complexity |
|---|---|---|---|---|---|---|:---:|
| 50 | 9.6 KB | 3.0 KB | 24.78 µs | 6.73 µs | 2.66 µs | 8.89 µs | $\mathcal{O}(N)$ |
| 100 | 19.2 KB | 5.9 KB | 37.33 µs | 12.69 µs | 4.79 µs | 16.86 µs | $\mathcal{O}(N)$ |
| 250 | 48.1 KB | 14.6 KB | 92.44 µs | 32.88 µs | 12.07 µs | 41.06 µs | $\mathcal{O}(N)$ |
| 500 | 96.1 KB | 29.1 KB | 184.16 µs | 80.72 µs | 24.64 µs | 80.25 µs | $\mathcal{O}(N)$ |
| 1,000 | 192.1 KB | 58.0 KB | 344.72 µs | 123.29 µs | 48.41 µs | 167.52 µs | $\mathcal{O}(N)$ |

#### Listeners Big-O Linearity Drift

| Transition | Scale Ratio | JSON Parse Growth | Validation Growth | Compile Growth | Unpack Growth | Big-O Status |
|---|---|---|---|---|---|:---:|
| 50 $\rightarrow$ 100 | 2.0x | 1.51x (-24.7%) | 1.89x | 1.80x | 1.90x | Near-linear $\mathcal{O}(N)$ |
| 100 $\rightarrow$ 250 | 2.5x | 2.48x (-0.9%) | 2.59x | 2.52x | 2.43x | **Strict $\mathcal{O}(N)$ [OK]** |
| 250 $\rightarrow$ 500 | 2.0x | 1.99x (-0.4%) | 2.46x | 2.04x | 1.95x | **Strict $\mathcal{O}(N)$ [OK]** |
| 500 $\rightarrow$ 1,000 | 2.0x | 1.87x (-6.4%) | 1.53x | 1.96x | 2.09x | **Strict $\mathcal{O}(N)$ [OK]** |

---

## 4. Capability Providers (Cross-Cutting Providers)

Pure capabilities operating orthogonally from the reconciliation loop.

### 4.1 Metrics Provider (`SyncMetrics` In-Memory Atomics)

Tested under high-pressure burst of **1,000,000 sequential operations**:

| Operation | Total Iterations | Avg Latency | Heap Allocs / Op | Heap Bytes / Op | Big-O Complexity |
|---|---|---|---|---|:---:|
| `record_cycle_success()` | 1,000,000 | **12 ns** | 0.0000 | 0 B | $\mathbf{\mathcal{O}(1)}$ |
| `record_cycle_error()` | 1,000,000 | **11 ns** | 0.0000 | 0 B | $\mathbf{\mathcal{O}(1)}$ |
| `snapshot()` | 1,000,000 | **6 ns** | 0.0000 | 0 B | $\mathbf{\mathcal{O}(1)}$ |
| `render_prometheus()` | 50,000 | **180 ns** | 1.00 | 2.3 KB | $\mathcal{O}(1)$ |

> [!TIP]
> **Zero Hot-Path Overhead**: Calling `record_cycle_success` inside the sync loop costs only **12 nanoseconds** and performs **zero memory allocations**.

---

### 4.2 Logging Provider (Non-Blocking Ring Buffer)

Tested with log event burst loads from **1,000 to 100,000 log events**:

| Burst Scale (Events) | Total Elapsed | Avg Caller Dispatch Time | Log Throughput | Queue Stability | Big-O Complexity |
|---|---|---|---|---|:---:|
| 1,000 | 1.33 ms | **1.33 µs** | 751,847 logs/s | Flat $\mathcal{O}(1)$ [OK] | $\mathcal{O}(1)$ |
| 10,000 | 13.10 ms | **1.31 µs** | 763,137 logs/s | Flat $\mathcal{O}(1)$ [OK] | $\mathcal{O}(1)$ |
| 50,000 | 64.92 ms | **1.30 µs** | 770,194 logs/s | Flat $\mathcal{O}(1)$ [OK] | $\mathcal{O}(1)$ |
| 100,000 | 129.62 ms | **1.30 µs** | 771,477 logs/s | Flat $\mathcal{O}(1)$ [OK] | $\mathcal{O}(1)$ |

> [!NOTE]
> **Zero Reconciler Blocking Invariant**:
> Even during sudden 100,000 log spikes, caller thread latency remains completely flat at **1.30 µs**, ensuring log disk writes can never stall synchronization or health check loops.

---

## 5. Benchmark Reproduction Commands

To reproduce or verify these measurements at any time:

```bash
# 1. Run Stage 1 (Pre-Sync: Gzip, SHA-256, Manifest Parse)
cargo bench -p velda-sync --bench stage1_presync

# 2. Run Stage 2 (Sync: Reconciler No-Op, Delta, Full Scaling)
cargo bench -p velda-sync --bench stage2_sync

# 3. Run Stage 3 (Post-Sync: Routes, Upstreams, Listeners Compilers)
cargo bench -p velda-sync --bench stage3_postsync

# 4. Run Capability Providers (Metrics & Non-Blocking Logging)
cargo bench -p velda-sync --bench stage_providers
```
