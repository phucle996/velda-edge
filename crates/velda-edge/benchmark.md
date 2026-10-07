# velda-edge — Benchmark & Performance Verification Report

This document reports empirical performance benchmarks for `velda-edge` (Composition Root, Shared Runtime & Dispatch Table Engine).

Tests were executed using the custom counting allocator and timing suite in:
- Single-thread suite: [`benches/single_thread_bench.rs`](benches/single_thread_bench.rs)

---

## 1. Executive Summary

| Target / Capability | Metric / Target | Measured Result | Status |
| :--- | :--- | :--- | :--- |
| **`SharedRuntime::load()` (100 Listeners)** | < 15 ns, 0 allocs | **12.13 ns**, **0.00 allocs** | **Passed** (82.5 M ops/s) |
| **`SharedRuntime::load()` (1,000 Listeners)** | < 15 ns, 0 allocs | **12.15 ns**, **0.00 allocs** | **Passed** (82.3 M ops/s) |
| **`SharedRuntime::load()` (5,000 Listeners)** | < 15 ns, 0 allocs | **12.18 ns**, **0.00 allocs** | **Passed** (82.1 M ops/s, Flat $O(1)$) |
| **`PipelineTable` HTTP/1.1 Cleartext Hit** | < 15 ns, 0 allocs | **8.05 ns**, **0.00 allocs** | **Exceeded** (124.2 M ops/s) |
| **`PipelineTable` HTTP/2 TLS Hit** | < 15 ns, 0 allocs | **10.21 ns**, **0.00 allocs** | **Passed** (98.0 M ops/s) |
| **`PipelineTable` gRPC Cleartext Hit** | < 15 ns, 0 allocs | **10.72 ns**, **0.00 allocs** | **Passed** (93.3 M ops/s) |
| **`PipelineTable` HTTP/3 QUIC Hit** | < 15 ns, 0 allocs | **10.63 ns**, **0.00 allocs** | **Passed** (94.0 M ops/s) |
| **`PipelineTable` Deterministic Miss** | < 15 ns, 0 allocs | **6.83 ns**, **0.00 allocs** | **Exceeded** (146.4 M ops/s) |
| **`RuntimeProfile::from_hardware` (RAM)** | < 200 ns | **90.38 ns**, **3.00 allocs** | **Passed** (11.1 M ops/s) |
| **`to_dns_resolver_config` Conversion** | < 5 ns, 0 allocs | **1.45 ns**, **0.00 allocs** | **Exceeded** (691.6 M ops/s) |
| **`resolve_runtime_profile` (Cold/Disk)** | Cold bootstrap file I/O | **16.48 µs**, **136 allocs** | **Passed** (Non-hot-path cold start) |
| **Ingress Active Bindings (N = 5,000)** | Linear 1 alloc / binding | **329.70 µs**, **5,001 allocs** | **Passed** (Bootstrap/Sync path only) |

---

## 2. Shared Runtime Lock-Free Read Latency (`SharedRuntime::load()`)

Evaluates hot-path reading of the active `RuntimeSnapshot` across tables containing up to 5,000 listeners, upstreams, and routes:

| Table Size | Scenario | Latency / op | Allocs / op | Throughput | Invariant Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **N = 100** | ArcSwap lock-free read | **12.13 ns** | **0.00** | **82,465,145 ops/s** | **PASS (< 15 ns, 0 allocs)** |
| **N = 1,000** | ArcSwap lock-free read | **12.15 ns** | **0.00** | **82,294,437 ops/s** | **PASS (< 15 ns, 0 allocs)** |
| **N = 5,000** | ArcSwap lock-free read | **12.18 ns** | **0.00** | **82,104,737 ops/s** | **PASS (< 15 ns, 0 allocs)** |

> **Key Invariant**: Regardless of whether the gateway holds 100 or 5,000 configured listeners, `SharedRuntime::load()` maintains a flat, strictly lock-free $\sim 12.1$ ns read latency with **0.00 memory allocations**.

---

## 3. Protocol Dispatch Table Lookup (`PipelineTable`)

Measures per-connection protocol pipeline lookup directly from the runtime dispatch table:

| Protocol Target | Lookup Type | Target ID | Latency / op | Allocs / op | Throughput |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **HTTP/1.1 Cleartext** | TCP Hit | `listener_http1_0000` | **8.05 ns** | **0.00** | **124,230,979 ops/s** |
| **HTTP/2 TLS** | TCP Hit | `listener_http2_0005` | **10.21 ns** | **0.00** | **97,951,606 ops/s** |
| **gRPC Cleartext** | TCP Hit | `listener_grpc_0009` | **10.72 ns** | **0.00** | **93,310,500 ops/s** |
| **HTTP/3 QUIC** | UDP Hit | `listener_http3_0007` | **10.63 ns** | **0.00** | **94,049,630 ops/s** |
| **Deterministic Miss** | TCP Miss | `listener_nonexistent_9999` | **6.83 ns** | **0.00** | **146,415,724 ops/s** |

---

## 4. Hardware Adaptation & Runtime Profile Resolution

Measures hardware tier probing, JSON deep-merge resolution, and conversion to downstream subsystem configs:

| Scenario | Execution Phase | Latency / op | Allocs / op | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **`from_hardware` (RAM)** | In-Memory Hardware Probe | **90.38 ns** | **3.00** | **11,064,906 ops/s** |
| **`to_dns_resolver_config`** | Struct Conversion | **1.45 ns** | **0.00** | **691,570,106 ops/s** |
| **`resolve_runtime_profile`** | Disk I/O + JSON Merge | **16.48 µs** | **135.99** | **60,667 ops/s** |

---

## 5. Ingress Transformation Latency (`RuntimeConfig::active_bindings()`)

Evaluates translating configuration listeners into transport bindings during control plane sync / startup:

| Listener Count | Total Bindings | Latency / op | Allocs / op | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **N = 100** | 100 ingress ports | **6.67 µs** | **101.00** | **150,023 ops/s** |
| **N = 1,000** | 1,000 ingress ports | **63.34 µs** | **1,001.00** | **15,786 ops/s** |
| **N = 5,000** | 5,000 ingress ports | **329.70 µs** | **5,001.00** | **3,033 ops/s** |
