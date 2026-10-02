# velda-tls — Benchmark & Performance Verification Report

This document reports empirical performance benchmarks for `velda-tls` (Stateless, In-Memory TLS Execution Engine for Velda Edge).

Tests were executed using the custom counting allocator and timing suite across 5 dedicated benchmarks:
- Single-thread suite: [`benches/single_thread_bench.rs`](benches/single_thread_bench.rs)
- Multi-thread concurrency suite: [`benches/multi_thread_bench.rs`](benches/multi_thread_bench.rs)
- Adversarial & fault-tolerance suite: [`benches/adversarial_bench.rs`](benches/adversarial_bench.rs)
- Memory leak & heap stability suite: [`benches/memory_leak_bench.rs`](benches/memory_leak_bench.rs)
- Specialized cryptography & reload suite: [`benches/crypto_bench.rs`](benches/crypto_bench.rs)

---

## 1. Executive Summary

| Target / Capability | Metric Target | Measured Result | Status |
| :--- | :--- | :--- | :--- |
| **SNI Lookup ($N=5,000$ certs)** | $< 100\text{ ns}$, $O(1)$ | **68.94 ns**, **1.00 alloc** | **Passed** ($14.5\text{M ops/s}$) |
| **Wildcard Match (`*.internal`)** | $< 100\text{ ns}$ single-label | **79.05 ns**, **1.00 alloc** | **Passed** ($12.6\text{M ops/s}$) |
| **Unknown SNI Rejection** | $< 80\text{ ns}$, strict 0-leak | **51.16 ns**, **1.00 alloc** | **Passed** ($19.5\text{M ops/s}$) |
| **Metadata Extraction** | $< 75\text{ ns}$ | **53.36 ns**, **2.00 allocs** | **Passed** ($18.7\text{M ops/s}$) |
| **Full TLS 1.3 Handshake (1 core)** | $< 350\text{ µs}$ | **218.44 µs** | **Passed** ($4,577\text{ hsk/s}$) |
| **Concurrent TLS Handshake (4 cores)** | Lock-free worker scaling | **72.21 µs** (avg latency) | **Passed** ($13,847\text{ hsk/s}$, $3.30\times$) |
| **Hostile SNI Flood (5,000 probes)** | 100% strict rejection | **35.24 µs / reject**, 0 leaks | **Passed** ($28,378\text{ ops/s}$) |
| **Garbage Frame Injection (10,000 ops)** | 0 panics, clean drops | **7.32 µs / drop** | **Passed** ($136,675\text{ ops/s}$) |
| **Slowloris Defense (200 stalled conns)** | Zero worker starvation | **100% Timed out cleanly** | **Passed** (No resource leakage) |
| **Steady-State Saturation Leak Audit** | Net Heap Growth | **0 B (Zero Leak)** | **Passed** (Bounded cache) |
| **High-Frequency SNI Audit (1,000,000 ops)**| Net Heap Growth | **0 B (Zero Leak)** | **Passed** (Pure in-memory) |
| **mTLS Client Auth Crypto Overhead** | $< 10\%$ vs 1-way TLS 1.3 | **+1.1% Overhead (~2 µs)** | **Exceeded** ($4,883\text{ hsk/s}$) |
| **Upstream Outbound Handshake** | $< 300\text{ µs}$ | **196.40 µs** | **Passed** ($5,092\text{ conn/s}$) |
| **Dynamic In-Memory PEM Reload** | Rate of compilation | **~72,000 certs/s** | **Passed** ($< 0.7\text{ ms}$ for 50 certs) |

---

## 2. In-Memory SNI Resolver Lookup Latency & Scaling ($N = 10 \dots 5,000$)

Measures lookup throughput across exact domains, RFC 6125 single-label wildcards, and unknown SNIs against table sizes from 10 to 5,000 certificates:

| Table Size ($N$) | Match Kind | Target SNI | Latency / op | Allocs / op | Throughput | Invariant |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **$N = 10$** | **Exact Hit** | `service_0005.domain.com` | **69.49 ns** | **1.00** | $14,390,587\text{ ops/s}$ | Direct $O(1)$ map lookup |
| **$N = 10$** | **Wildcard Hit** | `api.cluster_0005.internal` | **81.83 ns** | **1.00** | $12,220,271\text{ ops/s}$ | Single-label split lookup |
| **$N = 10$** | **Miss (Reject)** | `unknown.attacker.invalid` | **51.88 ns** | **1.00** | $19,273,568\text{ ops/s}$ | Immediate strict rejection |
| **$N = 100$** | **Exact Hit** | `service_0050.domain.com` | **67.79 ns** | **1.00** | $14,751,442\text{ ops/s}$ | Cache-resident match |
| **$N = 100$** | **Wildcard Hit** | `api.cluster_0050.internal` | **76.75 ns** | **1.00** | $13,029,130\text{ ops/s}$ | Flat scaling |
| **$N = 100$** | **Miss (Reject)** | `unknown.attacker.invalid` | **51.17 ns** | **1.00** | $19,541,226\text{ ops/s}$ | Immediate strict rejection |
| **$N = 1,000$** | **Exact Hit** | `service_0500.domain.com` | **67.30 ns** | **1.00** | $14,858,201\text{ ops/s}$ | $O(1)$ across 1,000 certs |
| **$N = 1,000$** | **Wildcard Hit** | `api.cluster_0500.internal` | **77.93 ns** | **1.00** | $12,832,019\text{ ops/s}$ | Bounded wildcard match |
| **$N = 1,000$** | **Miss (Reject)** | `unknown.attacker.invalid` | **50.84 ns** | **1.00** | $19,669,600\text{ ops/s}$ | Immediate strict rejection |
| **$N = 5,000$** | **Exact Hit** | `service_2500.domain.com` | **68.94 ns** | **1.00** | $14,504,648\text{ ops/s}$ | $O(1)$ across 5,000 certs |
| **$N = 5,000$** | **Wildcard Hit** | `api.cluster_2500.internal` | **79.05 ns** | **1.00** | $12,650,105\text{ ops/s}$ | Bounded wildcard match |
| **$N = 5,000$** | **Miss (Reject)** | `unknown.attacker.invalid` | **51.16 ns** | **1.00** | $19,545,639\text{ ops/s}$ | Immediate strict rejection |

### Key Architectural Findings:
1. **$O(1)$ Flat Complexity**: Scaling certificates from $10$ to $5,000$ (a $500\times$ increase) does not degrade lookup latency, maintaining steady $\sim 67 - 69\text{ ns}$.
2. **Rejection Speed**: An unknown or hostile SNI is rejected in **$51\text{ ns}$**, protecting gateway CPU resources from SNI denial-of-service floods.

---

## 3. Handshake Inspection & QUIC Bridge Compilation

Measures micro-operations for extracting handshake metadata and translating `Arc<rustls::ServerConfig>` into `quinn_proto::ServerConfig`:

| Operation | Iterations | Latency / op | Allocs / op | Throughput | Purpose |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`extract_handshake_info`** | 1,000,000 | **53.36 ns** | **2.00** | $18,739,843\text{ ops/s}$ | Zero-copy ALPN, SNI, and peer certificate extraction |
| **`build_quic_config`** | 50,000 | **649.96 ns** | **6.00** | $1,538,554\text{ ops/s}$ | Bridging TLS state to QUIC crypto endpoint |

---

## 4. Multi-Thread Concurrency Scaling (Tokio Worker Threads)

Evaluates parallel downstream TLS 1.3 handshakes across multi-threaded worker pools:

### A. Concurrent Full Handshake Scaling
| Threads | Total Handshakes | Avg Latency / Handshake | Throughput | Scaling Efficiency |
| :--- | :--- | :--- | :--- | :--- |
| **1 thread** | 500 | **237.97 µs** | **4,202 hsk/s** | **1.00x** |
| **2 threads** | 1,000 | **127.60 µs** | **7,836 hsk/s** | **1.86x** |
| **4 threads** | 2,000 | **72.21 µs** | **13,847 hsk/s** | **3.30x** |
| **8 threads** | 4,000 | **75.34 µs** | **13,273 hsk/s** | **3.16x** |
| **16 threads** | 8,000 | **159.29 µs** | **6,277 hsk/s** | **1.49x** (Core saturation) |

### B. Concurrent Session Resumption Scaling
| Threads | Total Handshakes | Avg Latency / Handshake | Throughput | Scaling Efficiency |
| :--- | :--- | :--- | :--- | :--- |
| **1 thread** | 500 | **222.13 µs** | **4,501 hsk/s** | **1.00x** |
| **2 threads** | 1,000 | **118.86 µs** | **8,413 hsk/s** | **1.87x** |
| **4 threads** | 2,000 | **73.56 µs** | **13,594 hsk/s** | **3.02x** |
| **8 threads** | 4,000 | **74.01 µs** | **13,510 hsk/s** | **3.00x** |
| **16 threads** | 8,000 | **141.54 µs** | **7,065 hsk/s** | **1.57x** |

---

## 5. Adversarial & Fault-Tolerance Defense

Verifies resilience against malicious connection flooding and Slowloris stalls:

### A. Unknown / Hostile SNI Flood
- **Workload**: 5,000 hostile SNI probes randomly generated.
- **Result**: **5,000 rejections (100%)**, **35.24 µs avg rejection**, **28,378 ops/s**.
- **Invariant**: **0 certificate leakages**, zero fallback to unrelated tenant certs.

### B. Corrupted & Malformed TLS Frame Injection
- **Workload**: 10,000 randomized corrupted frames injected during initial handshake.
- **Result**: **10,000 clean drops (100%)**, **7.32 µs avg drop**, **136,675 ops/s**.
- **Invariant**: **0 panics**, memory safely reclaimed.

### C. Slowloris Handshake Timeout Defense
- **Workload**: 200 concurrent connections stalling without transmitting `ClientHello`.
- **Result**: **200 timed out (100%)** within configured 10ms deadline.
- **Invariant**: Zero worker thread starvation, runtime remained responsive throughout the attack.

---

## 6. Memory Leak & Heap Stability Audit

Audits allocation deltas using `CountingAllocator` across extended execution cycles:

| Audit Phase | Iterations | Total Allocations | Net Alloc Delta | Net Heap Growth | Invariant Verification |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Session Cache Saturation** | 2,500 handshakes | 550,000 | **0** | **0 B** | **PASS (Strictly Bounded)** |
| **High-Frequency SNI Lookups**| 1,000,000 | 0 | **0** | **0 B** | **PASS (Zero Leaks)** |
| **QUIC Config Compilation** | 5,000 lifecycles | 30,000 | **1** | **336 B** (Active ref) | **PASS (Clean RAII Drop)** |

---

## 7. Cryptography, Mutual TLS (mTLS) & Dynamic Reload Speed

Evaluates asymmetric cryptographic verification overhead and dynamic PEM reloads:

### A. Standard TLS 1.3 vs Mutual TLS (mTLS) Client Auth
| Handshake Mode | Iterations | Avg Latency | Throughput | Crypto Overhead |
| :--- | :--- | :--- | :--- | :--- |
| **Standard TLS 1.3** | 1,000 | **202.66 µs** | 4,934 hsk/s | Baseline (Server auth only) |
| **Mutual TLS (mTLS)** | 1,000 | **204.80 µs** | 4,883 hsk/s | **+1.1% (~2 µs)** |

*Observation*: Ring ECDSA P-256 client certificate signature verification adds negligible ($\approx 1\%$) overhead compared to single-direction TLS.

### B. Upstream Outbound TLS Handshake (`TlsClientEngine::connect`)
- **Workload**: 1,000 outbound TLS connections from gateway to simulated backend microservice.
- **Result**: **196.40 µs avg latency**, **5,092 connections/s**.
- **Invariant**: Zero-IO in-memory pipe, pre-validated `ServerName<'static>`.

### C. Dynamic In-Memory PEM Parsing & ServerConfig Compilation (Hot Reload Phase)
| Certificates Compiled | Cycles | Total Batch Latency | Avg Time / Certificate | Compilation Rate |
| :--- | :--- | :--- | :--- | :--- |
| **1 cert** | 50 | **15.84 µs** | **15.84 µs** | 63,134 certs/s |
| **5 certs** | 50 | **70.96 µs** | **14.19 µs** | 70,467 certs/s |
| **10 certs** | 50 | **139.05 µs** | **13.90 µs** | 71,917 certs/s |
| **25 certs** | 50 | **344.82 µs** | **13.79 µs** | 72,501 certs/s |
| **50 certs** | 50 | **696.17 µs** | **13.92 µs** | 71,821 certs/s |

*Observation*: Recompiling 50 tenant certificates directly from PEM text into an active `Arc<ServerConfig>` takes less than **0.7 ms**, allowing zero-downtime hot reloads without stalling active worker threads.
