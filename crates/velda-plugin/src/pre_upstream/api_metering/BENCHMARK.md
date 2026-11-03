# Performance Benchmark (`api_metering`)

Production stress-test benchmark performed on Azure Infrastructure measuring micro-credit deduction, tier routing, and monthly window accounting under load.

---

## 1. Test Environment

- **Gateway Node (VM1)**: Azure Standard D4s v5 (4 vCPUs AMD EPYC 7763, 16 GiB RAM), Ubuntu 24.04 LTS (Kernel 7.0.0-1017-azure).
- **Client Generator (VM2)**: Azure Standard D4s v5 (4 vCPUs AMD EPYC 7763, 16 GiB RAM).
- **Upstream Backend**: High-performance HTTP/1.1 echo server on VM2 (`172.16.0.5:8081`).
- **Load Generation Tool**: `wrk -t4 -c100 -d10s --latency` (4 threads, 100 persistent connections, 10-second duration).

---

## 2. Benchmark Results Matrix

| Architecture / Scenario | Throughput (RPS) | Avg Latency | P50 Latency | P99 Latency | Performance vs Raw |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge (Raw Baseline)** | **33,983.6 req/s** | **3.03 ms** | **2.59 ms** | **9.64 ms** | **100.0%** (Baseline) |
| **Velda Edge + API Metering** | **32,710.4 req/s** | **3.17 ms** | **2.71 ms** | **10.45 ms** | **96.3%** (-3.7% overhead) |
| **Velda Edge + Quota & Metering** | **32,192.7 req/s** | **3.28 ms** | **2.80 ms** | **11.30 ms** | **94.7%** (-5.3% overhead) |
| **Aurora Gateway (Raw Baseline)** | 25,654.3 req/s | 4.00 ms | 3.37 ms | 11.35 ms | 75.5% of Velda Raw |
| **Kong Gateway (Raw Baseline)** | 24,673.3 req/s | 4.16 ms | 3.35 ms | 11.74 ms | 72.6% of Velda Raw |
| **Kong Gateway + Rate-Limiting** | 16,564.9 req/s | 6.44 ms | 5.63 ms | 23.56 ms | <span style="color:red">**-32.9% penalty**</span> |

---

## 3. Analysis & Key Takeaways

1. **Ultra-Low Accounting Overhead (-3.7%)**:
   - Deducting micro-credits and updating monthly windows on every request introduces only **3.7% overhead** (~1,273 RPS) compared to a completely bare proxy.
2. **Deterministic Response Generation**:
   - When a tenant exhausts their credit balance, `Action::Respond(402)` short-circuits in $< 5 \mu\text{s}$, completely avoiding upstream network hops, DNS lookups, and socket leasing.
3. **Superiority over Traditional Gateways**:
   - Velda Edge running full-stack monetization and quota enforcement achieves **32,192.7 req/s**, maintaining a **+94.3% throughput lead** over Kong Gateway (16,564.9 req/s).
