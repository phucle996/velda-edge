# Performance Benchmark (`consumer_quota`)

Production stress-test benchmark performed on Azure Infrastructure comparing Velda Edge against Kong Gateway 3.9.0 and Aurora API Gateway.

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
| **Velda Edge + Consumer Quota** | **32,840.2 req/s** | **3.14 ms** | **2.68 ms** | **10.12 ms** | **96.6%** (-3.4% overhead) |
| **Velda Edge + Quota & Metering** | **32,192.7 req/s** | **3.28 ms** | **2.80 ms** | **11.30 ms** | **94.7%** (-5.3% overhead) |
| **Aurora Gateway (Raw Baseline)** | 25,654.3 req/s | 4.00 ms | 3.37 ms | 11.35 ms | 75.5% of Velda Raw |
| **Kong Gateway (Raw Baseline)** | 24,673.3 req/s | 4.16 ms | 3.35 ms | 11.74 ms | 72.6% of Velda Raw |
| **Kong Gateway + Rate-Limiting** | 16,564.9 req/s | 6.44 ms | 5.63 ms | 23.56 ms | <span style="color:red">**-32.9% penalty**</span> |

---

## 3. Analysis & Key Takeaways

1. **Near-Zero Degradation (-3.4%)**:
   - Single-window consumer quota evaluation reduces raw proxy throughput by only **3.4%** (~1,143 RPS), with median latency increasing by just **90 microseconds**.
2. **Double the Speed of Kong (+94.3%)**:
   - Kong Gateway suffers severe degradation (-32.9%) under in-memory rate limiting due to OpenResty `lua_shared_dict` global spinlock contention.
   - Velda Edge's lock-striped cacheline-aligned sharding enables it to outpace Kong by **+94.3%** under equivalent load.
3. **P99 Stability**:
   - Kong's P99 latency jumps from 11.74 ms to 23.56 ms due to LuaJIT garbage collection cycles.
   - Velda Edge maintains rock-solid P99 latency (10.12 ms) with zero memory allocations on the hot path.
