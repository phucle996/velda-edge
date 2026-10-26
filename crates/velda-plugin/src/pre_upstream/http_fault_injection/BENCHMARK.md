# HTTP Fault Injection Plugin (`http_fault_injection`) — Performance Benchmark Report

This document records the empirical performance benchmarks, live chaos engineering verification, short-circuit throughput capacity, and latency percentiles for the **`http_fault_injection`** plugin evaluated live across the Microsoft Azure Virtual Network cluster.

---

## 1. Test Environment & Topology

```
┌─────────────────────────────────┐                 ┌─────────────────────────────────┐
│     Client Node (Azure VM2)     │                 │     Gateway Node (Azure VM1)    │
│  - Standard D4s_v5 (4 vCPUs)    │  Private VNet   │  - Standard D4s_v5 (4 vCPUs)    │
│  - 16 GiB RAM, Linux 6.8        │ ───────────────>│  - 16 GiB RAM, Linux 7.0 Accel │
│  - Private VNet Interconnect    │  (< 0.3ms RTT)  │  - Private VNet Interconnect    │
│  - Tools: wrk, h2load, curl     │                 │  - velda-edge (:80 & :443)      │
│  - velda-bench-backend (:8081)  │                 │  - http_fault_injection Plugin  │
└─────────────────────────────────┘                 └─────────────────────────────────┘
```

- **Gateway Node**: Azure VM1 (`172.16.0.4`), 4 worker threads affined 1:1 to physical CPU cores, `RLIMIT_NOFILE=1048576`, Jemalloc memory allocator.
- **Client & Upstream Node**: Azure VM2 (`172.16.0.5`), 4 vCPUs running `velda-bench-backend` on port `8081` and `wrk` / `h2load`.
- **Wire Latency**: Private VNet wire RTT of ~0.15ms – 0.25ms.

---

## 2. Benchmark Summary & Latency Impact

### 2.1 HTTP/1.1 Multi-Scenario Matrix (`wrk -t4 -c50 -d5s --latency`)

| Scenario / Mode | Target Route / Injected Fault | Throughput (RPS) | Mean Latency | P50 (ms) | P90 (ms) | P99 (ms) | Gateway RSS | Status Codes |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Baseline Route** | `/api/canary/baseline/ping` (Zero Plugins) | **34,200.15 req/s** | 1.60 ms | 1.08 ms | 2.61 ms | 10.43 ms | 69.94 MB | 100% 200 OK |
| **Fast-Path Bypass** | `/api/fault/ping` (No QA Header) | **35,081.16 req/s** | 1.61 ms | 1.08 ms | 2.61 ms | 12.06 ms | 71.27 MB | 100% 200 OK |
| **Scope Active (0%)** | `/api/fault/ping` (`x-chaos-test: qa-stage`) | **34,419.25 req/s** | 1.63 ms | 1.12 ms | 2.60 ms | 11.23 ms | 71.54 MB | 100% 200 OK |
| **Synthetic Abort** | 100% Short-Circuit (`503 Service Unavailable`) | **78,687.56 req/s** | **0.78 ms** | **0.43 ms** | **1.30 ms** | **8.32 ms** | 71.85 MB | 100% 503 Abort |
| **Synthetic Delay** | 100% Latency Injection (**10ms Fixed Delay**) | **3,862.43 req/s** | 12.39 ms | 11.81 ms | 13.78 ms | 24.16 ms | 71.67 MB | 100% 200 OK |
| **Combined Chaos** | 10ms Fixed Delay + 503 Abort | **4,052.24 req/s** | 11.80 ms | 11.50 ms | 12.51 ms | 19.17 ms | 72.14 MB | 100% 503 Abort |

---

### 2.2 HTTP/2 Multiplexed Abort Profile (`h2load -n50000 -c50 -m20` over TLS 1.3)

| Scenario / Mode | Protocol / Framing | Throughput (RPS) | Mean Latency | Upstream Forwarded Requests | Gateway RSS |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **HTTP/2 Baseline** | TLS 1.3 / H2 Multiplexed (Clean) | **17,532.80 req/s** | 47.39 ms | 30,000 / 30,000 (100%) | 108.27 MB |
| **HTTP/2 503 Abort** | TLS 1.3 / H2 Short-Circuit | **28,924.02 req/s** | **30.36 ms** | **0 / 50,000 (0% Upstream Traffic)** | 101.44 MB |

---

## 3. Empirical Benchmark Details

### 3.1 Fast-Path Zero-Overhead Verification
```text
Running 5s test @ http://172.16.0.4:80/api/fault/ping
  4 threads and 50 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.61ms    1.64ms  19.52ms   93.22%
    Req/Sec     8.83k     1.30k   12.18k    63.50%
  Latency Distribution
     50%    1.08ms
     75%    1.55ms
     90%    2.61ms
     99%   12.06ms
  175893 requests in 5.01s, 25.66MB read
Requests/sec:  35081.16
Transfer/sec:      5.12MB
```

### 3.2 High-Throughput Short-Circuit Abort (>78,600 req/s)
```text
Running 5s test @ http://172.16.0.4:80/api/fault/ping
  4 threads and 50 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   778.52us    1.29ms  17.96ms   93.44%
    Req/Sec    19.78k     2.59k   25.86k    65.00%
  Latency Distribution
     50%  426.00us
     75%  653.00us
     90%    1.30ms
     99%    8.32ms
  394435 requests in 5.01s, 79.37MB read
  Non-2xx or 3xx responses: 394435
Requests/sec:  78687.56
Transfer/sec:     15.83MB
```

### 3.3 Non-Blocking Timer Precision Under Load (10ms Delay)
```text
Running 5s test @ http://172.16.0.4:80/api/fault/ping
  4 threads and 50 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    12.39ms    2.54ms  32.55ms   93.42%
    Req/Sec   974.79    206.13     1.41k    69.00%
  Latency Distribution
     50%   11.81ms
     75%   12.28ms
     90%   13.78ms
     99%   24.16ms
  19363 requests in 5.01s, 2.82MB read
Requests/sec:   3862.43
Transfer/sec:    577.89KB
```
