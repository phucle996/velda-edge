# Resource Limit Plugin (`resource_limit`) — Performance Benchmark Report

This document records the empirical performance benchmarks, adversarial attack mitigation metrics, throughput capacity, and latency percentiles for the hardened, branch-minimized **`resource_limit`** plugin deployed across the Azure Virtual Network cluster.

---

## 1. Test Environment & Topology

```
┌─────────────────────────────────┐                 ┌─────────────────────────────────┐
│     Client Node (Azure VM2)     │                 │     Gateway Node (Azure VM1)    │
│  - Standard D4s_v5 (4 vCPUs)    │  Private VNet   │  - Standard D4s_v5 (4 vCPUs)    │
│  - 16 GiB RAM, Linux 6.8        │ ───────────────>│  - 16 GiB RAM, Linux 7.0 Accel │
│  - Private VNet Interconnect    │  (< 0.5ms RTT)  │  - Private VNet Interconnect    │
│  - Tools: wrk, python3          │                 │  - velda-edge + velda-bench-be  │
└─────────────────────────────────┘                 └─────────────────────────────────┘
```

- **Gateway Node**: Azure VM1 (`<GATEWAY_VM_IP>`), 4 worker threads affined 1:1 to physical CPU cores, `RLIMIT_NOFILE=1048576`, Datacenter Jemalloc profile.
- **Client Node**: Azure VM2 (`<CLIENT_VM_IP>`), executing `wrk` (50 concurrent keep-alive connections, 4 threads) and custom raw socket attack suites.
- **Backend Upstream**: `velda-bench-backend` running on VM2 port 8081 (`/healthz` 4-byte `pong` response).

---

## 2. Benchmark Summary & Latency Impact

| Benchmark Metric | Baseline Route (Zero Plugins) | Clean Traffic (`resource_limit` Active) | Adversarial Rejection (`414 URI Too Long`) | Delta / Overhead |
| :--- | :---: | :---: | :---: | :---: |
| **Peak Throughput (`wrk`)** | **41,372.74 RPS** | **41,515.11 RPS** | **79,294.81 RPS** | **+0.34% (Zero Degradation)** |
| **Transfer Rate** | 6.04 MB/s | 6.06 MB/s | 14.67 MB/s | — |
| **Average Latency (`wrk`)** | 1.30 ms | 1.33 ms | 647.54 µs | +30 µs (Clean) / -652 µs (Blocked) |
| **P50 Latency** | **1.01 ms** | **1.02 ms** | **457.00 µs** | **+10 µs (Within measurement variance)** |
| **P75 Latency** | 1.28 ms | 1.28 ms | 620.00 µs | **0 µs (Identical)** |
| **P90 Latency** | 1.65 ms | 1.67 ms | 890.00 µs | +20 µs |
| **P99 Latency** | 10.98 ms | 11.58 ms | 5.68 ms | +600 µs |
| **Heap Allocations** | 0 | 0 | 0 | **0 bytes (Stack Only)** |

---

## 3. Empirical Benchmark Details

### 3.1 Clean Traffic Serving: Baseline vs Protected Route

#### Baseline (No Plugin) — `wrk -t4 -c50 -d10s`
```text
Running 10s test @ http://<GATEWAY_HOST>/api/resourcelimit/baseline
  4 threads and 50 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.30ms    1.65ms  24.75ms   96.75%
    Req/Sec    10.47k   689.81    20.30k    89.78%
  Latency Distribution
     50%    1.01ms
     75%    1.28ms
     90%    1.65ms
     99%   10.98ms
  417811 requests in 10.10s, 60.96MB read
Requests/sec:  41372.74
Transfer/sec:      6.04MB
```

#### Protected (`resource_limit` Active) — `wrk -t4 -c50 -d10s`
```text
Running 10s test @ http://<GATEWAY_HOST>/api/resourcelimit/protected
  4 threads and 50 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.33ms    1.69ms  30.25ms   96.57%
    Req/Sec    10.43k   440.07    11.37k    70.25%
  Latency Distribution
     50%    1.02ms
     75%    1.28ms
     90%    1.67ms
     99%   11.58ms
  415309 requests in 10.00s, 60.60MB read
Requests/sec:  41515.11
Transfer/sec:      6.06MB
```

### 3.2 Peak 414 URI Too Long Attack Mitigation Throughput

```text
Running 10s test @ http://<GATEWAY_HOST>/api/resourcelimit/protected?overflow=...
  4 threads and 50 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   647.54us    1.21ms  22.35ms   97.67%
    Req/Sec    20.02k     1.18k   34.10k    89.55%
  Latency Distribution
     50%  457.00us
     75%  620.00us
     90%    0.89ms
     99%    5.68ms
  800885 requests in 10.10s, 148.17MB read
  Non-2xx or 3xx responses: 800885
Requests/sec:  79294.81
Transfer/sec:     14.67MB
```

---

## 4. Live Adversarial Attack Scenarios & Verification on Azure

| Scenario # | Attack Vector & Payload | Threshold Config | Live Result on Azure | Defense Mechanism |
| :---: | :--- | :--- | :---: | :--- |
| **1** | Clean Request within bounds | Limits: URI 2048, Hdr 32, Tot 16KB, Sgl 4KB | **200 OK** | Fast-path passthrough |
| **2** | Query String Overflow (`?q=A*2500`) | `max_uri_length: 2048` | **414 URI Too Long**<br>`Cache-Control: no-store` | Terminated at `pre_route`; CPDoS defense active |
| **3** | Header Count Bombing (35 headers) | `max_headers_count: 32` | **431 Request Header Fields Too Large**<br>`Cache-Control: no-store` | Pre-loop threshold check blocks before byte scan |
| **4** | Header Name Bombing (4500 byte name) | `max_single_header_bytes: 4096` | **431 Request Header Fields Too Large**<br>`Cache-Control: no-store` | Name length evaluated against single header threshold |
| **5** | Single Header Value Overflow (5000 bytes) | `max_single_header_bytes: 4096` | **431 Request Header Fields Too Large**<br>`Cache-Control: no-store` | Single value bound enforced in single pass |
| **6** | Multi-Value Fragmentation Bypass (3 x 1500 byte `Cookie` lines = 4500 B) | `max_single_header_bytes: 4096` | **431 Request Header Fields Too Large**<br>`Cache-Control: no-store` | Aggregates consecutive identical header keys |
