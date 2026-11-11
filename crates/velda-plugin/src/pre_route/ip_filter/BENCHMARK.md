# Performance Benchmark Report: IP Allow/Deny Filter Plugin (`ip_filter`)

Performance benchmark specification and evaluation framework for the `ip_filter` plugin across **Velda Edge**, **Kong Gateway 3.9**, **Apache APISIX 3.10**, and **Envoy Proxy 1.31**.

---

## 1. Test Environment & Measurement Methodology

### 1.1 Production-Grade Hardware & Network Testbed

*[TBD]*

---

### 1.2 Standardized 5-Tier Metric Taxonomy

All benchmark scenarios are evaluated across five standardized, non-abbreviated metric tiers:

1. **Tier 1: Macro Network, L7 & IP Filtering Delivery Metrics**:
   - **Duration (seconds)**: Continuous execution duration of the benchmark session (standard 15.00 seconds).
   - **Total Requests**: Total downstream requests successfully served.
   - **Throughput (requests/second)**: Processing throughput rate.
   - **Wire Bandwidth (Megabytes/second)**: Aggregate data transfer rate across the network interface.
   - **Total IP Addresses Evaluated**: Total client IP addresses matched against allow/deny rules.
   - **Blocked Requests Count**: Total unauthorized client requests rejected with 403 Forbidden.
   - **Rejection Rate (rejections/second)**: Rate of unauthorized IP requests short-circuited before routing.
   - **Mean Latency (milliseconds)**: Average time from request receipt to response completion.
   - **Min Latency (milliseconds)**: Minimum recorded latency.
   - **Median Latency P50 (milliseconds)**: 50th percentile response latency.
   - **P75 Latency (milliseconds)**: 75th percentile response latency.
   - **P90 Latency (milliseconds)**: 90th percentile response latency.
   - **P99 Tail Latency (milliseconds)**: 99th percentile response latency.
   - **P99.9 Tail Latency (milliseconds)**: 99.9th percentile deep tail latency.
   - **Max Latency (milliseconds)**: Maximum recorded response latency.
   - **Latency Standard Deviation (milliseconds)**: Jitter and dispersion of response latency.
   - **Connect Time Average (milliseconds)**: Downstream connection handshake duration.
   - **HTTP Status Code Distribution (2xx / 4xx / 5xx)**: Proportional breakdown of response status codes.
   - **Success Rate (%)**: Percentage of requests successfully completed without connection resets or drops.
2. **Tier 2: System Resource Footprint**:
   - **Total CPU Utilization (% CPU)**: Normalized aggregate CPU utilization across all allocated processor cores.
   - **User CPU (% User CPU)**: CPU time spent in userspace logic.
   - **System / Kernel CPU (% System / Kernel CPU)**: CPU time spent in kernel syscalls and socket I/O.
   - **SoftIRQ CPU (% SoftIRQ CPU)**: CPU time spent processing network interrupt handlers.
   - **Frontline Worker 0 CPU (%)**: Core 0 request-serving worker CPU load.
   - **Frontline Worker 1 CPU (%)**: Core 1 request-serving worker CPU load.
   - **Shadow Logistics Worker CPU (%)**: Dedicated background worker CPU load running under `SCHED_IDLE`.
   - **Peak Resident Set Size (Megabytes)**: Maximum non-swapped physical memory footprint (`VmHWM`).
   - **RAM Usage (%)**: Percentage of total system memory consumed.
   - **Heap Allocations per Request (Bytes / Count)**: Dynamic memory allocation rate on the serving hot-path.
   - **Memory Leak Rate (Megabytes / minute)**: Long-term memory drift over sustained execution.
3. **Tier 3: Micro Linux Kernel & OS Telemetry via `perf stat` & tracepoints**:
   - **System Calls per Request (System Calls / Request)**: Average syscall overhead per request.
   - **Context Switches per Request (Context Switches / Request)**: Average thread/process context switch rate.
   - **Total System Calls**: Aggregate transitions into the Linux kernel.
   - **Total Context Switches**: Aggregate involuntary and voluntary scheduler context switches.
   - **Kernel Time (seconds)**: Total cumulative time spent inside the Linux kernel.
   - **Futex Calls**: Number of user-space locking and synchronization sleep/wake syscalls.
   - **Futex Percentage of Total System Calls (%)**: Ratio of futex contention syscalls to total syscalls.
4. **Tier 4: Micro High-Performance Computing & CPU Hardware Counters via `perf stat`**:
   - **Total CPU Instructions**: Aggregate retired machine instructions executed by the CPU.
   - **Instructions per Request (Instructions / Request)**: Average retired instructions per request.
   - **Total CPU Cycles**: Aggregate unhalted hardware clock cycles consumed.
   - **Cycles per Request (Cycles / Request)**: Average CPU clock cycles consumed per request.
   - **Instructions Per Cycle (IPC)**: Hardware instruction pipeline execution efficiency.
   - **Total Branches**: Aggregate hardware conditional and unconditional branch instructions.
   - **Branches per Request (Branches / Request)**: Average branch instructions per request.
   - **Branch Mispredictions**: Hardware branch prediction misses.
   - **Branch Misprediction Rate (%)**: Ratio of branch misses to total branches.
   - **L1 Data Cache Miss Rate (%)**: Level 1 data cache miss ratio.
   - **Last Level Cache Miss Rate (%)**: Level 3 (LLC) cache miss ratio.
5. **Tier 5: Plugin-Specific Mechanics & Wire Efficiency**:
   - **IPv4 LPM Trie / Radix Tree Lookup Latency (nanoseconds)**: CPU latency to evaluate IPv4 32-bit address in longest-prefix match trie.
   - **IPv6 128-Bit LPM Trie Lookup Latency (nanoseconds)**: CPU latency to evaluate IPv6 128-bit address in longest-prefix match trie.
   - **Radix Tree / Bitwise Trie Memory Footprint (Kilobytes)**: Resident memory consumed by compiled IP lookup data structures.
   - **Client SocketAddr Zero-Allocation Parse Overhead (nanoseconds)**: Overhead to extract binary IP address from downstream connection handle.
   - **Immediate 403 Rejection Short-Circuit Latency (nanoseconds)**: Time required to synthesize and transmit 403 Forbidden without upstream dispatch.
   - **CIDR Rule In-Memory Traversal Depth (levels)**: Average bitwise tree depth traversed per client lookup.
   - **Cacheline Efficiency During LPM Trie Branching (% L1 Miss)**: CPU Level 1 cache miss percentage during IP trie traversal.

---

## 2. Focused Benchmark Scenarios

To focus directly on essential operational requirements, the evaluation framework defines five targeted scenarios:
- **Scenario 01: Baseline Single-IP Exact Match Allowlist (5,000 req/s)** — Measure added baseline latency overhead when validating client IPs against a small exact-match allowlist under light load.
- **Scenario 02: Nominal Production Load with Large CIDR Table (40,000 req/s, 10,000 CIDR Rules)** — Evaluate sustained serving stability, tail latency, and resource footprint when evaluating 10,000 active CIDR subnets.
- **Scenario 03: Peak Saturation Under Mixed Traffic (100,000+ req/s, 50% Allowed, 50% Denied)** — Extreme stress testing to verify LPM trie throughput and immediate 403 rejection efficiency under heavy mixed traffic saturation.
- **Scenario 04: Deep IPv6 LPM Trie Traversal (Complex /64 and /48 Subnets)** — Evaluate 128-bit IPv6 bitwise tree traversal throughput and cacheline behavior under heterogeneous IPv6 address distributions.
- **Scenario 05: Massive Malicious IP Flood Rejection (Early 403 Forbidden Drop)** — Simulate 100% malicious IP traffic flood to verify that 403 short-circuits consume minimal CPU and produce zero memory leaks.

---

## 3. Detailed Benchmark Results

---

### Scenario 01: Baseline Single-IP Exact Match Allowlist (5,000 req/s)
- **Objective**: Determine baseline latency addition and CPU footprint when matching client IPs under low concurrency.
- **Execution Command**: `oha -z 15s -q 5000 -c 50 http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total IP Addresses Evaluated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Blocked Requests Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Rejection Rate (rejections/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Mean Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Min Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Median Latency P50 (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P75 Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P90 Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P99 Tail Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P99.9 Tail Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Max Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Latency Standard Deviation (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Connect Time Average (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **HTTP Status Code Distribution (2xx / 4xx / 5xx)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Success Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Utilization (% CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **User CPU (% User CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **System / Kernel CPU (% System / Kernel CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **SoftIRQ CPU (% SoftIRQ CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Frontline Worker 0 CPU (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Frontline Worker 1 CPU (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Shadow Logistics Worker CPU (%)** | *[TBD]* | N/A | N/A | N/A |
| **Peak Resident Set Size (Megabytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **RAM Usage (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Heap Allocations per Request (Bytes / Count)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Memory Leak Rate (Megabytes / minute)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **System Calls per Request (System Calls / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Context Switches per Request (Context Switches / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total System Calls** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Context Switches** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Kernel Time (seconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Futex Calls** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Futex Percentage of Total System Calls (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Instructions** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions per Request (Instructions / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Cycles** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cycles per Request (Cycles / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions Per Cycle (IPC)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Branches** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branches per Request (Branches / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branch Mispredictions** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branch Misprediction Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **L1 Data Cache Miss Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Last Level Cache Miss Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **IPv4 LPM Trie / Radix Tree Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **IPv6 128-Bit LPM Trie Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Radix Tree / Bitwise Trie Memory Footprint (Kilobytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Client SocketAddr Zero-Allocation Parse Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Immediate 403 Rejection Short-Circuit Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **CIDR Rule In-Memory Traversal Depth (levels)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cacheline Efficiency During LPM Trie Branching (% L1 Miss)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 02: Nominal Production Load with Large CIDR Table (40,000 req/s, 10,000 CIDR Rules)
- **Objective**: Measure sustained throughput, latency profile, and resource consumption when evaluating complex CIDR rule tables.
- **Execution Command**: `oha -z 15s -q 40000 -c 100 http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total IP Addresses Evaluated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Blocked Requests Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Rejection Rate (rejections/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Mean Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Min Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Median Latency P50 (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P75 Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P90 Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P99 Tail Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P99.9 Tail Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Max Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Latency Standard Deviation (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Connect Time Average (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **HTTP Status Code Distribution (2xx / 4xx / 5xx)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Success Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Utilization (% CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **User CPU (% User CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **System / Kernel CPU (% System / Kernel CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **SoftIRQ CPU (% SoftIRQ CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Frontline Worker 0 CPU (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Frontline Worker 1 CPU (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Shadow Logistics Worker CPU (%)** | *[TBD]* | N/A | N/A | N/A |
| **Peak Resident Set Size (Megabytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **RAM Usage (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Heap Allocations per Request (Bytes / Count)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Memory Leak Rate (Megabytes / minute)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **System Calls per Request (System Calls / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Context Switches per Request (Context Switches / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total System Calls** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Context Switches** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Kernel Time (seconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Futex Calls** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Futex Percentage of Total System Calls (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Instructions** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions per Request (Instructions / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Cycles** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cycles per Request (Cycles / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions Per Cycle (IPC)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Branches** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branches per Request (Branches / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branch Mispredictions** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branch Misprediction Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **L1 Data Cache Miss Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Last Level Cache Miss Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **IPv4 LPM Trie / Radix Tree Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **IPv6 128-Bit LPM Trie Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Radix Tree / Bitwise Trie Memory Footprint (Kilobytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Client SocketAddr Zero-Allocation Parse Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Immediate 403 Rejection Short-Circuit Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **CIDR Rule In-Memory Traversal Depth (levels)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cacheline Efficiency During LPM Trie Branching (% L1 Miss)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 03: Peak Saturation Under Mixed Traffic (100,000+ req/s, 50% Allowed, 50% Denied)
- **Objective**: Verify that unauthorized requests are rejected instantly without impacting allowed request serving throughput.
- **Execution Command**: `oha -z 15s -c 256 http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total IP Addresses Evaluated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Blocked Requests Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Rejection Rate (rejections/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Mean Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Min Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Median Latency P50 (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P75 Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P90 Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P99 Tail Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P99.9 Tail Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Max Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Latency Standard Deviation (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Connect Time Average (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **HTTP Status Code Distribution (2xx / 4xx / 5xx)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Success Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Utilization (% CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **User CPU (% User CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **System / Kernel CPU (% System / Kernel CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **SoftIRQ CPU (% SoftIRQ CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Frontline Worker 0 CPU (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Frontline Worker 1 CPU (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Shadow Logistics Worker CPU (%)** | *[TBD]* | N/A | N/A | N/A |
| **Peak Resident Set Size (Megabytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **RAM Usage (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Heap Allocations per Request (Bytes / Count)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Memory Leak Rate (Megabytes / minute)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **System Calls per Request (System Calls / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Context Switches per Request (Context Switches / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total System Calls** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Context Switches** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Kernel Time (seconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Futex Calls** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Futex Percentage of Total System Calls (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Instructions** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions per Request (Instructions / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Cycles** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cycles per Request (Cycles / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions Per Cycle (IPC)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Branches** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branches per Request (Branches / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branch Mispredictions** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branch Misprediction Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **L1 Data Cache Miss Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Last Level Cache Miss Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **IPv4 LPM Trie / Radix Tree Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **IPv6 128-Bit LPM Trie Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Radix Tree / Bitwise Trie Memory Footprint (Kilobytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Client SocketAddr Zero-Allocation Parse Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Immediate 403 Rejection Short-Circuit Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **CIDR Rule In-Memory Traversal Depth (levels)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cacheline Efficiency During LPM Trie Branching (% L1 Miss)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 04: Deep IPv6 LPM Trie Traversal (Complex /64 and /48 Subnets)
- **Objective**: Measure memory footprint and CPU overhead when evaluating deep IPv6 CIDR prefix trees.
- **Execution Command**: `oha -z 15s -q 50000 -c 128 http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total IP Addresses Evaluated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Blocked Requests Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Rejection Rate (rejections/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Mean Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Min Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Median Latency P50 (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P75 Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P90 Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P99 Tail Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P99.9 Tail Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Max Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Latency Standard Deviation (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Connect Time Average (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **HTTP Status Code Distribution (2xx / 4xx / 5xx)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Success Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Utilization (% CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **User CPU (% User CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **System / Kernel CPU (% System / Kernel CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **SoftIRQ CPU (% SoftIRQ CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Frontline Worker 0 CPU (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Frontline Worker 1 CPU (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Shadow Logistics Worker CPU (%)** | *[TBD]* | N/A | N/A | N/A |
| **Peak Resident Set Size (Megabytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **RAM Usage (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Heap Allocations per Request (Bytes / Count)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Memory Leak Rate (Megabytes / minute)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **System Calls per Request (System Calls / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Context Switches per Request (Context Switches / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total System Calls** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Context Switches** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Kernel Time (seconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Futex Calls** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Futex Percentage of Total System Calls (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Instructions** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions per Request (Instructions / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Cycles** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cycles per Request (Cycles / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions Per Cycle (IPC)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Branches** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branches per Request (Branches / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branch Mispredictions** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branch Misprediction Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **L1 Data Cache Miss Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Last Level Cache Miss Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **IPv4 LPM Trie / Radix Tree Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **IPv6 128-Bit LPM Trie Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Radix Tree / Bitwise Trie Memory Footprint (Kilobytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Client SocketAddr Zero-Allocation Parse Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Immediate 403 Rejection Short-Circuit Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **CIDR Rule In-Memory Traversal Depth (levels)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cacheline Efficiency During LPM Trie Branching (% L1 Miss)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 05: Massive Malicious IP Flood Rejection (Early 403 Forbidden Drop)
- **Objective**: Confirm that blacklisted traffic is terminated at wire speed with zero upstream contact.
- **Execution Command**: `oha -z 15s -q 80000 -c 128 http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total IP Addresses Evaluated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Blocked Requests Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Rejection Rate (rejections/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Mean Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Min Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Median Latency P50 (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P75 Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P90 Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P99 Tail Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **P99.9 Tail Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Max Latency (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Latency Standard Deviation (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Connect Time Average (milliseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **HTTP Status Code Distribution (2xx / 4xx / 5xx)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Success Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Utilization (% CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **User CPU (% User CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **System / Kernel CPU (% System / Kernel CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **SoftIRQ CPU (% SoftIRQ CPU)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Frontline Worker 0 CPU (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Frontline Worker 1 CPU (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Shadow Logistics Worker CPU (%)** | *[TBD]* | N/A | N/A | N/A |
| **Peak Resident Set Size (Megabytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **RAM Usage (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Heap Allocations per Request (Bytes / Count)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Memory Leak Rate (Megabytes / minute)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **System Calls per Request (System Calls / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Context Switches per Request (Context Switches / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total System Calls** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Context Switches** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Kernel Time (seconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Futex Calls** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Futex Percentage of Total System Calls (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Instructions** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions per Request (Instructions / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CPU Cycles** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cycles per Request (Cycles / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions Per Cycle (IPC)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Branches** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branches per Request (Branches / Request)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branch Mispredictions** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Branch Misprediction Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **L1 Data Cache Miss Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Last Level Cache Miss Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **IPv4 LPM Trie / Radix Tree Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **IPv6 128-Bit LPM Trie Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Radix Tree / Bitwise Trie Memory Footprint (Kilobytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Client SocketAddr Zero-Allocation Parse Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Immediate 403 Rejection Short-Circuit Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **CIDR Rule In-Memory Traversal Depth (levels)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cacheline Efficiency During LPM Trie Branching (% L1 Miss)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

## 4. Comprehensive Architecture Comparison: Velda Edge vs Kong vs APISIX vs Envoy

| Architecture Evaluation Criterion | Velda Edge (Rust Edition 2024) | Kong Gateway 3.9 (LuaJIT) | Apache APISIX 3.10 (LuaJIT) | Envoy Proxy 1.31 (C++) |
| :--- | :--- | :--- | :--- | :--- |
| **Implementation Language** | **Rust (Edition 2024, Zero-Cost Abstractions)** | C (OpenResty) + LuaJIT 2.1 | C (OpenResty) + LuaJIT 2.1 | C++ 20 |
| **Thread Model** | **Thread-Per-Core (Shared-Nothing)** | Multi-Process (Nginx Master-Worker) | Multi-Process (Nginx Master-Worker) | Thread-Per-Core (Worker EventLoop) |
| **Primary Execution Mechanism** | **Radix Tree / Bitwise LPM Trie in Pre-Route Phase** | Kong ip-restriction plugin via lua table lookup | APISIX ip-restriction plugin via lrucache | Envoy rbac / ip_detection filter in filter chain |
| **Heap Allocations** | **Zero allocations (Bitwise integer lookup)** | Lua string parsing of IP address | Lua string parsing of IP address | C++ network address object allocations |
| **Garbage Collection (GC) Impact** | None (Zero GC Pauses by design) | Periodic Lua GC collection cycles | Periodic Lua GC collection cycles | None (TCMalloc heap churn) |
| **Hot-Path Request Serving Impact** | **LPM Trie bitwise walk (< 10 ns)** | Lua hash table / string comparison | Lua table traversal in access phase | C++ CIDR range binary search |
| **Backpressure / Failure Mechanism** | **Immediate 403 local reply** | Lua ngx.exit(403) | Lua ngx.exit(403) | Envoy local reply 403 |
| **Worker Execution Allocation** | **Frontline Worker 0/1 (`velda-worker-{id}`)** | Main Nginx worker thread | Main Nginx worker thread | Main Envoy worker threads |
| **HTTP/3 Support (QUIC Ingress)** | **Native RFC 9114 / RFC 9000 (H3 Production)** | Not Supported | Experimental | Native HTTP/3 Production |
| **Resident Memory Footprint (Peak RSS)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
