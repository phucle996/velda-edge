# Performance Benchmark Report: Cross-Origin Resource Sharing Plugin (`cors`) (`cors`)

Performance benchmark specification and evaluation framework for the `cors` plugin across **Velda Edge**, **Kong Gateway 3.9**, **Apache APISIX 3.10**, and **Envoy Proxy 1.31**.

---

## 1. Test Environment & Measurement Methodology

### 1.1 Production-Grade Hardware & Network Testbed

*[TBD]*

---

### 1.2 Standardized 5-Tier Metric Taxonomy

All benchmark scenarios are evaluated across five standardized, non-abbreviated metric tiers:

1. **Tier 1: Macro Network, L7 & CORS Response Delivery Metrics**:
   - **Duration (seconds)**: Continuous execution duration of the benchmark session (standard 15.00 seconds).
   - **Total Requests**: Total downstream requests successfully served.
   - **Throughput (requests/second)**: Processing throughput rate.
   - **Wire Bandwidth (Megabytes/second)**: Aggregate data transfer rate across the network interface.
   - **Total CORS Responses Decorated**: Total downstream responses decorated with CORS headers.
   - **Headers Injected Count**: Total `Access-Control-*` and `Vary` headers injected into response streams.
   - **Header Injection Rate (headers/second)**: Rate of CORS header synthesis and injection.
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
   - **Origin Request Header Fast-Path Lookup Latency (nanoseconds)**: CPU cost to extract and evaluate the `Origin` header from downstream request.
   - **Origin Allowlist Matching Evaluation Duration (nanoseconds)**: Duration to match the requested origin against configured allowlist (exact/wildcard).
   - **Access-Control-Allow-Origin Response Header Insertion Latency (nanoseconds)**: Time required to format and append `Access-Control-Allow-Origin` to response headers.
   - **Access-Control-Expose-Headers Multi-Header Injection Cost (nanoseconds)**: CPU cost to format exposed header lists into response header maps.
   - **Vary: Origin Header Management Overhead (nanoseconds)**: Overhead to append or merge `Vary: Origin` into existing response headers.
   - **Pre-Allocated Static Header Slab Reuse Rate (%)**: Percentage of static CORS header buffers reused from pre-allocated memory slabs.
   - **Non-CORS Request Bypass Overhead (nanoseconds)**: Latency overhead observed on benign non-CORS requests (zero header mutation path).

---

## 2. Focused Benchmark Scenarios

To focus directly on essential operational requirements, the evaluation framework defines five targeted scenarios:
- **Scenario 01: Low-Rate Baseline CORS Response Decoration (5,000 req/s)** — Measure added baseline latency overhead when evaluating Origin headers and appending CORS response headers under light load.
- **Scenario 02: Nominal Production CORS Traffic (40,000 req/s, Dynamic Origin Whitelist)** — Evaluate sustained serving stability, tail latency, and resource footprint when matching origins against a multi-domain whitelist.
- **Scenario 03: Peak CORS Header Injection Saturation (100,000+ req/s)** — Extreme stress testing to verify response header injection throughput and confirm non-blocking frontline worker guarantees.
- **Scenario 04: Multi-Origin Regex / Wildcard Match & Header Reflection** — Evaluate regex pattern matching throughput and header echo efficiency when handling dynamic subdomain origins.
- **Scenario 05: Non-CORS Request Fast-Path Bypass (Zero Mutation Overhead)** — Verify zero-overhead fast-path bypass when requests do not contain an `Origin` header, ensuring zero degradation for standard traffic.

---

## 3. Detailed Benchmark Results

---

### Scenario 01: Low-Rate Baseline CORS Response Decoration (5,000 req/s)
- **Objective**: Determine baseline latency addition and CPU footprint when decorating responses under low concurrency.
- **Execution Command**: `oha -z 15s -q 5000 -c 50 -H "Origin: https://app.example.com" http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CORS Responses Decorated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Headers Injected Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Header Injection Rate (headers/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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
| **Origin Request Header Fast-Path Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Origin Allowlist Matching Evaluation Duration (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Access-Control-Allow-Origin Response Header Insertion Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Access-Control-Expose-Headers Multi-Header Injection Cost (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Vary: Origin Header Management Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Pre-Allocated Static Header Slab Reuse Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Non-CORS Request Bypass Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 02: Nominal Production CORS Traffic (40,000 req/s, Dynamic Origin Whitelist)
- **Objective**: Measure sustained throughput, latency profile, and resource consumption under nominal production CORS traffic.
- **Execution Command**: `oha -z 15s -q 40000 -c 100 -H "Origin: https://app.example.com" http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CORS Responses Decorated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Headers Injected Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Header Injection Rate (headers/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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
| **Origin Request Header Fast-Path Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Origin Allowlist Matching Evaluation Duration (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Access-Control-Allow-Origin Response Header Insertion Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Access-Control-Expose-Headers Multi-Header Injection Cost (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Vary: Origin Header Management Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Pre-Allocated Static Header Slab Reuse Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Non-CORS Request Bypass Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 03: Peak CORS Header Injection Saturation (100,000+ req/s)
- **Objective**: Confirm maximum possible throughput when decorating responses at wire speed across heavy multi-core concurrency.
- **Execution Command**: `oha -z 15s -c 256 -H "Origin: https://app.example.com" http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CORS Responses Decorated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Headers Injected Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Header Injection Rate (headers/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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
| **Origin Request Header Fast-Path Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Origin Allowlist Matching Evaluation Duration (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Access-Control-Allow-Origin Response Header Insertion Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Access-Control-Expose-Headers Multi-Header Injection Cost (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Vary: Origin Header Management Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Pre-Allocated Static Header Slab Reuse Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Non-CORS Request Bypass Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 04: Multi-Origin Regex / Wildcard Match & Header Reflection
- **Objective**: Measure origin verification latency and regex cacheline efficiency under complex multi-tenant origins.
- **Execution Command**: `oha -z 15s -q 50000 -c 128 -H "Origin: https://subdomain42.example.com" http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CORS Responses Decorated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Headers Injected Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Header Injection Rate (headers/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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
| **Origin Request Header Fast-Path Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Origin Allowlist Matching Evaluation Duration (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Access-Control-Allow-Origin Response Header Insertion Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Access-Control-Expose-Headers Multi-Header Injection Cost (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Vary: Origin Header Management Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Pre-Allocated Static Header Slab Reuse Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Non-CORS Request Bypass Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 05: Non-CORS Request Fast-Path Bypass (Zero Mutation Overhead)
- **Objective**: Confirm that non-CORS requests pass through the post-response phase with near-zero latency penalty.
- **Execution Command**: `oha -z 15s -q 50000 -c 128 http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total CORS Responses Decorated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Headers Injected Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Header Injection Rate (headers/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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
| **Origin Request Header Fast-Path Lookup Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Origin Allowlist Matching Evaluation Duration (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Access-Control-Allow-Origin Response Header Insertion Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Access-Control-Expose-Headers Multi-Header Injection Cost (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Vary: Origin Header Management Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Pre-Allocated Static Header Slab Reuse Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Non-CORS Request Bypass Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

## 4. Comprehensive Architecture Comparison: Velda Edge vs Kong vs APISIX vs Envoy

| Architecture Evaluation Criterion | Velda Edge (Rust Edition 2024) | Kong Gateway 3.9 (LuaJIT) | Apache APISIX 3.10 (LuaJIT) | Envoy Proxy 1.31 (C++) |
| :--- | :--- | :--- | :--- | :--- |
| **Implementation Language** | **Rust (Edition 2024, Zero-Cost Abstractions)** | C (OpenResty) + LuaJIT 2.1 | C (OpenResty) + LuaJIT 2.1 | C++ 20 |
| **Thread Model** | **Thread-Per-Core (Shared-Nothing)** | Multi-Process (Nginx Master-Worker) | Multi-Process (Nginx Master-Worker) | Thread-Per-Core (Worker EventLoop) |
| **Primary Execution Mechanism** | **Post-Response In-Place Header Decoration + Pre-Baked Slabs** | Kong cors plugin in header_filter phase | APISIX cors plugin in header_filter phase | Envoy cors filter in HTTP filter chain |
| **Heap Allocations** | **Zero allocations (Static pre-baked header array)** | Lua table creation in header_filter | Lua table creation in header_filter | C++ HeaderMap string allocations |
| **Garbage Collection (GC) Impact** | None (Zero GC Pauses by design) | Periodic Lua GC collection cycles | Periodic Lua GC collection cycles | None (TCMalloc heap churn) |
| **Hot-Path Request Serving Impact** | **HeaderMap append in-place (< 8 ns)** | Lua ngx.header assignment in filter | Lua ngx.header assignment in filter | C++ HeaderMap mutation in filter chain |
| **Backpressure / Failure Mechanism** | **Transparent downstream delivery** | Worker execution overhead in Lua | Worker execution overhead in Lua | Worker execution overhead in C++ |
| **Worker Execution Allocation** | **Frontline Worker 0/1 (`velda-worker-{id}`)** | Main Nginx worker thread | Main Nginx worker thread | Main Envoy worker threads |
| **HTTP/3 Support (QUIC Ingress)** | **Native RFC 9114 / RFC 9000 (H3 Production)** | Not Supported | Experimental | Native HTTP/3 Production |
| **Resident Memory Footprint (Peak RSS)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
