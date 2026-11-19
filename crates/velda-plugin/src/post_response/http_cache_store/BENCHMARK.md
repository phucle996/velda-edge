# Performance Benchmark Report: HTTP In-Memory Cache Store Plugin (`http_cache_store`)

Performance benchmark specification and evaluation framework for the `http_cache_store` plugin across **Velda Edge**, **Kong Gateway 3.9**, **Apache APISIX 3.10**, and **Envoy Proxy 1.31**.

---

## 1. Test Environment & Measurement Methodology

### 1.1 Production-Grade Hardware & Network Testbed

*[TBD]*

---

### 1.2 Standardized 5-Tier Metric Taxonomy

All benchmark scenarios are evaluated across five standardized, non-abbreviated metric tiers:

1. **Tier 1: Macro Network, L7 & HTTP Caching Delivery Metrics**:
   - **Duration (seconds)**: Continuous execution duration of the benchmark session (standard 15.00 seconds).
   - **Total Requests**: Total downstream requests successfully served.
   - **Throughput (requests/second)**: Processing throughput rate.
   - **Wire Bandwidth (Megabytes/second)**: Aggregate data transfer rate across the network interface.
   - **Total Requests Evaluated**: Total downstream requests checked against HTTP cache store.
   - **Cache Hits Count**: Total requests served directly from in-memory cache without upstream contact.
   - **Cache Hit Ratio (%)**: Percentage of requests successfully satisfied by cached entries.
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
   - **Cache Key Cryptographic / Hash Calculation Duration (nanoseconds)**: CPU cost to compute 64-bit or 128-bit cache key hash from method, URI, and headers.
   - **In-Memory Lock-Free Cache Store Read Latency (nanoseconds)**: Duration to look up and lease cached response entry from sharded lock-free memory store.
   - **RFC 9111 Cache-Control Directives Evaluation Overhead (nanoseconds)**: Overhead to parse and evaluate `max-age`, `no-cache`, `s-maxage`, and `stale-while-revalidate`.
   - **Conditional ETag / If-None-Match Comparison Latency (nanoseconds)**: CPU cost to validate client conditional headers and synthesize 304 Not Modified.
   - **Zero-Upstream Short-Circuit Response Delivery Efficiency (%)**: Percentage of cache hit requests delivered with zero upstream socket contact.
   - **Cache Entry Memory Slab Slab Recycling Rate (%)**: Percentage of cached payload memory slabs reused across cache store write operations.
   - **Cache Eviction Lock Contention Under Memory Pressure (0 in Sharded Ring)**: Hardware verification of zero lock contention during high-frequency cache evictions.

---

## 2. Focused Benchmark Scenarios

To focus directly on essential operational requirements, the evaluation framework defines five targeted scenarios:
- **Scenario 01: Baseline Cache Hit Forwarding (5,000 req/s, 100% Cache Hit)** — Measure added baseline latency overhead when serving pre-warmed cached responses entirely from memory under light load.
- **Scenario 02: Nominal Mixed Production Traffic (40,000 req/s, 80% Hit / 20% Miss)** — Evaluate sustained serving stability, tail latency, and resource footprint under 80% cache hit / 20% upstream miss distribution.
- **Scenario 03: Peak In-Memory Cache Read Saturation (100,000+ req/s, Zero-Upstream Serving)** — Extreme stress testing to verify lock-free cache store read throughput and confirm non-blocking frontline worker guarantees.
- **Scenario 04: RFC 9111 Conditional Request Validation (304 Not Modified via If-None-Match / ETag)** — Evaluate conditional header evaluation throughput and early 304 Not Modified synthesis.
- **Scenario 05: High-Concurrency Cache Stampede & Lock-Free Storage Invalidation** — Simulate concurrent cache invalidation and re-population to verify single-flight lock-free upstream fetching.

---

## 3. Detailed Benchmark Results

---

### Scenario 01: Baseline Cache Hit Forwarding (5,000 req/s, 100% Cache Hit)
- **Objective**: Determine baseline serving latency and CPU footprint when serving cache hits under low concurrency.
- **Execution Command**: `oha -z 15s -q 5000 -c 50 http://<gateway_host>:<port>/api/v1/static/config`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Requests Evaluated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Hits Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Hit Ratio (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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
| **Cache Key Cryptographic / Hash Calculation Duration (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **In-Memory Lock-Free Cache Store Read Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **RFC 9111 Cache-Control Directives Evaluation Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Conditional ETag / If-None-Match Comparison Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Zero-Upstream Short-Circuit Response Delivery Efficiency (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Entry Memory Slab Slab Recycling Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Eviction Lock Contention Under Memory Pressure (0 in Sharded Ring)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 02: Nominal Mixed Production Traffic (40,000 req/s, 80% Hit / 20% Miss)
- **Objective**: Measure sustained throughput, latency profile, and resource consumption under mixed cache hit/miss workloads.
- **Execution Command**: `oha -z 15s -q 40000 -c 100 http://<gateway_host>:<port>/api/v1/content`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Requests Evaluated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Hits Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Hit Ratio (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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
| **Cache Key Cryptographic / Hash Calculation Duration (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **In-Memory Lock-Free Cache Store Read Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **RFC 9111 Cache-Control Directives Evaluation Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Conditional ETag / If-None-Match Comparison Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Zero-Upstream Short-Circuit Response Delivery Efficiency (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Entry Memory Slab Slab Recycling Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Eviction Lock Contention Under Memory Pressure (0 in Sharded Ring)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 03: Peak In-Memory Cache Read Saturation (100,000+ req/s, Zero-Upstream Serving)
- **Objective**: Confirm maximum possible throughput when frontline workers serve cached content directly from RAM.
- **Execution Command**: `oha -z 15s -c 256 http://<gateway_host>:<port>/api/v1/static/config`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Requests Evaluated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Hits Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Hit Ratio (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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
| **Cache Key Cryptographic / Hash Calculation Duration (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **In-Memory Lock-Free Cache Store Read Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **RFC 9111 Cache-Control Directives Evaluation Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Conditional ETag / If-None-Match Comparison Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Zero-Upstream Short-Circuit Response Delivery Efficiency (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Entry Memory Slab Slab Recycling Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Eviction Lock Contention Under Memory Pressure (0 in Sharded Ring)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 04: RFC 9111 Conditional Request Validation (304 Not Modified via If-None-Match / ETag)
- **Objective**: Measure 304 header synthesis speed and bandwidth savings when validating client ETags.
- **Execution Command**: `oha -z 15s -q 50000 -c 128 -H "If-None-Match: \"etag_hash_12345\"" http://<gateway_host>:<port>/api/v1/static/config`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Requests Evaluated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Hits Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Hit Ratio (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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
| **Cache Key Cryptographic / Hash Calculation Duration (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **In-Memory Lock-Free Cache Store Read Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **RFC 9111 Cache-Control Directives Evaluation Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Conditional ETag / If-None-Match Comparison Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Zero-Upstream Short-Circuit Response Delivery Efficiency (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Entry Memory Slab Slab Recycling Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Eviction Lock Contention Under Memory Pressure (0 in Sharded Ring)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

### Scenario 05: High-Concurrency Cache Stampede & Lock-Free Storage Invalidation
- **Objective**: Confirm that cache stampedes do not degrade gateway latency or exhaust upstream connection pools.
- **Execution Command**: `oha -z 15s -q 20000 -c 64 http://<gateway_host>:<port>/api/v1/dynamic/data`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Requests Evaluated** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Hits Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Hit Ratio (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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
| **Cache Key Cryptographic / Hash Calculation Duration (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **In-Memory Lock-Free Cache Store Read Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **RFC 9111 Cache-Control Directives Evaluation Overhead (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Conditional ETag / If-None-Match Comparison Latency (nanoseconds)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Zero-Upstream Short-Circuit Response Delivery Efficiency (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Entry Memory Slab Slab Recycling Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Cache Eviction Lock Contention Under Memory Pressure (0 in Sharded Ring)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

## 4. Comprehensive Architecture Comparison: Velda Edge vs Kong vs APISIX vs Envoy

| Architecture Evaluation Criterion | Velda Edge (Rust Edition 2024) | Kong Gateway 3.9 (LuaJIT) | Apache APISIX 3.10 (LuaJIT) | Envoy Proxy 1.31 (C++) |
| :--- | :--- | :--- | :--- | :--- |
| **Implementation Language** | **Rust (Edition 2024, Zero-Cost Abstractions)** | C (OpenResty) + LuaJIT 2.1 | C (OpenResty) + LuaJIT 2.1 | C++ 20 |
| **Thread Model** | **Thread-Per-Core (Shared-Nothing)** | Multi-Process (Nginx Master-Worker) | Multi-Process (Nginx Master-Worker) | Thread-Per-Core (Worker EventLoop) |
| **Primary Execution Mechanism** | **Sharded Lock-Free Memory Arena + Zero-Copy Byte Lease** | Kong proxy-cache plugin via shared memory | APISIX proxy-cache plugin via shared dict / disk | Envoy http_cache filter in HTTP filter chain |
| **Heap Allocations** | **Zero allocations on cache hit (Direct byte slice)** | Lua shared memory table & string allocations | Lua shared memory table & string allocations | C++ cache buffer heap allocations |
| **Garbage Collection (GC) Impact** | None (Zero GC Pauses by design) | Periodic Lua GC collection cycles | Periodic Lua GC collection cycles | None (TCMalloc heap churn) |
| **Hot-Path Request Serving Impact** | **Lock-free hash lookup & slice lease (< 12 ns)** | Lua shared dict lock & string retrieval | Lua shared dict lock & string retrieval | C++ cache manager lookup & stream copy |
| **Backpressure / Failure Mechanism** | **Instant local reply, zero upstream queuing** | Worker execution overhead in Lua | Worker execution overhead in Lua | Worker execution overhead in C++ |
| **Worker Execution Allocation** | **Frontline Worker 0/1 (`velda-worker-{id}`)** | Main Nginx worker thread | Main Nginx worker thread | Main Envoy worker threads |
| **HTTP/3 Support (QUIC Ingress)** | **Native RFC 9114 / RFC 9000 (H3 Production)** | Not Supported | Experimental | Native HTTP/3 Production |
| **Resident Memory Footprint (Peak RSS)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
