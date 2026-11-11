# Performance Benchmark Report: Prometheus Telemetry Exporter Plugin (`prometheus_exporter`)

Performance benchmark specification and evaluation framework for the `prometheus_exporter` plugin across **Velda Edge**, **Kong Gateway 3.9**, **Apache APISIX 3.10**, and **Envoy Proxy 1.31**.

---

## 1. Test Environment & Measurement Methodology

### 1.1 Production-Grade Hardware & Network Testbed

*[TBD]*

---

### 1.2 Standardized 4-Tier Metric Taxonomy

All benchmark scenarios are evaluated across four standardized, non-abbreviated metric tiers:

1. **Tier 1: Macro Network, L7 & Prometheus Exposition Delivery Metrics**:
   - **Duration (seconds)**: Continuous execution duration of the benchmark session (standard 15.00 seconds).
   - **Total Requests**: Total scrape requests successfully served by the `/metrics` endpoint.
   - **Throughput (requests/second)**: Scrape processing throughput rate.
   - **Wire Bandwidth (Megabytes/second)**: Aggregate data transfer rate across the network interface.
   - **Total Metric Samples Exported**: Total individual gauge, counter, and histogram samples formatted.
   - **Metric Samples Throughput (samples/second)**: Formatting and delivery rate of Prometheus metric samples.
   - **Active Metric Series Count**: Number of distinct metric series formatted in the output exposition text.
   - **Mean Latency (milliseconds)**: Average time from scrape request receipt to response completion.
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
   - **Heap Allocations per Request (Bytes / Count)**: Dynamic memory allocation rate during scrape exposition.
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

---

## 2. Focused Benchmark Scenarios

To focus directly on essential operational requirements, the evaluation framework defines five targeted scenarios:
- **Scenario 01: Low-Latency Floor Scrape (Concurrency = 1, Keep-Alive)** — Measure floor serialization latency, TTFB, and CPU cycles per scrape request under sequential scraping conditions without concurrent load.
- **Scenario 02: Multi-Scraper Concurrent Burst (Concurrency = 16)** — Evaluate scraping throughput, latency distribution, and CPU scaling when multiple Prometheus instances scrape the edge gateway concurrently.
- **Scenario 03: Scraper Saturation & Semaphore Bounds (Concurrency = 64, max_concurrency = 16)** — Evaluate gateway stability, 503 Service Unavailable enforcement, and semaphore queue behavior when scrape request concurrency exceeds configured limits.
- **Scenario 04: Cryptographic TLS & mTLS Scrape Overhead (HTTPS /metrics)** — Evaluate cryptographic overhead, TLS handshake latency, and session resumption impact on Prometheus scrape serving.
- **Scenario 05: Scraper Backpressure & Slowloris Resistance (200ms Read Stalls & Socket Backpressure)** — Verify that slow Prometheus clients (simulated 200ms socket read stalls) do not cause socket leaks, buffer inflation, or front-line traffic degradation.

---

## 3. Detailed Benchmark Results

---

### Scenario 01: Low-Latency Floor Scrape (Concurrency = 1, Keep-Alive) — Floor Latency Evaluation
- **Objective**: Measure floor serialization latency, TTFB, and CPU cycles per scrape request under sequential scraping conditions without concurrent load.
- **Execution Command**: `wrk -t1 -c1 -d15s --latency http://<gateway_host>:9090/metrics`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Metric Samples Exported** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Metric Samples Throughput (samples/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Active Metric Series Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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

---

### Scenario 02: Multi-Scraper Concurrent Burst (Concurrency = 16) — Multi-Node Scrape Scalability
- **Objective**: Evaluate scraping throughput, latency distribution, and CPU scaling when multiple Prometheus instances scrape the edge gateway concurrently.
- **Execution Command**: `wrk -t2 -c16 -d15s --latency http://<gateway_host>:9090/metrics`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Metric Samples Exported** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Metric Samples Throughput (samples/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Active Metric Series Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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

---

### Scenario 03: Scraper Saturation & Semaphore Bounds (Concurrency = 64, max_concurrency = 16) — Semaphore Resilience
- **Objective**: Evaluate gateway stability, 503 Service Unavailable enforcement, and semaphore queue behavior when scrape request concurrency exceeds configured limits.
- **Execution Command**: `wrk -t4 -c64 -d15s --latency http://<gateway_host>:9090/metrics`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Metric Samples Exported** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Metric Samples Throughput (samples/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Active Metric Series Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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

---

### Scenario 04: Cryptographic TLS & mTLS Scrape Overhead (HTTPS /metrics) — Cryptographic Impact
- **Objective**: Evaluate cryptographic overhead, TLS handshake latency, and session resumption impact on Prometheus scrape serving.
- **Execution Command**: `wrk -t2 -c16 -d15s --latency https://<gateway_host>:9090/metrics`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Metric Samples Exported** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Metric Samples Throughput (samples/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Active Metric Series Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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

---

### Scenario 05: Scraper Backpressure & Slowloris Resistance (200ms Read Stalls & Socket Backpressure) — Slow-Scraper Resilience
- **Objective**: Verify that slow Prometheus clients (simulated 200ms socket read stalls) do not cause socket leaks, buffer inflation, or front-line traffic degradation.
- **Execution Command**: `wrk -t4 -c32 -d15s http://<gateway_host>:9090/metrics`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total Metric Samples Exported** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Metric Samples Throughput (samples/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Active Metric Series Count** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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

---

## 4. Comprehensive Architecture Comparison: Velda Edge vs Kong vs APISIX vs Envoy

| Architecture Evaluation Criterion | Velda Edge (Rust Edition 2024) | Kong Gateway 3.9 (LuaJIT) | Apache APISIX 3.10 (LuaJIT) | Envoy Proxy 1.31 (C++) |
| :--- | :--- | :--- | :--- | :--- |
| **Implementation Language** | **Rust (Edition 2024, Zero-Cost Abstractions)** | C (OpenResty) + LuaJIT 2.1 | C (OpenResty) + LuaJIT 2.1 | C++ 20 |
| **Thread Model** | **Thread-Per-Core (Shared-Nothing)** | Multi-Process (Nginx Master-Worker) | Multi-Process (Nginx Master-Worker) | Thread-Per-Core (Worker EventLoop) |
| **Formatting Engine** | **200B `DIGIT_PAIRS LUT` + Reciprocal Division-Free Formatting** | Lua table building + string concat | Lua table building + string concat | Dynamic Prometheus text generator (C++) |
| **Concurrency Limiting** | **Hard Atomic Semaphore (`max_concurrency`)** | Unbounded (Nginx connection limit) | Unbounded (Nginx connection limit) | Configurable connection limit |
| **Exposition Serving Thread** | **Dedicated Shadow Logistics Worker (`SCHED_IDLE`)** | Nginx worker thread handling `/metrics` | Nginx worker thread handling `/metrics` | Worker thread / admin interface |
| **Heap Allocations** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Garbage Collection (GC) Impact** | None (Zero GC Pauses by design) | Heavy Lua GC churn during formatting | Heavy Lua GC churn during formatting | None (TCMalloc heap churn) |
| **Hot-Path Request Serving Impact** | Non-blocking (Atomic snapshots over cacheline-isolated slots) | Spinlock contention on shared dict | Spinlock contention on shared dict | Lock-free atomic counter reading |
| **Cryptographic TLS & mTLS Handling** | **Dedicated rustls / kTLS stack** | OpenSSL via OpenResty | OpenSSL via OpenResty | BoringSSL |
| **HTTP/3 Support (QUIC Ingress)** | **Native RFC 9114 / RFC 9000 (H3 Production)** | Not Supported | Experimental | Native HTTP/3 Production |
| **Resident Memory Footprint (Peak RSS)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
