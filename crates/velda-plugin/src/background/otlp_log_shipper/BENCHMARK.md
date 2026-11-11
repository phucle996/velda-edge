# Performance Benchmark Report: OpenTelemetry Log Shipper Plugin (`otlp_log_shipper`)

Performance benchmark specification and evaluation framework for the `otlp_log_shipper` plugin across **Velda Edge**, **Kong Gateway 3.9**, **Apache APISIX 3.10**, and **Envoy Proxy 1.31**.

---

## 1. Test Environment & Measurement Methodology

### 1.1 Production-Grade Hardware & Network Testbed

*[TBD]*

---

### 1.2 Standardized 4-Tier Metric Taxonomy

All benchmark scenarios are evaluated across four standardized, non-abbreviated metric tiers:

1. **Tier 1: Macro Network, L7 & OpenTelemetry Telemetry Delivery Metrics**:
   - **Duration (seconds)**: Continuous execution duration of the benchmark session (standard 15.00 seconds).
   - **Total Requests**: Total downstream requests successfully served.
   - **Throughput (requests/second)**: Processing throughput rate.
   - **Wire Bandwidth (Megabytes/second)**: Aggregate data transfer rate across the network interface.
   - **Total OTLP LogRecords Emitted**: Total compliant OpenTelemetry log records successfully exported.
   - **OTLP Records Throughput (records/second)**: Formatting and delivery rate of OpenTelemetry records.
   - **Average OTLP Record Size (bytes)**: Serialized payload size per OTLP log record.
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

---

## 2. Focused Benchmark Scenarios

To focus directly on essential operational requirements, the evaluation framework defines five targeted scenarios:
- **Scenario 01: Low-Rate Baseline (5,000 OTLP Records/sec)** — Measure added baseline latency overhead when extracting W3C TraceContext and packing OTLP ResourceLogs under light load.
- **Scenario 02: Nominal Production Load (40,000 OTLP Records/sec)** — Evaluate sustained serving stability, tail latency, and resource footprint under nominal daily traffic with > 50% CPU headroom.
- **Scenario 03: Peak Queue Saturation (100,000+ OTLP Records/sec)** — Extreme stress testing to verify SPSC ring buffer saturation behavior and confirm non-blocking frontline worker guarantees under drop-overflow conditions.
- **Scenario 04: W3C TraceContext & Extended OTLP Payload (~1,200 Bytes)** — Challenge zero-copy parsing of `traceparent` headers via `HEX_LUT` and packing multi-dimensional Resource attributes.
- **Scenario 05: OTLP Collector Backpressure & Failure (~200ms Delay & Downed Collector)** — Verify that collector connection stalls (200ms delay or connection refusal) do not block frontline traffic or cause process instability.

---

## 3. Detailed Benchmark Results

---

### Scenario 01: Low-Rate Baseline (5,000 OTLP Records/sec) — Baseline Overhead Evaluation
- **Objective**: Determine baseline latency addition and CPU footprint when extracting trace headers and queueing OTLP log records under low concurrency.
- **Execution Command**: `oha -z 15s -q 5000 -c 50 -H "traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total OTLP LogRecords Emitted** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **OTLP Records Throughput (records/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Average OTLP Record Size (bytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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

### Scenario 02: Nominal Production Load (40,000 OTLP Records/sec) — Sustained Baseline Performance
- **Objective**: Measure sustained throughput, latency profile, and resource consumption when continuously processing 40,000 OTLP log records/second with > 50% CPU headroom.
- **Execution Command**: `oha -z 15s -q 40000 -c 100 -H "traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total OTLP LogRecords Emitted** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **OTLP Records Throughput (records/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Average OTLP Record Size (bytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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

### Scenario 03: Peak Queue Saturation (100,000+ OTLP Records/sec) — Ring Buffer Saturation & Drop-Overflow
- **Objective**: Overload the gateway with extreme request volume to saturate the SPSC ring buffer and verify that the non-blocking drop-overflow mechanism guarantees zero latency degradation for frontline workers.
- **Execution Command**: `wrk -t4 -c200 -d15s -H "traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" --latency http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total OTLP LogRecords Emitted** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **OTLP Records Throughput (records/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Average OTLP Record Size (bytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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

### Scenario 04: W3C TraceContext & Extended OTLP Payload (~1,200 Bytes)
- **Objective**: Challenge zero-copy parsing of `traceparent` headers via `HEX_LUT` and packing multi-dimensional Resource attributes and error metadata without dynamic heap allocations.
- **Execution Command**: `wrk -t4 -c100 -d15s -H "traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" --latency http://<gateway_host>:<port>/api/v1/heavy-otlp`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total OTLP LogRecords Emitted** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **OTLP Records Throughput (records/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Average OTLP Record Size (bytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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

### Scenario 05: OTLP Collector Backpressure & Failure (~200ms Delay & Downed Collector)
- **Objective**: Verify that collector connection stalls (200ms delay or connection refusal) do not block frontline traffic or cause process instability, validating SPSC ring buffer isolation.
- **Execution Command**: `wrk -t4 -c100 -d15s http://<gateway_host>:<port>/api/v1/ping`

| Detailed Measurement Metric | **Velda Edge** (Rust 2024) | **Kong Gateway 3.9** (LuaJIT) | **Apache APISIX 3.10** (LuaJIT) | **Envoy Proxy 1.31** (C++) |
| :--- | :---: | :---: | :---: | :---: |
| **Duration (seconds)** | **15.00** | **15.00** | **15.00** | **15.00** |
| **Total Requests** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Throughput (requests/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Wire Bandwidth (Megabytes/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Total OTLP LogRecords Emitted** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **OTLP Records Throughput (records/second)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Average OTLP Record Size (bytes)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
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
| **OTLP Formatting Mechanism** | **Static Envelope Frame + 200B LUT + Hex LUT** | JSON Encoder cosocket (`cjson`) | Protobuf / JSON buffer ngx.timer | Protobuf C++ API (`google.protobuf`) |
| **Heap Allocations** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Garbage Collection (GC) Impact** | None (Zero GC Pauses by design) | Periodic Lua GC collection cycles | Periodic Lua GC collection cycles | None (TCMalloc heap churn) |
| **W3C TraceContext Extraction** | Zero-Copy Byte Pointer + Offset Scan via HEX_LUT | Substring slicing `string.sub` | Substring slicing `string.sub` | C++ String Substr & Header Map |
| **Hot-Path Request Serving Impact** | Non-blocking (Atomic SPSC ring buffer enqueue) | Serializing overhead on worker thread | Batch buffer in ngx.timer | Access log filter within request lifecycle |
| **Collector Congestion Mechanism** | Proactive drop-overflow in SPSC ring buffer | Worker blocking or buffer overrun | ngx.timer buffer retention or drop | Ring buffer overflow drop / non-blocking flush |
| **Telemetry Push Worker Thread** | Dedicated Shadow Logistics Worker (`SCHED_IDLE`) | Main Nginx worker thread | Nginx timer callback thread | Dedicated gRPC ALS client thread |
| **HTTP/3 Support (QUIC Ingress)** | **Native RFC 9114 / RFC 9000 (H3 Production)** | Not Supported | Experimental | Native HTTP/3 Production |
| **Resident Memory Footprint (Peak RSS)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
