# Performance Benchmark Specification: OpenTelemetry Trace Exporter (`otlp_trace_exporter`)

This document defines the first-principles performance evaluation and micro-architectural verification specification for the [`otlp_trace_exporter`](crates/velda-plugin/src/background/otlp_trace_exporter/) background plugin in **Velda Edge**, comparing its architecture against OpenTelemetry trace exporters in **Envoy Proxy**, **OpenResty**, and **Kong Gateway**.

> [!NOTE]
> This document is a **Standardized Benchmark Specification Form** authored strictly under [`/velda-benchmark`](../../../../.agents/skills/velda-benchmark/SKILL.md). **No empirical results, throughput guarantees, or latency promises are claimed here.** All measured metric cells remain `*[TBD]*` until executed on an isolated hardware testbed, and all architectural hypotheses must be validated against Linux `perf` and release assembly `.s` output.

---

## 1. First-Principles Reasoning & Component Characterization

### 1.1 Physical Axes of Operation

| Operating Axis | Physical Bottleneck / Mechanism | Relevant Implementation Surface | Verification Focus |
| :--- | :--- | :--- | :--- |
| **Memory & Cache Hierarchy** | L1D cache pollution, Write-Combining Buffer (WCB) pressure, page alignment | `SpanRecord` (`#[repr(C, align(64))]`, 64B); `SpscSpanRing::push` (`_mm256_stream_si256` / `VMOVNTDQ`); `SpscSpanRing::drain_batch` (`_MM_HINT_T1` prefetch + `VMOVNTDQA`); 4KB `RouteCatalog` / `UpstreamCatalog` | Confirm non-temporal stores keep L1D clean on frontline cores; measure L1D/LLC miss rates on `velda-shadow-{i}`. |
| **Synchronization & Threading** | MESI RFO invalidations, atomic load/store ordering, `SCHED_IDLE` preemption | `CachePadded<AtomicUsize>` (`head`, `tail`, `cached_head`, `cached_tail`); `ArcSwapOption` catalog loads; Linux `SCHED_IDLE` shadow thread | Measure `ring.dropped` under 100% frontline CPU saturation; detect RFO spikes during catalog swaps. |
| **Compute & Serialization** | Varint encoding, stack IPv4 formatting, UTF-8 byte splicing, stack hex conversion | `OtlpTraceProtoEnvelope::format_spans_wire` (direct wire tags, zero Prost AST); `OtlpTraceJsonEnvelope::format_spans_json` (`itoa::Buffer`) | Verify 0 heap string allocations in `encode_single_span_wire`; check `spans_wire` reallocation frequency. |
| **Kernel & OS I/O** | HTTP/2 frame multiplexing, TCP socket send buffer (`SO_SNDBUF`), TLS encryption | `OtlpTracesGrpcClient` (`h2` + 5B LPM framing); `OtlpTraceHttpClient` (HTTP/1.1 Keep-Alive TCP/Rustls) | Verify zero socket contention with traffic worker FDs; measure `request_timeout_ms` impact on subscriber queue backpressure. |

### 1.2 Core Engineering Hypotheses (To Be Tested, Not Assumed)
1. **Frontline Zero-Pollution Invariant**: On frontline traffic workers, capturing a span executes strictly on the stack + one 64-byte non-temporal streaming write (`VMOVNTDQ` + `sfence`) into a pre-allocated 256 KiB ring (`4096` slots), incurring **0 bytes** of heap allocation and **0 mutex/futex calls**.
2. **Shadow Serialization Cost Center**: On the background shadow worker (`velda-shadow-{i}`), the dominant CPU cost in `grpc` mode lies in protobuf byte assembly (`encode_single_span_wire`) + `h2` HTTP/2 stream framing + optional TLS encryption, rather than catalog string locks (`ArcSwapOption`) or Prost AST construction.
3. **Physical Scaling Ceiling**: Sustained export throughput (`100K – 1M spans/sec`) is bounded by (a) shadow thread CPU share under `SCHED_IDLE` scheduling, (b) scratch vector growth in `format_spans_wire`, and (c) downstream collector ingestion bandwidth / HTTP/2 flow-control windows.

---

## 2. Predictive Blind-Spot Reasoning

Instead of testing only low-rate happy paths, this specification maps 6 concrete interaction boundaries where silent degradation or failure modes can emerge at scale (100K – 1M spans/s):

1. **Scheduler Starvation Boundary (`SCHED_OTHER` vs `SCHED_IDLE`)**:
   - *Mechanism*: Frontline workers run at normal priority (`SCHED_OTHER`) while `velda-shadow-{i}` runs under `SCHED_IDLE`. Each core's ring holds `4096` spans (`DEFAULT_TRACE_CORE_RING_CAPACITY`).
   - *Predicted Blind Spot*: At `100K spans/sec` per core, a `4096`-slot ring fills in **40.96 ms**. If a core is saturated at 100% CPU and CFS starves `SCHED_IDLE` past `flush_interval_ms`, or if `flush_interval_ms > 40ms` under per-core burst, `SpscSpanRing::push` drops spans (`dropped` counter increments).
2. **Write-Combining Buffer (WCB) & Memory Bus Contention**:
   - *Mechanism*: `SpscSpanRing::push` emits `_mm256_stream_si256` + `_mm_sfence()` on every request completion.
   - *Predicted Blind Spot*: On high-core-count CPUs (e.g., 16–64 cores producing `1M spans/sec` total), frequent `_mm_sfence()` instructions serialize CPU store buffers and may degrade frontline P99 latency even without touching L1D.
3. **Wire Buffer High-Watermark Growth (`spans_wire`)**:
   - *Mechanism*: `OtlpTraceProtoEnvelope` owns persistent `spans_wire` (16 KiB initial capacity) and `single_span` (256B) scratch buffers reused across ticks, alongside `proto_buf` (`32 KiB`).
   - *Predicted Blind Spot*: If a single burst reaches `max_batch_size = 10_000` spans with long routes, upstream names, and IPs (~300B/span $\approx 3$ MB), both `spans_wire` and `proto_buf` grow via `Vec::reserve` to the burst high-watermark and retain that capacity in memory afterwards. *(This benchmark matrix explicitly measures peak RSS drift and buffer growth under 10K-span batches).*
4. **Slow Collector Backpressure & Subscriber Queue Clipping**:
   - *Mechanism*: `plugin.rs` calls `self.trace_store.subscribe(256)`, flushes via `flush_all_lanes(max_batch_size)`, and executes `send_traces(&proto_buf, ...)` synchronously in the single `run()` loop before ticking again.
   - *Predicted Blind Spot*: Because `send_traces` awaits `request_timeout` inline within the flush loop, a degraded collector stalling for `request_timeout_ms` (e.g. 5000 ms) halts `flush_all_lanes` for the entire timeout window, causing frontline SPSC rings to overflow even though `TraceStore` holds 256 batch slots.
5. **Catalog Swap Coherence Window (`ArcSwapOption` Reload)**:
   - *Mechanism*: Hot config reload invokes `set_global_route_catalog` / `set_global_upstream_catalog`.
   - *Predicted Blind Spot*: High-frequency `with_route_bytes` and `with_upstream_bytes` calls (`ArcSwapOption::load`) during export loops experience pointer-swap cache invalidation and atomic refcount contention if the control plane pushes rapid reloads.
6. **gRPC Protobuf vs HTTP JSON Bandwidth Amplification**:
   - *Mechanism*: Protobuf sends raw binary `trace_id` (16B) + `span_id` (8B) and varints; JSON expands IDs to hex strings (32B + 16B) plus full JSON key wrappers per span.
   - *Predicted Blind Spot*: At `100K–1M spans/s`, HTTP JSON expands payload volume substantially over Protobuf, shifting the bottleneck from CPU formatting to kernel `tcp_sendmsg` / collector HTTP parsing.

---

## 3. Contextual Baselines & Isolated Testbed Specification

### 3.1 Contextual Baselines
- **Velda Edge (`otlp_trace_exporter`)**: Rust 2024, 64B `SpanRecord`, AVX2 `VMOVNTDQ` SPSC ring, 4KB page-aligned `RouteCatalog` / `UpstreamCatalog` `&[u8]`, direct Protobuf wire encoder (no Prost AST) over persistent `h2` (`4317`), JSON envelope over HTTP/1.1 Keep-Alive (`4318`).
- **Envoy Proxy**: C++20, `envoy.tracers.opentelemetry` (async gRPC dynamic Protobuf AST allocation per span).
- **OpenResty / Kong Gateway**: Nginx C + LuaJIT 2.1 OpenTelemetry plugin (Lua table allocation per span + GC cycles).

### 3.2 Dedicated Cloud Testbed Specification
*(Pending Setup — Intentionally blank until dedicated isolated hardware is provisioned).*

- **Network Fabric**: *[Pending Setup]*
- **Gateway Server Node (VM2)**: *[Pending Setup — CPU pinning `taskset -c`, SMT disabled, `performance` governor]*
- **OTLP Collector / Sink Node (VM1)**: *[Pending Setup — Mock high-rate gRPC/HTTP receiver measuring ingested bytes & spans/s]*
- **Traffic Generator Node (VM0)**: *[Pending Setup — Separate physical/VM host generating `100K – 1M RPS`]*
- **Kernel & Allocator Profile**: *[Pending Setup]*

---

## 4. Comprehensive 6-Scenario Test Matrix (100K – 1M Spans/s Target Range)

```text
╔═════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║                         OTLP TRACE EXPORTER — 6-SCENARIO BENCHMARK SPECIFICATION MATRIX                         ║
╠══════════════════════════════════╦═══════════════════════════════╦══════════════════════╦═══════════════════════╣
║ Scenario Identifier              ║ Workload & Target Span Rate   ║ Transport / State    ║ Blind Spot Evaluated  ║
╠══════════════════════════════════╬═══════════════════════════════╬══════════════════════╬═══════════════════════╣
║ KB 01: Direct Wire Floor (100K)  ║ 100K spans/s, 1000/batch      ║ OTLP/gRPC (Plain)    ║ Baseline & Alloc Audit║
║ KB 02: High-Rate Scale (500K-1M) ║ 500K–1M spans/s, 10K/batch    ║ OTLP/gRPC (Plain)    ║ WCB, sfence & realloc ║
║ KB 03: Protocol Shift (PB vs JS) ║ 100K–250K spans/s             ║ gRPC PB vs HTTP JSON ║ Wire Size & Kernel IO ║
║ KB 04: Frontline Core Saturation ║ 100% Frontline CPU + Tracing  ║ SCHED_IDLE Shadow    ║ Starvation & Ring Drop║
║ KB 05: Collector Stall Backpress ║ 100K spans/s + 5s Stall       ║ request_timeout_ms   ║ Inline Flush Blockage ║
║ KB 06: Catalog Reload Churn      ║ 200K spans/s + Reload/1s      ║ ArcSwapOption Swap   ║ RFO & Memory Drift    ║
╚══════════════════════════════════╩═══════════════════════════════╩══════════════════════╩═══════════════════════╝
```

---

## 5. Detailed 5-Tier Diagnostic Tables (Pending Empirical Execution)

### KB 01: Baseline Wire Serialization & 100K Spans/s Floor (`protocol: "grpc"`)
- **Objective**: Measure baseline serialization latency, allocations per span (`spans_wire` vs `out_buf`), IPC, and frontline impact at 100,000 spans/s (`flush_interval_ms: 10`, `max_batch_size: 1000`).

| Detailed Metric (5 Tiers) | **Velda Edge** (`otlp_trace_exporter`) | **Envoy** (C++) | **OpenResty / Kong** (LuaJIT) |
| :--- | :---: | :---: | :---: |
| **--- 1. Macro Tier: Span Export Throughput & Latency ---** | | | |
| Measurement Duration | *[TBD]* | *[TBD]* | *[TBD]* |
| Target Offered Load | 100,000 spans/s | 100,000 spans/s | 100,000 spans/s |
| Sustained Exported Spans/s | *[TBD]* | *[TBD]* | *[TBD]* |
| Serialization Duration / 1000-Span Batch | *[TBD]* | *[TBD]* | *[TBD]* |
| Flush Loop Latency (P50 / P90 / P99 / P99.9) | *[TBD]* | *[TBD]* | *[TBD]* |
| Client Serving Delta vs Tracing-Off (%) | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 2. Domain Dynamics & SLA Diagnostics ---** | | | |
| `SpscSpanRing` Drop Count (`dropped`) | *[TBD]* | N/A | N/A |
| Average Encoded Wire Bytes / Span | *[TBD]* | *[TBD]* | *[TBD]* |
| Direct Wire AST Allocations | **0** *(Zero Prost AST)* | *[TBD]* | *[TBD]* |
| `spans_wire` / `single_span` Reallocs / Flush | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 3. System Resource & Core Isolation Tier ---** | | | |
| Frontline Worker CPU % (Tracing On vs Off Delta) | *[TBD]* | *[TBD]* | *[TBD]* |
| Shadow Worker CPU % (`velda-shadow-{i}`) | *[TBD]* | N/A | N/A |
| Peak RSS Memory & Drift (MB / min) | *[TBD]* | *[TBD]* | *[TBD]* |
| **--- 4. Micro Tier: Kernel & OS ---** | | | |
| Syscalls / Export Batch | *[TBD]* | *[TBD]* | *[TBD]* |
| Context Switches / Export Batch | *[TBD]* | *[TBD]* | *[TBD]* |
| TCP Sockets Held Open | **1 (Persistent `h2`)** | *[TBD]* | *[TBD]* |
| **--- 5. Micro Tier: HPC & CPU PMU Counters ---** | | | |
| Instructions / Span (`insn / span`) | *[TBD]* | *[TBD]* | *[TBD]* |
| Cycles / Span (`cycles / span`) | *[TBD]* | *[TBD]* | *[TBD]* |
| IPC (`insn / cycle`) | *[TBD]* | *[TBD]* | *[TBD]* |
| Non-Temporal Store Stalls / `_mm_sfence` Cost | *[TBD]* | N/A | N/A |
| `L1-dcache-miss %` & `LLC-miss %` | *[TBD]* | *[TBD]* | *[TBD]* |

---

### KB 02: High-Rate Saturation (500K – 1M Spans/s, `max_batch_size: 10000`)
- **Objective**: Stress `SpscSpanRing` (`4096` slots/core), Write-Combining Buffers (`VMOVNTDQ`), and `spans_wire` capacity scaling with long routes + `upstream_id` + `client_ip`.

| Detailed Metric | **Velda Edge** (`otlp_trace_exporter`) | **Envoy** (C++) | **OpenResty / Kong** (LuaJIT) |
| :--- | :---: | :---: | :---: |
| Sustained Exported Spans/s (at 500K / 1M target) | *[TBD]* | *[TBD]* | *[TBD]* |
| Ring Drop Ratio (`dropped / emitted %`) | *[TBD]* | *[TBD]* | *[TBD]* |
| Frontline P99 / P99.9 Latency Impact vs Baseline | *[TBD]* | *[TBD]* | *[TBD]* |
| `spans_wire` Peak Size & Realloc Count | *[TBD]* | *[TBD]* | *[TBD]* |
| Shadow Worker CPU % / Core | *[TBD]* | N/A | N/A |
| WCB / Store-Buffer Stall Cycles (`RESOURCE_STALLS`) | *[TBD]* | N/A | N/A |

---

### KB 03: Protocol Shift (`protocol: "grpc"` Protobuf vs `protocol: "http"` JSON)
- **Objective**: Quantify CPU serialization delta, payload byte amplification, and socket write syscalls between binary Protobuf wire encoding and JSON text encoding at equal span rates.

| Detailed Metric | **Velda Edge (`grpc` Protobuf)** | **Velda Edge (`http` JSON)** | Delta (%) |
| :--- | :---: | :---: | :---: |
| Encoded Payload Size (Bytes / 1000 Spans) | *[TBD]* | *[TBD]* | *[TBD]* |
| Serialization Time / 1000 Spans | *[TBD]* | *[TBD]* | *[TBD]* |
| Shadow CPU Utilization (%) | *[TBD]* | *[TBD]* | *[TBD]* |
| Syscalls / Flush (`write` / `sendmsg`) | *[TBD]* | *[TBD]* | *[TBD]* |
| `insn / span` & `L1-dcache-miss %` | *[TBD]* | *[TBD]* | *[TBD]* |

---

### KB 04: Frontline Saturation & `SCHED_IDLE` Starvation Test
- **Objective**: Saturate frontline cores to 100% CPU with L7 proxy traffic while observing `velda-shadow-{i}` wakeup slip, ring fill level, and dropped span counts.

| Detailed Metric | **Velda Edge** | Observation Notes |
| :--- | :---: | :--- |
| Frontline Core CPU Utilization (%) | *[TBD]* | Saturated workload |
| Frontline P99 Request Latency Impact | *[TBD]* | Must isolate client serving from export activity |
| Shadow Thread Timer Slip (`actual_tick - flush_interval_ms`) | *[TBD]* | Measures `SCHED_IDLE` preemption delay |
| `SpscSpanRing::dropped` Rate (spans/s) | *[TBD]* | Triggers when starved > `4096 / per_core_rps` |

---

### KB 05: Collector Timeout & Fail-Open Backpressure Isolation
- **Objective**: Inject artificial collector hangs matching `request_timeout_ms` (1000 ms – 5000 ms) while serving live client traffic to confirm zero client traffic degradation and quantify `failed_spans` / ring loss behavior.

| Detailed Metric | **Velda Edge** | Observation Notes |
| :--- | :---: | :--- |
| Frontline Client Success Rate (%) during Collector Hang | *[TBD]* | Verify 100% isolation (Fail-Open) |
| Frontline P99.9 Latency Delta during Hang | *[TBD]* | Verify zero blocking on frontline cores |
| `failed_spans` Counter Increment Rate | *[TBD]* | Matches dropped/timed-out batches |
| Recovery Time After Collector Returns | *[TBD]* | H2 / TCP reconnect latency on next tick |

---

### KB 06: Lock-Free Catalog Reload Churn (`RouteCatalog` / `UpstreamCatalog` Swaps)
- **Objective**: Execute `set_global_route_catalog` every 1s under continuous `200K spans/s` load to measure `ArcSwapOption` RFO impact and verify RSS stability (no leaked catalog buffers).

| Detailed Metric | **Velda Edge** | Observation Notes |
| :--- | :---: | :--- |
| Export Flush P99.9 Latency Spike During Swap | *[TBD]* | Captures `ArcSwapOption` RFO / atomic contention |
| RSS Memory Drift Over 300s (MB) | *[TBD]* | Confirms prompt drop of superseded `PageAlignedBuffer` |
| `L1-dcache-miss %` Delta During Swap Window | *[TBD]* | — |

---

## 6. ASM & Symbol Verification Protocol (Unbiased Audit)

Before publishing any empirical benchmark run, run Linux `perf` and assembly audits using the [`/velda-benchmark`](../../../../.agents/skills/velda-benchmark/SKILL.md) toolchain to verify zero hidden allocations or compiler traps:

### 6.1 CPU Symbol Distribution (`perf report`)
```bash
perf record -F 997 -p $(pidof velda-edge) -g -- sleep 30
perf report --stdio --no-children > /tmp/perf_report.txt
python3 .agents/skills/velda-benchmark/scripts/analyze_perf.py /tmp/perf_report.txt --markdown
```
**Verification Criteria**:
- Frontline traffic worker call stacks must show **0.00%** in `otlp_trace_exporter`, `alloc`, or `futex`, and show `SpscSpanRing::push` completing in a tight register-to-streaming-store sequence.
- `encode_single_span_wire` must show **0.00%** calls to `alloc::string` or `core::fmt`.

### 6.2 Release Assembly Audit (`.s`)
```bash
cargo rustc -p velda-observability --lib --release -- --emit asm
python3 .agents/skills/velda-benchmark/scripts/audit_asm.py \
  target/release/deps/velda_observability-*.s --symbol SpscSpanRing --markdown

cargo rustc -p velda-plugin --lib --release -- --emit asm
python3 .agents/skills/velda-benchmark/scripts/audit_asm.py \
  target/release/deps/velda_plugin-*.s --symbol encode_single_span_wire --markdown
```
**Checklist**:
1. Confirm `vmovntdq` + `sfence` in `SpscSpanRing::push` and `vmovntdqa` + `prefetcht1` in `SpscSpanRing::drain_batch`.
2. Scan `encode_single_span_wire` for hidden `raw_vec::reserve` or stack register spilling (`movq %reg, offset(%rsp)`).
3. Measure whether `spans_wire` high-watermark retention requires explicit shrink thresholds after bursts.
