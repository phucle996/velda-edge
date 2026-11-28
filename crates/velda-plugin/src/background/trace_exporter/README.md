# Background Trace Exporter Plugin (`trace_exporter`)

## Architecture Overview

```text
  Frontline Traffic Workers (velda-worker-{0..N-1}, SCHED_OTHER, Zero IO / Zero Locks)
    └─► Request Completion ─► StatusSampling (3-ALU Weyl Gate: add/shr/cmp)
          │
          └─► keep == true ─► TelemetryRecord (256B, 4 Cache Lines, Span fields inside CL0 0..64B)
                │               └─ non-temporal store (_mm256_stream) -> SpscTelemetryRing (2MB HugePage/core)
                ▼
  TelemetryBatchPool (64 Slots × 256 Records = 64KB Contiguous Pages, Multi-Buffer Rotation)
                │
                ▼ wait-free fan-out (leases = K subscribers, zero-copy pool borrow)
  Shadow Logistics Worker (velda-shadow-{i}, SCHED_IDLE, pbww = 50):
  ┌────────────────────────────────────────────────────────────────────────────────────────┐
  │ TraceExporterPlugin::run(shutdown) ──► JoinSet per TraceTargetConfig:                  │
  │   └─► attach_etp(etp_id) ─► OtlpTraceProvider (velda-etp::OtlpEtp)                     │
  │         ├─ HttpProtobuf ─► OtlpTraceProtoEnvelope (CL0-only binary varint + LUT)      │
  │         ├─ HttpJson     ─► OtlpTraceJsonEnvelope (CL0-only hex + itoa)                 │
  │         └─ Grpc         ─► OtlpTraceProtoEnvelope + 5B H2 length-prefixed framing      │
  └────────────────────────────────────────────────────────────────────────────────────────┘
```

## 1. What This Plugin Does

`trace_exporter` captures distributed request spans from Velda Edge's lock-free `TelemetryStore` and ships OpenTelemetry OTLP trace batches asynchronously to one or more collectors without putting parsing, allocation, or exporter network latency onto Data Plane request workers:
- **Zero-Contentious Fan-Out Over Shared Pools (`TelemetryStore`)**: Instead of building separate span structs or duplicating request events per telemetry sink, Data Plane workers write one `256 B` `TelemetryRecord` into a core-local `SpscTelemetryRing`. All `log_exporter` and `trace_exporter` targets borrow identical `64 KB` pooled batches via atomic reference leases (`TelemetryBatchPool`).
- **Cache Line 0 Span Isolation**: Every field consumed by `trace_exporter` (`request_id`, `duration_ns`, `peer_ip`, `ingress_time_nanos`, `route_id`, `upstream_id`, `method`, `internal_status`) packs contiguously inside the first `64 B` cache line of `TelemetryRecord`, skipping CL1–CL3 URL bytes (`184 B`) completely during trace serialization.
- **Zero-Alloc `OtlpTraceProtoEnvelope` & `OtlpTraceJsonEnvelope`**: Bypassing `prost::Message` intermediate trees on export, `trace_exporter` pre-bakes `Resource` (`service.name`) and `InstrumentationScope` (`velda.edge.router`) wire headers at startup and encodes binary OTLP `ExportTraceServiceRequest` payloads directly via varint/LE writes + wait-free `RouteCatalog` / `UpstreamCatalog` lookups.
- **Multi-Target Fan-Out (`targets`)**: A single `"trace_exporter"` plugin binds multiple `etp_id` endpoints concurrently (OTLP HTTP/Protobuf, OTLP HTTP/JSON, and OTLP gRPC `h2c`/TLS simultaneously) through `BackgroundPlugin::etp_ids()` + `attach_etp()`.

## 2. Where It Sits in Velda Edge

- **Pipeline Stage**: `background` (`BackgroundPlugin`, assigned onto a `SCHED_IDLE` Shadow Worker via static LPT scheduling with `pbww = 50`).
- **Worker Placement**:
  - Sampling (`StatusSampling`) and ring insertion run wait-free on **Frontline Traffic Workers** using 3-ALU Weyl gates (`add`, `shr`, `cmp`) and core-local SPSC rings.
  - Batch draining (`subscriber.drain_batches(64)`), serialization, and HTTP/1.1 or HTTP/2 gRPC transport run asynchronously on **Shadow Logistics Workers**.
- **Supported Protocols**: `SupportedProtocols::empty()` (consumes unified spans across all L7 ingress pipelines).

## 3. Requirements & Prerequisites

- **Target ETP Pools (`etp.json`)**: Every entry inside `targets` must specify a non-empty `etp_id` resolving to an `Otlp` pool (`velda-etp::OtlpEtp`) supporting `"http"` / `"http/protobuf"`, `"http/json"` / `"json"`, or `"grpc"`. Non-OTLP ETPs are rejected during `attach_etp()`.
- **Listener Sampling (`listeners.json`)**: Optional `observability.sampling` (`default`, `statuses` overrides per `InternalStatus`, and `shed` rate) gates how much traffic enters `TelemetryStore` (defaults to `1.0` / `100%` when omitted).

## 4. Quick Start Configuration

```json
{
  "id": "trace_exporter",
  "config": {
    "targets": [
      {
        "etp_id": "otlp-http-proto",
        "service_name": "velda-edge",
        "flush_interval_ms": 100,
        "max_batch_size": 1000,
        "request_timeout_ms": 5000
      },
      {
        "etp_id": "otlp-grpc",
        "service_name": "velda-edge",
        "flush_interval_ms": 100,
        "max_batch_size": 1000,
        "request_timeout_ms": 5000
      }
    ]
  }
}
```

## 5. Documentation Directory

- [RUNBOOK.md](./RUNBOOK.md) — Configuration schema (`TraceTargetConfig`), ETP transport recipes, high-RPS `flush_interval_ms` sizing math, backoff/cooldown behavior, and incident triage.
- [ARCHITECTURE.md](./ARCHITECTURE.md) — `TelemetryRecord` Cache Line 0 geometry, `SpscTelemetryRing` + `TelemetryBatchPool` multi-buffer rotation, Weyl sampling, and pre-baked protobuf/gRPC framing.
- [BENCHMARK.md](./BENCHMARK.md) — 2-KVM `tc netem` 5-tier comparison against APISIX, Kong, and Envoy feeding an **official `otel/opentelemetry-collector-contrib` (`v0.166.0`)** collector, sampling scaling, outage recovery, and `x86-64-v3` ASM audit.
- [TEST.md](./TEST.md) — Executed unit test suites (`velda-plugin`, `velda-observability`) and real `otelcol-contrib` wire verification report.
