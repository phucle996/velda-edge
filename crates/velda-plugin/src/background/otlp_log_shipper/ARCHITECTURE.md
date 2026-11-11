# Architectural Specification: OpenTelemetry Log Shipper (`otlp_log_shipper`)

This document details the architectural invariants, threading model, framing mechanisms, and network socket management of the `otlp_log_shipper` background logging plugin in the **Velda Edge Data Plane**.

---

## 1. System Context & Dual-Transport Architecture

The `otlp_log_shipper` operates strictly on companion thread `velda-shadow-0` under Linux `SCHED_IDLE`. It provides native dual-transport support to target OpenTelemetry Collectors:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        VELDA EDGE DATA PLANE ARCHITECTURE                              │
│                                                                                        │
│   FRONTLINE TRAFFIC WORKERS (Core 0..N, SCHED_OTHER)                                  │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Client Ingress -> Route Matching -> Upstream Proxying -> Client Response       │   │
│   │                                                                                │   │
│   │ [Hot-Path Telemetry Hook]                                                      │   │
│   │ 64-byte scalar AccessLogRecord -> Core-Local SPSC Ring (< 70 CPU clock cycles) │   │
│   └───────────────────────────────────────┬────────────────────────────────────────┘   │
│                                           │ Atomic In-Memory Hand-off                  │
│                                           ▼                                            │
│   SHADOW LOGISTICS WORKER (velda-shadow-0, SCHED_IDLE)                                 │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ LogStore::flush_all_lanes() -> Batched Arc<[AccessLogRecord]> Slices           │   │
│   │                                                                                │   │
│   │ OtlpLogShipperPlugin::run()                                                    │   │
│   │  ├─ Transport Branching:                                                       │   │
│   │  │   ├─ [HTTP/1.1 JSON Path] (OtlpLogClient)                                   │   │
│   │  │   │   ├─ OtlpJsonEnvelope prefix / suffix framing                           │   │
│   │  │   │   ├─ Division-free 2-digit ASCII LUT integer conversion                 │   │
│   │  │   │   └─ Persistent HTTP/1.1 TCP stream write with Keep-Alive               │   │
│   │  │   │                                                                         │   │
│   │  │   └─ [gRPC HTTP/2 Path] (OtlpLogsGrpcClient)                                │   │
│   │  │       ├─ OtlpProtoEnvelope::format_records() -> &ExportLogsServiceRequest   │   │
│   │  │       ├─ In-place SIMD & multi-offset store coalescing into record_pool     │   │
│   │  │       ├─ 5-byte gRPC length-prefixed binary framing                         │   │
│   │  │       └─ Persistent HTTP/2 multiplexed stream transmission                  │   │
│   │  │                                                                             │   │
│   │  └─ Automatic Reference-Counted Batch Reclamation on Drop                      │   │
│   └────────────────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Pre-Baked Static Envelope Pattern (`OtlpJsonEnvelope`)

Standard OpenTelemetry JSON logging payloads require deeply nested JSON structures:
```json
{
  "resourceLogs": [{
    "resource": {
      "attributes": [
        {"key": "service.name", "value": {"stringValue": "velda-edge"}},
        {"key": "telemetry.sdk.name", "value": {"stringValue": "velda-edge"}}
      ]
    },
    "scopeLogs": [{
      "scope": {"name": "velda.access_log"},
      "logRecords": [ ... ]
    }]
  }]
}
```

Instead of reconstructing this JSON AST dynamically for every flush:
1. `OtlpJsonEnvelope::new(service_name)` runs **once** at plugin initialization time.
2. The outer JSON string is encoded into static byte buffers:
   - `prefix`: Contains everything up to `\"logRecords\":[`.
   - `suffix`: Contains static closing braces `b\"]}]}]}\"`.
3. During runtime flushes, `format_otlp_records_json` performs zero JSON object allocations:
   - Copies `prefix` into the pre-allocated reusable vector (`out: &mut Vec<u8>`).
   - Sequentially splices record JSON blocks using the pre-computed 200-byte lookup table (`DIGIT_PAIRS`).
   - Appends `suffix`.

---

## 3. Pre-Allocated Protobuf Template Engine (`OtlpProtoEnvelope`)

In addition to JSON, high-throughput clusters utilize OTLP/gRPC binary Protocol Buffers (`ExportLogsServiceRequest`). Constructing deep Prost protobuf AST trees per batch allocates thousands of heap objects and consumes >4,000 CPU clock cycles per record.

`OtlpProtoEnvelope` eliminates dynamic allocation via an in-place object pool architecture:
1. **Startup Pre-allocation**:
   - `OtlpProtoEnvelope::new(service_name)` constructs the outer `ExportLogsServiceRequest`, `ResourceLogs`, `Resource`, `ScopeLogs`, and `InstrumentationScope` structs once at startup.
   - Pre-allocates a capacity pool `record_pool: Vec<LogRecord>`.
2. **Pre-baked 14-Attribute Record Template**:
   - `LogRecord::template()` pre-allocates keys and string buffers for 14 standardized OpenTelemetry attributes, including an explicit `"request_id"` string attribute (capacity 16).
   - In accordance with distributed tracing boundaries, `trace_id` and `span_id` are **not** spoofed with request identifiers; they remain empty unless explicitly populated by distributed trace context propagation.
3. **SIMD & Multi-Offset Parallel Store Coalescing**:
   - `populate_from_record(&mut self, rec: &AccessLogRecord)` writes into pre-allocated template memory in-place:
     - **Dual-Timestamp SIMD Store**: On `x86_64`, `_mm_set1_epi64x` broadcast and `_mm_storeu_si128` write both `time_unix_nano` and `observed_time_unix_nano` in a single 128-bit store instruction.
     - **Coalesced Multi-Offset Stores**: Contiguous integer attributes (`duration_us`, `bytes_tx`, `bytes_rx`, `route_id`) and remaining scalar slots are mapped across non-branching tuple destructuring patterns, allowing CPU out-of-order execution across store ports (Ports 4, 7, and 8).
     - **Zero-Allocation Formatting**: Body text (`"request_id=... status=... dur_us=..."`) is written directly into pre-allocated string byte vectors using division-free 2-digit ASCII LUT conversion (`fast_write_u64`, `fast_write_hex16`, `fast_write_ip128`).
4. **Zero-Deallocation Recycling**:
   - During `format_records()`, records are transferred into the request's `scope_logs[0].log_records` array.
   - Upon gRPC completion, `reclaim()` uses `record_pool.append(&mut scope_logs[0].log_records)` to instantly reclaim templates without freeing memory, achieving **0 heap allocations** during steady-state request processing.

---

## 4. High-Performance Transport Clients

### 4.1 HTTP/1.1 Keep-Alive Client (`OtlpLogClient`)
- Manages an active `Option<tokio::net::TcpStream>`.
- Reuses established TCP connections across successive export intervals.
- Detects server-side idle connection teardown and transparently reconnects without failing the batch or blocking the thread.
- Automatically handles socket configuration: `TCP_NODELAY` is enabled to eliminate Nagle's algorithm buffering delays.

### 4.2 gRPC HTTP/2 Multiplexed Client (`OtlpLogsGrpcClient`)
- Establishes HTTP/2 binary framing via the `h2` engine.
- Formats log batches into binary Protocol Buffers (`ExportLogsServiceRequest`).
- Prepends canonical 5-byte gRPC framing:
  - Byte 0: `0x00` (Uncompressed payload flag).
  - Bytes 1–4: Big-endian 32-bit unsigned integer representing the exact payload length.
- Streams requests across multiplexed HTTP/2 streams over a single long-lived TCP connection to port 4317.

---

## 5. Shadow Worker Threading & Zero-Contention Execution

- **Affinity**: Bound strictly to CPU Core 0 alongside primary traffic worker `velda-worker-0`.
- **CFS Preemption**: Because `otlp_log_shipper` runs under Linux `SCHED_IDLE`, any inbound traffic to `velda-worker-0` instantly preempts the background export routine with 0 nanoseconds lock contention.
- **Fail-Open Collector Isolation**: If the OpenTelemetry Collector crashes, drops connections, or experiences network partitions, the exporter drops stale batches, increments `failed_pushes`, and backs off gracefully without impacting edge request serving throughput.
