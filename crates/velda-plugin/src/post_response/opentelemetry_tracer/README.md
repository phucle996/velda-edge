# OpenTelemetry Tracer Plugin (`opentelemetry_tracer`)

Ultra-high-performance, asynchronous distributed trace span reporter for the **Velda Edge Data Plane**. Captures completed request lifecycle spans during the `post_response` phase using cacheline-aligned 256-byte POD structs (`PackedOtlpSpan`), streaming directly to page-aligned RAM slots via AVX2 non-temporal vector stores (`_mm256_stream_si256`), and batch exporting via **OTLP/gRPC** (Protobuf over HTTP/2) or **OTLP/HTTP** (JSON over HTTP/1.1) on Shadow Worker threads.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `opentelemetry_tracer` |
| **Canonical Phase** | `post_response` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (Non-mutating telemetry probe) |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, and gRPC) |
| **Hot-Path Overhead** | **$< 3\ \text{ns}$** (sampled push) / **$1.4\ \text{ns}$** (unsampled escape); zero heap alloc, zero locks |
| **Record Memory Footprint** | **Exact 256 bytes** (`#[repr(C, align(64))]`), exactly 4 CPU cache lines |

### What it Captures
- **Distributed Trace Identifiers**: Captures `trace_id` (16 bytes binary), `span_id` (8 bytes binary), and `parent_span_id` (8 bytes binary) directly from [`HeaderWorkspace`](file:///home/phucle/Desktop/velda-edge/crates/velda-core/src/buffer/header.rs), representing the edge gateway as an authoritative hop in distributed trace graphs.
- **Hierarchical Trace Continuity**: Seamlessly extracts `x-parent-span-id` injected by `w3c_trace_context` to link client-initiated spans to upstream backends, eliminating orphan root spans in Jaeger and Grafana Tempo.
- **Span Kind & Timing**: Stamps span kind as `SPAN_KIND_SERVER (2)`, recording high-resolution nanosecond timestamps (`start_time_unix_nano`, `end_time_unix_nano`).
- **Span Status**: Automatically flags spans as `STATUS_CODE_OK (1)` for status codes $< 400$, or `STATUS_CODE_ERROR (2)` for status codes $\ge 400$.
- **Semantic Attributes**: Enriches spans with `http.request.method`, `http.response.status_code`, `url.path`, `http.route`, `client.address`, and `network.peer.address` (upstream backend).

---

## 2. Micro-Architectural Guarantees

### A. 256-Byte Cacheline-Aligned POD (`PackedOtlpSpan`)
- Exactly 4 CPU cache lines (64 bytes each).
- Contains zero pointers, zero dynamic `String` allocations, and zero heap indirection.
- Binary 128-bit `trace_id` and 64-bit `span_id` / `parent_span_id` are stored directly in register-friendly byte arrays.

### B. Hardware-Harmonized Vector Transport (`simd.rs`)
- On x86_64 CPUs with AVX2, writes are executed via 8 × 256-bit `_mm256_stream_si256` non-temporal vector stores followed by an `_mm_sfence()`.
- Data flows directly from worker stack registers to DRAM Write-Combining Buffers, completely **bypassing L1/L2/L3 CPU cache pollution** on the frontline serving core.

### C. Sharded SPSC Ring Buffer in Physical RAM (`ring_buffer.rs`)
- Each Frontline Traffic Worker thread accesses strictly its own dedicated SPSC ring buffer shard (`lane = worker_id`).
- Heads, tails, and slot buffers are aligned to 64-byte boundaries, eliminating False Sharing across CPU cores.
- Handoff from Traffic Worker to Shadow Logistics Worker takes **$\sim 1\ \text{ns}$**.

### D. Direct-Index Status-Aware Sampling Engine (`sampling.rs`)
- Direct-index 600-entry array for status codes `0..=599`.
- Status codes marked with 0% sampling escape on the hot path in **$1.4\ \text{ns}$** with zero branch mispredictions.

---

## 3. Configuration Field Specification

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: To ensure absolute operational transparency, **every parameter must be explicitly declared** in the JSON configuration. Velda Edge rejects hidden conventions and magic defaults.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :---: | :--- | :--- | :--- |
| `protocol` | string | **Required** | `"grpc"` or `"http"` | `"grpc"` (Production)<br>`"http"` (Debug) | Transport protocol. `"grpc"` exports binary Protobuf via HTTP/2 (port 4317); `"http"` exports JSON via HTTP/1.1 (port 4318). |
| `endpoint` | string | **Required** | Valid URI starting with `http://`, `https://`, `grpc://`, or `grpcs://` | `"grpc://127.0.0.1:4317"` | Target OpenTelemetry Collector ingestion endpoint. |
| `service_name` | string | **Required** | Non-empty string (`^[a-zA-Z0-9_\-\.]+$`) | `"velda-edge-prod"` | Logical service identifier reported in OpenTelemetry Resource attributes (`service.name`). |
| `batch_size` | integer | **Required** | `1` to `4096` | `256` (High Load)<br>`64` (Low Load) | Number of spans buffered before triggering an immediate export flush. |
| `flush_interval_ms` | integer | **Required** | `10` to `60000` (milliseconds) | `200` (Production)<br>`50` (Low Latency) | Maximum duration to hold pending spans before flushing an incomplete batch. |
| `buffer_capacity` | integer | **Required** | `128` to `262144` | `65536` | Pre-allocated slot capacity per CPU core lane in physical RAM. |
| `connect_timeout_ms` | integer | **Required** | `100` to `60000` (milliseconds) | `5000` (5s) | TCP and TLS connection establishment timeout before aborting and retrying. |
| `request_timeout_ms` | integer | **Required** | `100` to `60000` (milliseconds) | `10000` (10s) | Maximum round-trip timeout allowed for an export request to complete. |
| `headers` | object | **Required** | Key-value map of string pairs (use `{}` if none) | `{}` or Auth tokens | Custom HTTP or gRPC headers sent with every export request. |
| `sampling` | object | Optional | Map of 3-digit HTTP status codes to rates `0.0..=1.0` | `{"default": 1.0, "200": 0.05, "500": 1.0}` | Per-status sampling rates. |

---

## 4. Configuration Examples

### Profile 1: High-Throughput Production Profile (OTLP / gRPC)

```json
{
  "id": "opentelemetry_tracer",
  "config": {
    "protocol": "grpc",
    "endpoint": "grpc://otel-collector.monitoring.svc.cluster.local:4317",
    "service_name": "velda-edge-prod",
    "batch_size": 256,
    "flush_interval_ms": 200,
    "buffer_capacity": 65536,
    "connect_timeout_ms": 5000,
    "request_timeout_ms": 10000,
    "headers": {
      "x-api-key": "prod-tracing-secret"
    },
    "sampling": {
      "default": 1.0,
      "200": 0.05,
      "500": 1.0
    }
  }
}
```

### Profile 2: Standard REST Profile (OTLP / HTTP JSON)

```json
{
  "id": "opentelemetry_tracer",
  "config": {
    "protocol": "http",
    "endpoint": "http://127.0.0.1:4318/v1/traces",
    "service_name": "velda-edge-local",
    "batch_size": 32,
    "flush_interval_ms": 100,
    "buffer_capacity": 4096,
    "connect_timeout_ms": 5000,
    "request_timeout_ms": 10000,
    "headers": {},
    "sampling": {
      "default": 1.0
    }
  }
}
```

---

## 5. Route Attachment Example (`routes.json`)

```json
{
  "id": "api-orders-route",
  "path": "/api/v1/orders",
  "upstream": "orders-backend",
  "plugins": [
    "w3c_trace_context",
    "rate_limit",
    "security_headers",
    "opentelemetry_tracer"
  ]
}
```

---

## 6. Failure Modes & Operational Resilience

1. **Non-Blocking Hot Path Invariant**: The serving worker thread uses `ShardedRingBuffer::try_push`. If a lane shard is saturated, the span is dropped immediately in $< 1\ \text{ns}$ and `dropped_count` is incremented. **Under no circumstances will client traffic be paused or stalled by telemetry export.**
2. **Transparent Reconnect**: Maintains a persistent keep-alive connection to the OTLP Collector. If the connection drops or the remote server restarts, the flusher detects EOF and reconnects transparently on the next flush cycle without losing the current batch.
3. **Queue Draining on Shutdown**: When the edge node receives a termination signal, the flusher drains any remaining spans in the ring buffer before terminating.
