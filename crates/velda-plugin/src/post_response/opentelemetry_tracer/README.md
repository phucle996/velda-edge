# OpenTelemetry Tracer Plugin (`opentelemetry_tracer`)

High-performance, asynchronous distributed trace span reporter for the **Velda Edge Data Plane**. Captures completed request lifecycle spans during the `post_response` phase, enqueuing them into a lock-free bounded channel for batch export via **OTLP/gRPC** (Protobuf over HTTP/2) or **OTLP/HTTP** (JSON over HTTP/1.1) on Shadow Worker threads.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `opentelemetry_tracer` |
| **Canonical Phase** | `post_response` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (Non-mutating telemetry probe) |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, and gRPC) |
| **Hot-Path Overhead** | **$< 30\ \text{ns}$**; zero disk I/O, zero network I/O, zero lock contention on traffic worker cores |

### What it Captures
- **Distributed Trace Identifiers**: Captures `trace_id` (16 bytes / 32 hex), `span_id` (8 bytes / 16 hex), and `parent_span_id` (8 bytes / 16 hex) to represent the edge gateway node as an authoritative hop in distributed trace graphs.
- **Span Kind & Timing**: Stamps span kind as `SPAN_KIND_SERVER (2)`, recording high-resolution nanosecond timestamps (`start_time_unix_nano`, `end_time_unix_nano`).
- **Span Status**: Automatically flags spans as `STATUS_CODE_OK (1)` for status codes `< 400`, or `STATUS_CODE_ERROR (2)` for status codes `>= 400`.
- **Semantic Attributes**: Enriches spans with `http.request.method`, `http.response.status_code`, `url.path`, `http.route`, `client.address`, and `network.peer.address` (upstream backend).

### Out of Scope
- **W3C Header Generation & Propagation**: Creating or validating `traceparent` headers for upstream requests is owned by `w3c_trace_context` during `pre_upstream`.
- **Structured Access Logging**: Request logging is owned by `opentelemetry_logger`.
- **System Metrics**: Aggregated edge metrics are owned by `otlp_metrics_exporter`.

---

## 2. Configuration Field Specification

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: To ensure absolute operational transparency, **every parameter must be explicitly declared** in the JSON configuration. Velda Edge rejects hidden conventions and magic defaults. If any parameter is omitted, configuration validation will fail fast during sync.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :---: | :--- | :--- | :--- |
| `protocol` | string | **Required** | `"grpc"` or `"http"` | `"grpc"` (Production)<br>`"http"` (Debug) | Transport protocol. `"grpc"` exports binary Protobuf via HTTP/2 (port 4317); `"http"` exports JSON via HTTP/1.1 (port 4318). |
| `endpoint` | string | **Required** | Valid URI starting with `http://`, `https://`, `grpc://`, or `grpcs://` | `"grpc://127.0.0.1:4317"` | Target OpenTelemetry Collector ingestion endpoint. |
| `service_name` | string | **Required** | Non-empty string (`^[a-zA-Z0-9_\-\.]+$`) | `"velda-edge-prod"` | Logical service identifier reported in OpenTelemetry Resource attributes (`service.name`). |
| `batch_size` | integer | **Required** | `1` to `4096` | `256` (High Load)<br>`64` (Low Load) | Number of spans buffered before triggering an immediate export flush. |
| `flush_interval_ms` | integer | **Required** | `10` to `60000` (milliseconds) | `200` (Production)<br>`50` (Low Latency) | Maximum duration to hold pending spans before flushing an incomplete batch. |
| `buffer_capacity` | integer | **Required** | `128` to `262144` | `65536` | Bounded capacity of the lock-free mpsc queue. Determines maximum memory reserved for in-flight spans. |
| `connect_timeout_ms` | integer | **Required** | `100` to `60000` (milliseconds) | `5000` (5s) | TCP and TLS connection establishment timeout before aborting and retrying. |
| `request_timeout_ms` | integer | **Required** | `100` to `60000` (milliseconds) | `10000` (10s) | Maximum round-trip timeout allowed for an export request to complete. |
| `headers` | object | **Required** | Key-value map of string pairs (use `{}` if none) | `{}` or Auth tokens | Custom HTTP or gRPC headers sent with every export request (e.g., Ingestion tokens, Authorization). |

---

## 3. Dual Protocol Architecture & Wire Encoding

### A. OTLP / gRPC (Production Standard)
- **Transport**: Persistent HTTP/2 connection (`h2`) with multiplexed binary streams.
- **RPC Service**: `/opentelemetry.proto.collector.trace.v1.TraceService/Export`
- **Framing**: Canonical 5-byte Length-Prefixed Message (`[1B compressed flag = 0x00] [4B big-endian length] [Protobuf payload]`).
- **Wire Codec**: Pure Rust Protocol Buffers via `prost` (`ExportTraceServiceRequest`).
- **Overhead**: **70%–80% smaller payload size** compared to JSON. Trace IDs and Span IDs are sent as raw 16-byte and 8-byte buffers, maximizing trace export throughput.

### B. OTLP / HTTP (Inspection & REST)
- **Transport**: Persistent HTTP/1.1 stream with `Connection: keep-alive`.
- **REST Endpoint**: `POST /v1/traces`
- **Wire Codec**: OpenTelemetry ResourceSpans JSON (`application/json`).
- **Use Case**: Testing, trace payload validation, or environments without HTTP/2 support.

---

## 4. Configuration Examples

### Profile 1: High-Throughput Production Profile (OTLP / gRPC)
*Recommended for production clusters to maximize span ingestion throughput and minimize egress bandwidth.*

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
    }
  }
}
```

### Profile 2: Standard REST Profile (OTLP / HTTP JSON)
*Recommended for development, staging, or debug environments.*

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
    "headers": {}
  }
}
```

---

## 5. Route Attachment Example (`routes.json`)

To activate the OpenTelemetry Tracer on an edge route, attach its plugin identifier to the route's `plugins` sequence in `routes.json`:

```json
{
  "id": "api-orders-route",
  "path": "/api/v1/orders",
  "upstream": "orders-backend",
  "plugins": [
    "w3c_trace_context",
    "rate_limit",
    "opentelemetry_logger",
    "opentelemetry_tracer"
  ]
}
```

---

## 6. Failure Modes & Operational Resilience

1. **Non-Blocking Hot Path Invariant**: The serving worker thread uses `mpsc::Sender::try_send`. If the queue is saturated, the span is immediately dropped and `dropped_count` is incremented. **Under no circumstances will client traffic be paused or stalled by telemetry export.**
2. **Transparent Reconnect**: Maintains exactly 1 persistent connection to the OTLP Collector. If the connection drops or the remote server restarts, the flusher detects EOF and reconnects transparently on the next flush cycle without losing the current batch.
3. **Queue Draining on Shutdown**: When the edge node receives a termination signal, the flusher drains any remaining spans in the queue before terminating.
