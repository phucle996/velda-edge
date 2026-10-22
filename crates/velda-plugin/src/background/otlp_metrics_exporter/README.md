# OpenTelemetry Metrics Exporter Plugin (`otlp_metrics_exporter`)

High-performance, lock-free OpenTelemetry Metrics background exporter for the **Velda Edge Data Plane**. Periodically extracts point-in-time [`velda_observability::MetricsSnapshot`] states and pushes compliant OpenTelemetry `ResourceMetrics` payloads via either **OTLP/gRPC** (Protobuf over HTTP/2) or **OTLP/HTTP** (JSON over HTTP/1.1).

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `otlp_metrics_exporter` |
| **Canonical Phase** | `background` (Autonomous Worker) |
| **Execution Runtime** | Evaluated strictly on Shadow Worker supervisory threads (`velda-shadow-0`) with idle priority (`SCHED_IDLE`) |
| **Reverse Capability** | `PluginCapability::default()` (Non-request-serving background auxiliary service) |
| **Supported Protocols** | Protocol-agnostic telemetry ingestion (Reports metrics across HTTP/1.1, HTTP/2, HTTP/3, and gRPC) |
| **Hot-Path Overhead** | **0 ns serving cost**; zero lock contention, zero allocations, and zero network I/O on traffic worker cores |

### What it Exports
- **`velda.requests.total`**: Cumulative counter of requests partitioned by downstream protocol, upstream cluster, pipe strategy, and HTTP status code class (`2xx`, `3xx`, `4xx`, `5xx`, `other`).
- **`velda.active_streams`**: Instantaneous gauge of active in-flight streams currently served by the edge node.
- **`velda.bytes_received_total`**: Cumulative network ingress bytes read from downstream clients.
- **`velda.bytes_transmitted_total`**: Cumulative network egress bytes written to downstream clients.

### Out of Scope
- **Metric Mutation & Counter Storage**: Owned centrally by [`velda_observability::MetricsStore`]. Exporter plugins are strictly read-only snapshot consumers.
- **Request-Level Access Logging**: Owned by `opentelemetry_logger` during `post_response`.
- **Distributed Request Tracing**: Owned by `opentelemetry_tracer` and `w3c_trace_context`.

---

## 2. Configuration Field Specification

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: To ensure absolute operational transparency, **every parameter must be explicitly declared** in the JSON configuration. Velda Edge rejects hidden conventions and magic defaults. If any parameter is omitted, configuration validation will fail fast during sync.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :---: | :--- | :--- | :--- |
| `protocol` | string | **Required** | `"grpc"` or `"http"` | `"grpc"` (Production)<br>`"http"` (Debug) | Transport protocol. `"grpc"` exports binary Protobuf via HTTP/2 (port 4317); `"http"` exports JSON via HTTP/1.1 (port 4318). |
| `endpoint` | string | **Required** | Valid URI starting with `http://`, `https://`, `grpc://`, or `grpcs://` | `"grpc://127.0.0.1:4317"` | Target OpenTelemetry Collector ingestion endpoint. |
| `interval_secs` | integer | **Required** | `1` to `3600` (seconds) | `10` (Production)<br>`1` (Staging/Dev) | Telemetry push interval for extracting lock-free snapshots from `MetricsStore`. |
| `connect_timeout_ms` | integer | **Required** | `100` to `60000` (milliseconds) | `5000` (5s) | TCP and TLS connection establishment timeout before aborting and retrying. |
| `request_timeout_ms` | integer | **Required** | `100` to `60000` (milliseconds) | `10000` (10s) | Maximum round-trip timeout allowed for an export request to complete. |
| `service_name` | string | **Required** | Non-empty string (`^[a-zA-Z0-9_\-\.]+$`) | `"velda-edge-prod"` | Logical service identifier reported in OpenTelemetry Resource attributes (`service.name`). |
| `headers` | object | **Required** | Key-value map of string pairs (use `{}` if none) | `{}` or Auth tokens | Custom HTTP or gRPC headers sent with every export request (e.g., Bearer tokens, tenant IDs). |

---

## 3. Dual Protocol Architecture & Wire Encoding

The exporter encapsulates two fully isolated, high-performance transport clients:

### A. OTLP / gRPC (Production Standard)
- **Transport**: Persistent HTTP/2 connection (`h2`) with multiplexed binary streams.
- **RPC Service**: `/opentelemetry.proto.collector.metrics.v1.MetricsService/Export`
- **Framing**: Canonical 5-byte Length-Prefixed Message (`[1B compressed flag = 0x00] [4B big-endian length] [Protobuf payload]`).
- **Wire Codec**: Pure Rust Protocol Buffers via `prost` without external C++ `protoc` dependencies.
- **Overhead**: **65%–80% smaller payload size** compared to JSON, near-zero CPU serialization overhead.

### B. OTLP / HTTP (Inspection & REST)
- **Transport**: Persistent HTTP/1.1 stream with `Connection: keep-alive`.
- **REST Endpoint**: `POST /v1/metrics`
- **Wire Codec**: OpenTelemetry ResourceMetrics JSON (`application/json`).
- **Use Case**: Testing, direct browser/curl verification, or environments with gRPC-restricted firewalls.

---

## 4. Configuration Examples

### Profile 1: High-Throughput Production Profile (OTLP / gRPC)
*Recommended for production clusters to minimize CPU usage and network egress.*

```json
{
  "id": "otlp_metrics_exporter",
  "config": {
    "protocol": "grpc",
    "endpoint": "grpc://otel-collector.monitoring.svc.cluster.local:4317",
    "interval_secs": 10,
    "connect_timeout_ms": 5000,
    "request_timeout_ms": 10000,
    "service_name": "velda-edge-us-east-1",
    "headers": {
      "x-tenant-id": "prod-telemetry"
    }
  }
}
```

### Profile 2: Standard REST Profile (OTLP / HTTP JSON)
*Recommended for local debugging, development, or REST-only metric collectors.*

```json
{
  "id": "otlp_metrics_exporter",
  "config": {
    "protocol": "http",
    "endpoint": "http://127.0.0.1:4318/v1/metrics",
    "interval_secs": 5,
    "connect_timeout_ms": 5000,
    "request_timeout_ms": 10000,
    "service_name": "velda-edge-local",
    "headers": {}
  }
}
```

---

## 5. Failure Modes & Operational Resilience

1. **Hot-Path Isolation**: Metrics are read via atomic lock-free snapshots with `Ordering::Relaxed`. Metric collection **never halts or blocks request-serving worker threads**, even if the OTLP Collector is completely unreachable or deadlocked.
2. **Persistent Keep-Alive & Reconnect**: The exporter maintains exactly 1 persistent connection to the collector. If the remote collector restarts or closes the connection while idle, the client detects EOF, clears the socket, and reconnects transparently on the next tick.
3. **Graceful Shutdown**: Listens to the Data Plane supervisory `shutdown` watch channel. In-flight flushes complete cleanly before terminating the background worker thread.
