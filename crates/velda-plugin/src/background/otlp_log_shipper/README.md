# OpenTelemetry Log Shipper Plugin (`otlp_log_shipper`)

The `otlp_log_shipper` plugin is a high-performance background telemetry service for the **Velda Edge Data Plane**. It subscribes to reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches across all application protocols and exports standardized OpenTelemetry `ResourceLogs` payloads directly to OpenTelemetry Collectors via **OTLP/HTTP** (JSON over persistent HTTP/1.1 Keep-Alive) or **OTLP/gRPC** (binary Protocol Buffers over HTTP/2).

---

## 1. Overview & Service Specification

| Property | Value |
| :--- | :--- |
| **Plugin Identifier** | `otlp_log_shipper` |
| **Canonical Execution Phase** | `background` (Dedicated Shadow Logistics Worker) |
| **Execution Runtime** | Pinned to `velda-shadow-0` under Linux `SCHED_IDLE` scheduling priority |
| **Capability Declaration** | `PluginCapability::default()` (Non-request-serving background logistics service) |
| **Supported Ingress Protocols** | Ingests access log records across HTTP/1.1, HTTP/2, HTTP/3, gRPC, and Raw L4 TCP/UDP |
| **Hot-Path Request Overhead** | **0 nanoseconds serving cost**; zero lock contention, zero string allocations, zero I/O on traffic cores |
| **Export Transports** | OTLP/HTTP (JSON over HTTP/1.1, default port 4318) or OTLP/gRPC (Protobuf over HTTP/2, default port 4317) |
| **OpenTelemetry Compliance** | Conforms to OpenTelemetry Log Data Model (`opentelemetry.proto.logs.v1`) |

---

## 2. Technical Documentation Index

| Document | Purpose & Scope |
| :--- | :--- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Architectural specification, pre-baked static JSON envelope (`OtlpJsonEnvelope`), persistent connection reuse, Protobuf encoder, and `SCHED_IDLE` lifecycle |
| [MACHINE_CODE_ANALYSIS.md](MACHINE_CODE_ANALYSIS.md) | Assembly inspection, memory alignment, hardware CPU cycle counts via `RDTSC`, division-free LUT serialization, and A/B co-existence verification |
| [BENCHMARK.md](BENCHMARK.md) | Standardized 4-tier benchmark specification form (Macro Serving, System Resources, Kernel Network & Syscalls, Hardware PMU Counters) across five operational scenarios |
| [RUNBOOK.md](RUNBOOK.md) | Site Reliability Engineering (SRE) operational playbook, severity classification matrix, incident decision flowchart, diagnostic tooling, and collector blackhole remediation |
| [TEST.md](TEST.md) | Automated unit, integration, and machine code test catalog, schema validation, transparent reconnection tests, and reproduction commands |

---

## 3. Strict 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Invariant**: Every configuration field must be explicitly declared in `plugins.json`. Velda Edge strictly rejects implicit fallbacks, magic defaults, or omitted properties. If any property is omitted or malformed, configuration validation fails fast at synchronization time.

| Field | Type | Required | Constraints / Allowed Values | Recommended Production Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `protocol` | string | **Yes** | `"http"` or `"grpc"` | `"http"` (High Concurrency)<br>`"grpc"` (Low Wire Overhead) | Transport protocol for shipping logs to OpenTelemetry Collector. |
| `endpoint` | string | **Yes** | Valid URI starting with `http://`, `https://`, `grpc://`, or `grpcs://` | `"http://otel-collector:4318/v1/logs"` | Ingestion endpoint URL of the target OpenTelemetry Collector. |
| `service_name` | string | **Yes** | Non-empty string (`^[a-zA-Z0-9_\-\.]+$`) | `"velda-edge-prod"` | Logical service identifier reported in OpenTelemetry Resource attributes (`service.name`). |
| `flush_interval_ms` | integer (u64) | **Yes** | `100 ..= 60000` (milliseconds) | `100` (100 milliseconds) | Periodic timer interval for waking up shadow worker to drain SPSC rings and ship buffered records. |
| `max_batch_size` | integer (usize) | **Yes** | `1 ..= 10000` (records) | `1000` | Maximum number of log records grouped into a single exported payload. |
| `connect_timeout_ms`| integer (u64) | **Yes** | `100 ..= 60000` (milliseconds) | `1000` (1 second) | TCP and TLS connection establishment timeout before aborting and retrying. |
| `request_timeout_ms`| integer (u64) | **Yes** | `100 ..= 60000` (milliseconds) | `1000` (1 second) | Maximum round-trip time allowed for an HTTP/gRPC export request to complete. |
| `headers` | object | **Yes** | Key-value map of string pairs (use `{}` if empty) | `{}` or Auth headers | Custom HTTP or gRPC headers sent with every export request (for example, authorization tokens, tenant IDs). |

---

## 4. Production Configuration Profiles

### Profile 1: OTLP / HTTP Keep-Alive Production Profile (Default Port 4318)
*Recommended for production clusters with HTTP-based OpenTelemetry log aggregation pipelines.*

```json
{
  "id": "otlp_log_shipper",
  "phase": "background",
  "config": {
    "protocol": "http",
    "endpoint": "http://otel-collector.monitoring.svc.cluster.local:4318/v1/logs",
    "service_name": "velda-edge-production",
    "flush_interval_ms": 100,
    "max_batch_size": 1000,
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 1000,
    "headers": {
      "X-Environment": "production"
    }
  }
}
```

### Profile 2: OTLP / gRPC Binary Protobuf Profile (Default Port 4317)
*Recommended for high-throughput environments prioritizing minimal wire bandwidth via binary Protobuf framing.*

```json
{
  "id": "otlp_log_shipper",
  "phase": "background",
  "config": {
    "protocol": "grpc",
    "endpoint": "grpc://otel-collector.monitoring.svc.cluster.local:4317",
    "service_name": "velda-edge-production",
    "flush_interval_ms": 100,
    "max_batch_size": 2000,
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 1000,
    "headers": {}
  }
}
```

---

## 5. Architectural Invariants

1. **Pre-Baked Static JSON Envelope (`OtlpJsonEnvelope`)**: The complex OpenTelemetry outer JSON envelope containing resource attributes and scope descriptors is serialized once at startup into byte vectors, completely avoiding runtime format string construction.
2. **Persistent Connection Reuse**: Connections are kept open across export cycles via persistent TCP keep-alive, preventing connection thrashing and repeated handshakes.
3. **Reference-Counted Memory Reclamation**: Consumes `Arc<[AccessLogRecord]>` batches from `LogStore`. Memory is automatically returned to the global pool once the last active exporter drops its batch reference.
