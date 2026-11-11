# Grafana Loki Log Shipper Plugin (`loki_log_shipper`)

The `loki_log_shipper` plugin is an ultra-fast, bounded-cardinality background logging exporter for the **Velda Edge Data Plane**. It subscribes to reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches across all application protocols, partitions records by protocol family (`protocol="http"`, `protocol="grpc"`, `protocol="raw"`) into Grafana Loki multi-stream JSON payloads, and pushes them directly to `/loki/api/v1/push` over persistent HTTP/1.1 Keep-Alive connections.

---

## 1. Overview & Service Specification

| Property | Value |
| :--- | :--- |
| **Plugin Identifier** | `loki_log_shipper` |
| **Canonical Execution Phase** | `background` (Dedicated Shadow Logistics Worker) |
| **Execution Runtime** | Pinned to `velda-shadow-0` under Linux `SCHED_IDLE` scheduling priority |
| **Capability Declaration** | `PluginCapability::default()` (Non-request-serving background logistics service) |
| **Supported Ingress Protocols** | Ingests and partitions records across HTTP/1.1, HTTP/2, HTTP/3, gRPC, and Raw L4 TCP/UDP |
| **Hot-Path Request Overhead** | **0 nanoseconds serving cost**; zero lock contention, zero string allocations, zero I/O on traffic cores |
| **Export Protocol** | Grafana Loki Push API (`/loki/api/v1/push`) via HTTP/1.1 persistent Keep-Alive connections |
| **Multi-Tenancy Support** | Native support for multi-tenant headers (`X-Scope-OrgID`) |
| **Index Cardinality Control** | Strictly bounds label index cardinality by partitioning only by static labels and protocol families |

---

## 2. Technical Documentation Index

| Document | Purpose & Scope |
| :--- | :--- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Architectural specification, multi-stream partitioning model, pre-baked stream descriptors (`LokiStreamTemplate`), and index cardinality protection |
| [MACHINE_CODE_ANALYSIS.md](MACHINE_CODE_ANALYSIS.md) | Assembly inspection, memory alignment, hardware CPU cycle counts via `RDTSC`, division-free LUT serialization, and A/B co-existence verification |
| [BENCHMARK.md](BENCHMARK.md) | Standardized 4-tier benchmark specification form (Macro Serving, System Resources, Kernel Network & Syscalls, Hardware PMU Counters) across five operational scenarios |
| [RUNBOOK.md](RUNBOOK.md) | Site Reliability Engineering (SRE) operational playbook, severity classification matrix, incident decision flowchart, diagnostic tooling, and Loki ingestion backpressure remediation |
| [TEST.md](TEST.md) | Automated unit, integration, and machine code test catalog, multi-protocol stream validation, and reproduction commands |

---

## 3. Strict 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Invariant**: Every configuration field must be explicitly declared in `plugins.json`. Velda Edge strictly rejects implicit fallbacks, magic defaults, or omitted properties. If any property is omitted or malformed, configuration validation fails fast at synchronization time.

| Field | Type | Required | Constraints / Allowed Values | Recommended Production Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `endpoint` | string | **Yes** | Valid URI starting with `http://` or `https://` | `"http://loki:3100/loki/api/v1/push"` | Target Grafana Loki ingestion endpoint URL. |
| `tenant_id` | string | **Yes** | Alphanumeric tenant identifier (use `""` if single-tenant) | `""` or `"velda-production"` | Multi-tenant organization identifier passed via `X-Scope-OrgID` HTTP header. |
| `service_name` | string | **Yes** | Non-empty string (`^[a-zA-Z0-9_\-\.]+$`) | `"velda-edge-prod"` | Logical service name attached to the `service` stream label. |
| `flush_interval_ms` | integer (u64) | **Yes** | `50 ..= 60000` (milliseconds) | `100` (100 milliseconds) | Periodic timer interval for waking up shadow worker to drain SPSC rings and ship buffered records. |
| `max_batch_size` | integer (usize) | **Yes** | `1 ..= 10000` (records) | `1000` | Maximum number of log records grouped into a single Loki HTTP POST push request. |
| `connect_timeout_ms`| integer (u64) | **Yes** | `100 ..= 60000` (milliseconds) | `1000` (1 second) | TCP and TLS connection establishment timeout before aborting and retrying. |
| `request_timeout_ms`| integer (u64) | **Yes** | `100 ..= 60000` (milliseconds) | `1000` (1 second) | Maximum round-trip time allowed for an HTTP push request to complete. |
| `labels` | object | **Yes** | Key-value map of string pairs (use `{}` if empty) | `{"env": "production"}` | Static stream labels attached to every stream descriptor for index filtering. |

---

## 4. Production Configuration Profiles

### Profile 1: Multi-Tenant Enterprise Profile (Loki Ingestion Port 3100)
*Recommended for enterprise multi-tenant logging clusters with tenant isolation.*

```json
{
  "id": "loki_log_shipper",
  "phase": "background",
  "config": {
    "endpoint": "http://loki.logging.svc.cluster.local:3100/loki/api/v1/push",
    "tenant_id": "velda-tenant-primary",
    "service_name": "velda-edge-production",
    "flush_interval_ms": 100,
    "max_batch_size": 1000,
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 1000,
    "labels": {
      "environment": "production",
      "cluster": "us-east-1a"
    }
  }
}
```

### Profile 2: Single-Tenant Minimal Profile
*Recommended for standard Kubernetes logging stacks without multi-tenancy headers.*

```json
{
  "id": "loki_log_shipper",
  "phase": "background",
  "config": {
    "endpoint": "http://loki:3100/loki/api/v1/push",
    "tenant_id": "",
    "service_name": "velda-edge",
    "flush_interval_ms": 100,
    "max_batch_size": 1000,
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 1000,
    "labels": {
      "env": "production"
    }
  }
}
```

---

## 5. Architectural Invariants

1. **Strict Cardinality Boundary**: Loki indexes only stream labels (`service`, `protocol`, and static `labels`). High-cardinality fields (`request_id`, `peer_ip`, `status_code`, `duration_us`) are serialized directly into log line text, preventing Loki index explosion.
2. **Pre-Baked Stream Descriptors (`LokiStreamTemplate`)**: Static stream headers for `http`, `grpc`, and `raw` protocol buckets are pre-serialized into byte arrays at startup.
3. **Division-Free 2-Digit ASCII LUT Formatting**: Spliced directly into memory using the 200-byte lookup table (`DIGIT_PAIRS`).
