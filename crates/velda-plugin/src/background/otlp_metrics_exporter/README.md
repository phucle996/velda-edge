# OpenTelemetry Metrics Exporter Plugin (`otlp_metrics_exporter`)

The `otlp_metrics_exporter` plugin is a high-performance, lock-free background service for the **Velda Edge Data Plane**. It periodically extracts point-in-time [`velda_observability::MetricsSnapshot`] states and pushes compliant OpenTelemetry `ResourceMetrics` payloads directly to an OpenTelemetry Collector via either **OTLP/gRPC** (binary Protobuf over HTTP/2) or **OTLP/HTTP** (JSON over HTTP/1.1).

---

## 1. Overview & Service Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `otlp_metrics_exporter` |
| **Canonical Phase** | `background` (Dedicated Shadow Logistics Worker) |
| **Execution Runtime** | Pinned to `velda-shadow-0` under Linux `SCHED_IDLE` scheduling priority |
| **Reverse Capability** | `PluginCapability::default()` (Non-request-serving background logistics service) |
| **Supported Ingress Protocols** | Ingests telemetry across HTTP/1.1, HTTP/2, HTTP/3, and gRPC downstream traffic |
| **Hot-Path Overhead** | **0 ns serving cost**; zero lock contention, zero allocations, and zero network I/O on traffic worker cores |
| **Export Transports** | OTLP/gRPC (Port 4317, binary Protobuf over HTTP/2) or OTLP/HTTP (Port 4318, JSON over HTTP/1.1) |

---

## 2. Technical Documentation Index

| Document | Purpose & Scope |
| :--- | :--- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Deep-dive architectural specification, Shadow Logistics threading model, In-Place Protobuf AST template (`OtlpProtoTemplate`), 2-digit LUT JSON serialization, and persistent connection lifecycle |
| [MACHINE_CODE_ANALYSIS.md](MACHINE_CODE_ANALYSIS.md) | Ground-truth machine code disassembly, reciprocal division-free arithmetic, 5-byte gRPC length-prefixed framing, live RDTSC cycle profiling, and A/B comparative evaluation under traffic |
| [BENCHMARK.md](BENCHMARK.md) | Standardized 4-tier benchmark specification form (Macro, System, Kernel Syscalls, HPC/PMU Hardware Counters) across 5 scenarios, Azure dedicated testbed empirical data, and comparative matrix vs Envoy |
| [RUNBOOK.md](RUNBOOK.md) | SRE on-call emergency operational playbook, P0–P3 triage matrix, Mermaid incident decision flowchart, field diagnostic CLI toolkit, and collector blackhole remediation |
| [TEST.md](TEST.md) | Automated unit & integration test catalog, OpenTelemetry JSON & Protobuf schema conformance checks, and reproduction instructions |

---

## 3. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration block and parameter must be explicitly declared in `plugins.json`. Velda Edge strictly rejects implicit fallbacks, magic defaults, or omitted properties. If any property is omitted or malformed, configuration validation fails fast at sync time.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `protocol` | string | **Yes** | `"grpc"` or `"http"` | `"grpc"` (Production)<br>`"http"` (Debug) | Transport protocol. `"grpc"` exports binary Protobuf via HTTP/2 (port 4317); `"http"` exports JSON via HTTP/1.1 (port 4318). |
| `endpoint` | string | **Yes** | Valid URI starting with `grpc://`, `grpcs://`, `http://`, or `https://` | `"grpc://127.0.0.1:4317"` | Target OpenTelemetry Collector ingestion endpoint. |
| `interval_secs` | integer (u64) | **Yes** | `1 ..= 3600` (seconds) | `10` (Production)<br>`1` (Staging/Dev) | Telemetry push interval for capturing lock-free snapshots from `MetricsStore`. |
| `connect_timeout_ms` | integer (u64) | **Yes** | `100 ..= 60000` (milliseconds) | `5000` (5s) | TCP and TLS connection establishment timeout before aborting and retrying. |
| `request_timeout_ms` | integer (u64) | **Yes** | `100 ..= 60000` (milliseconds) | `10000` (10s) | Maximum round-trip timeout allowed for an export request to complete. |
| `service_name` | string | **Yes** | Non-empty string (`^[a-zA-Z0-9_\-\.]+$`) | `"velda-edge-prod"` | Logical service identifier reported in OpenTelemetry Resource attributes (`service.name`). |
| `headers` | object | **Yes** | Key-value map of string pairs (use `{}` if none) | `{}` or Auth headers | Custom HTTP or gRPC headers sent with every export request (e.g., Bearer tokens, tenant IDs). |

---

## 4. Production Configuration Profiles

### Profile 1: High-Throughput Production Profile (OTLP / gRPC)
*Recommended for production clusters to minimize CPU usage, wire overhead, and memory churn.*

```json
{
  "id": "otlp_metrics_exporter",
  "config": {
    "protocol": "grpc",
    "endpoint": "grpc://otel-collector.monitoring.svc.cluster.local:4317",
    "interval_secs": 10,
    "connect_timeout_ms": 5000,
    "request_timeout_ms": 10000,
    "service_name": "velda-edge-production",
    "headers": {
      "authorization": "Bearer sec-tok-98471982734",
      "x-tenant-id": "production-core"
    }
  }
}
```

### Profile 2: Standard REST / JSON Profile (OTLP / HTTP)
*Recommended for local development, debugging, or REST-only metric collectors.*

```json
{
  "id": "otlp_metrics_exporter",
  "config": {
    "protocol": "http",
    "endpoint": "http://127.0.0.1:4318/v1/metrics",
    "interval_secs": 5,
    "connect_timeout_ms": 5000,
    "request_timeout_ms": 10000,
    "service_name": "velda-edge-dev",
    "headers": {}
  }
}
```

---

## 5. Setup & Verification Guide

### Step 1: Declare Plugin in `plugins.json`
Include the plugin block in `plugins.json` within the storage directory:

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "otlp_metrics_exporter",
      "config": {
        "protocol": "grpc",
        "endpoint": "grpc://127.0.0.1:4317",
        "interval_secs": 10,
        "connect_timeout_ms": 5000,
        "request_timeout_ms": 10000,
        "service_name": "velda-edge-cluster-01",
        "headers": {}
      }
    }
  ]
}
```

### Step 2: Run Velda Edge Gateway
```bash
cargo run -p velda-edge
```

### Step 3: Verify Background Worker Startup
Inspect the gateway runtime logs to confirm the shadow supervisory task has launched:

```bash
journalctl -u velda-edge -f -o cat | grep "otlp_metrics"
```

Expected output:
```text
[INFO] Background plugin starting: plugin_id=otlp_metrics_exporter
[INFO] Initialized OTLP gRPC client for endpoint: grpc://127.0.0.1:4317
[INFO] Flushed OTLP metrics batch via gRPC successfully (288 data points, 4310 bytes)
```

### Step 4: Verify Socket Connection State
Check that the persistent TCP/TLS connection to the OpenTelemetry Collector is established:

```bash
# For gRPC (port 4317) or HTTP (port 4318)
ss -tinh 'dport = :4317 or dport = :4318'
```

Expected output:
```text
ESTAB  0  0  127.0.0.1:54210  127.0.0.1:4317  timer:(keepalive,...)
```

### Step 4: Verify Collector Ingestion
Inspect the OpenTelemetry Collector container or daemon logs to verify successful receipt:

```bash
docker logs otel-collector | grep "ResourceMetrics"
```
