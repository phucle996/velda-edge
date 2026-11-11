# Grafana Loki Log Shipper Plugin (`loki_log_shipper`)

The `loki_log_shipper` plugin is an ultra-fast, bounded-cardinality background logging exporter for the **Velda Edge Data Plane**. It subscribes to reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches across all application protocols, partitions records by protocol family (`protocol="http"`, `protocol="grpc"`, `protocol="raw"`), and ships them directly to Grafana Loki via **HTTP/1.1 JSON** (`/loki/api/v1/push` on port 3100) or **HTTP/2 gRPC Protobuf** (`/logproto.Pusher/Push` on port 9096).

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
| **Export Transports** | Dual-Transport: HTTP/1.1 JSON (`/loki/api/v1/push`) and HTTP/2 gRPC Protobuf (`/logproto.Pusher/Push`) |
| **Multi-Tenancy Support** | Native support for multi-tenant headers (`X-Scope-OrgID` HTTP / `x-scope-orgid` gRPC metadata) |
| **Request ID Precision** | Full 128-bit timestamp + entropy formatted into exactly 32 lowercase hex characters via SIMD |
| **IP Formatting** | RFC 5952 branchless zero-compression IPv6 on `[u8; 40]` stack buffer (0 heap allocations) |
| **Index Cardinality Control** | Strictly bounds label index cardinality by partitioning only by static labels and protocol families |

```text
╔══════════════════════════════════════════════════════════════════════════════════════════════════════╗
║                          VELDA EDGE — LOKI LOG SHIPPER ARCHITECTURE                                  ║
║              (crates/velda-plugin/src/background/loki_log_shipper: LokiLogShipperPlugin)             ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════════╝

 [FRONTLINE TRAFFIC WORKERS]                     [SHADOW LOGISTICS WORKER: velda-shadow-0]
 (Serving Hot Path, Priority 1)                 (Linux SCHED_IDLE, Core 0, 100ms Ticker)
  velda-worker-0 (Core 0)                        
  velda-worker-1 (Core 1)                        
        │                                                    │ (Every 100ms)
        ▼                                                    ▼
  ┌───────────┐  Zero-Lock SPSC Ring Drain  ┌──────────────────────────────────┐
  │ AccessLog │ ──────────────────────────► │  Drain Batches (max 1000 recs)   │
  │  Record   │    (< 2ns per record)       └────────────────┬─────────────────┘
  └───────────┘                                              │
                                              ┌──────────────┴──────────────┐
                                              │ Multi-Stream Partitioning   │
                                              │ (H1/H2/H3, Grpc, Raw L4)    │
                                              └──────────────┬──────────────┘
                                                             │
                                              ┌──────────────┴──────────────┐
                                              │                             │
                                        [mode: "grpc"]                [mode: "http"]
                                              ▼                             ▼
                                   ┌─────────────────────┐       ┌─────────────────────┐
                                   │  proto/template.rs  │       │   json/template.rs  │
                                   │ Prost PushRequest   │       │ Multi-Stream JSON   │
                                   │ 5-byte gRPC Framing │       │ SIMD Itoa + PSHUFB  │
                                   │ Scratch Buffer (.clear)     │ (439 CPU cycles)    │
                                   └──────────┬──────────┘       └──────────┬──────────┘
                                              │                             │
                                              ▼                             ▼
                                   ┌─────────────────────┐       ┌─────────────────────┐
                                   │   proto/client.rs   │       │    json/client.rs   │
                                   │ Persistent HTTP/2   │       │ Persistent HTTP/1.1 │
                                   │ X-Scope-OrgID gRPC  │       │ Keep-Alive (TCP/TLS)│
                                   └──────────┬──────────┘       └──────────┬──────────┘
                                              │                             │
                                              ▼                             ▼
                                  gRPC Port 9096 (Protobuf)      HTTP Port 3100 (JSON)
                                  ┌────────────────────────────────────────────────────┐
                                  │                 Grafana Loki                       │
                                  └────────────────────────────────────────────────────┘
```

---

## 2. Technical Documentation Index

| Document | Purpose & Scope |
| :--- | :--- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Architectural specification, dual-transport model, multi-stream partitioning, and zero-allocation scratch buffers |
| [MACHINE_CODE_ANALYSIS.md](MACHINE_CODE_ANALYSIS.md) | Assembly inspection, SIMD SSSE3 PSHUFB hex vectorization, division-free LUT serialization, and RDTSC benchmarks |
| [BENCHMARK.md](BENCHMARK.md) | Standardized 4-tier benchmark specification form (Macro Serving, System Resources, Kernel Network, PMU Counters) |
| [RUNBOOK.md](RUNBOOK.md) | Site Reliability Engineering (SRE) operational playbook, severity classification matrix, and diagnostic tooling |
| [TEST.md](TEST.md) | Automated unit, integration, and machine code test catalog, gRPC/HTTP end-to-end validation |

---

## 3. Strict 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Invariant**: Every configuration field must be explicitly declared in `plugins.json`. Velda Edge strictly rejects implicit fallbacks, magic defaults, or omitted properties. If any property is omitted or malformed, configuration validation fails fast at synchronization time.

| Field | Type | Required | Constraints / Allowed Values | Recommended Production Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `protocol` | string | **Yes** | `"http"` or `"grpc"` | `"http"` (or `"grpc"`) | Transport protocol family. |
| `endpoint` | string | **Yes** | Valid URI starting with `http://`, `https://`, `grpc://`, or `grpcs://` | `"http://loki:3100/loki/api/v1/push"` | Target Grafana Loki ingestion endpoint URL. |
| `tenant_id` | string | **Yes** | Alphanumeric tenant identifier (use `""` if single-tenant) | `""` or `"velda-production"` | Multi-tenant organization identifier (`X-Scope-OrgID`). |
| `service_name` | string | **Yes** | Non-empty string (`^[a-zA-Z0-9_\-\.]+$`) | `"velda-edge-prod"` | Logical service name attached to the `service` stream label. |
| `flush_interval_ms` | integer (u64) | **Yes** | `50 ..= 60000` (milliseconds) | `100` | Periodic timer interval for waking up shadow worker to drain SPSC rings. |
| `max_batch_size` | integer (usize) | **Yes** | `1 ..= 10000` (records) | `1000` | Maximum number of log records grouped into a single push payload. |
| `connect_timeout_ms`| integer (u64) | **Yes** | `100 ..= 60000` (milliseconds) | `1000` | TCP and TLS connection establishment timeout. |
| `request_timeout_ms`| integer (u64) | **Yes** | `100 ..= 60000` (milliseconds) | `1000` | Maximum round-trip time allowed for a push request. |
| `labels` | object | **Yes** | Key-value map of string pairs (use `{}` if empty) | `{"env": "production"}` | Static stream labels attached to every stream descriptor. |

---

## 4. Production Configuration Profiles

### Profile 1: High-Performance gRPC Protobuf Profile (Port 9096)
*Recommended for high-throughput production clusters using binary Protobuf framing.*

```json
{
  "id": "loki_log_shipper",
  "phase": "background",
  "config": {
    "protocol": "grpc",
    "endpoint": "grpc://loki.logging.svc.cluster.local:9096",
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

### Profile 2: Multi-Tenant HTTP/1.1 JSON Profile (Port 3100)
*Recommended for standard Grafana Loki HTTP ingestion endpoints.*

```json
{
  "id": "loki_log_shipper",
  "phase": "background",
  "config": {
    "protocol": "http",
    "endpoint": "http://loki:3100/loki/api/v1/push",
    "tenant_id": "velda-tenant-primary",
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

1. **Strict Cardinality Boundary**: Loki indexes only stream labels (`service`, `protocol`, and static `labels`). High-cardinality fields (`request_id`, `peer_ip`, `status_code`, `duration_us`) are serialized directly into log line text.
2. **Zero Heap Allocations on Flush**: `LokiJsonFormatter` preserves persistent scratch buffers across cycles. `LokiProtoFormatter` emits wire Protobuf directly into pre-allocated byte slices.
3. **128-Bit Request ID Precision**: 32 lowercase hex characters formatted via SIMD SSSE3 `PSHUFB` without string allocations.
4. **RFC 5952 Branchless IPv6**: Formatted on stack buffer `[u8; 40]` with zero-compression, eliminating `write!` overhead.
