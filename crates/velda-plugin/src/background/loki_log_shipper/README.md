# Grafana Loki Log Shipper Plugin (`loki_log_shipper`)

Background telemetry exporter for Velda Edge. Subscribes to reference-counted 64-byte [`velda_observability::TelemetryRecord`] slices across all application protocols (HTTP/1.1, HTTP/2, HTTP/3, gRPC, Raw L4 TCP/UDP), partitions records into protocol family streams (`protocol="http"`, `protocol="grpc"`, `protocol="raw"`), and ships them via pooled [`velda_etp::loki::LokiEtp`] leases over **HTTP/2 gRPC Protobuf** (`/logproto.Pusher/Push`) or **HTTP/1.1 JSON** (`/loki/api/v1/push`) strictly from isolated Shadow Logistics Workers (`velda-shadow-{i}`).

---

## 1. Documentation Index

| Document | Purpose | Primary Audience |
| :--- | :--- | :--- |
| **[README.md](README.md)** | Subsystem overview, configuration contract, production profiles, quickstart verification | All Engineers, Operators |
| **[RUNBOOK.md](RUNBOOK.md)** | SRE On-Call Incident Triage Matrix (P0–P3), diagnostic toolkit, emergency playbooks | On-Call SREs, Systems Engineers |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | Deep micro-architectural specification, memory geometry, multi-stream partitioning, SIMD PSHUFB hex stamping, physical boundaries | Systems Architects, Performance Engineers |
| **[BENCHMARK.md](BENCHMARK.md)** | Benchmark specification | Performance Engineers, SRE Leads |

---

## 2. Overview & Core Invariants

* **Shadow Worker Execution**: Runs strictly on background companion threads (`velda-shadow-{id}`) under Linux `SCHED_IDLE`. Frontline traffic workers never format log lines, serialize JSON/Protobuf, or perform outbound logging I/O.
* **Lock-Free SPSC Ring Drain**: Frontline workers push 64-byte scalar `TelemetryRecord` entries into core-local SPSC ring buffers drained asynchronously by [`LokiLogShipperPlugin::run`].
* **Strict Bounded Label Cardinality**: Stream labels are restricted to static metadata (`service`, user `labels`) and protocol family (`protocol="http"`, `protocol="grpc"`, `protocol="raw"`). High-cardinality fields (`request_id`, client IP, status) stay inside the log line.
* **Pooled ETP Sessions (`velda-etp`)**: All endpoints, transport protocol (`http` vs `grpc`), `tenant_id` (`X-Scope-OrgID`), authentication (`bearer_token`, `basic_auth`), custom headers, TLS/mTLS + ALPN (`h2` for gRPC), and `(N + 1) * 4096B` `EtpPageArena<LokiConn>` slot pooling live in the referenced `LokiEtp` entry in `etp.json`.
* **30-Second Cool-Down Shield**: After more than 10 consecutive failed pushes, incoming telemetry batches are drained and discarded for 30 seconds to bound gateway memory usage.

```text
╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
║                          VELDA EDGE — LOKI LOG SHIPPER PIPELINE                                  ║
║                    (crates/velda-plugin: loki_log_shipper::plugin)                               ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════╝

 [FRONTLINE TRAFFIC WORKERS (Cores 0..N-1)]     [SHADOW LOGISTICS WORKER (Core i)]
  velda-worker-0..N-1                             LokiLogShipperPlugin (SCHED_IDLE)
        │                                                    │ (flush_interval_ms)
        ▼                                                    ▼
  ┌───────────┐  Zero-Lock SPSC Ring Drain   ┌──────────────────────────────────┐
  │ Telemetry │ ──────────────────────────► │ Drain Batches & Clear Buffers    │
  │  Record   │                              └────────────────┬─────────────────┘
  └───────────┘                                               │
                                                ┌─────────────┴─────────────┐
                                                │ Format via etp.protocol   │
                                                └─────────────┬─────────────┘
                                                              │
                                                ┌─────────────┴─────────────┐
                                                │                           │
                                          [LokiProtocol::Grpc]        [LokiProtocol::Http]
                                                ▼                           ▼
                                     ┌─────────────────────┐     ┌─────────────────────┐
                                     │  proto/template.rs  │     │   json/template.rs  │
                                     │ Direct Wire Protobuf│     │ Pre-Baked Envelope  │
                                     └──────────┬──────────┘     └──────────┬──────────┘
                                                │                           │
                                                └─────────────┬─────────────┘
                                                              ▼
                                                ┌─────────────────────────────┐
                                                │ wire.rs (LokiEtp::acquire)  │
                                                │  - LokiConn::Grpc (h2)      │
                                                │  - LokiConn::Http (HTTP/1.1)│
                                                │  - In-place lease reconnect │
                                                └─────────────┬───────────────┘
                                                              ▼
                                                  Grafana Loki (:9096 / :3100)
```

---

## 3. Configuration Reference (`etp.json` + `plugins.json`)

1. Declare the Loki ETP pool in `etp.json` (`kind: "loki"`):

```json
{
  "id": "loki-primary",
  "kind": "loki",
  "pool": {
    "pool_size": 4,
    "connect_timeout_ms": 2000,
    "idle_timeout_ms": 60000
  },
  "settings": {
    "protocol": "grpc",
    "endpoint": "grpc://loki.logging.svc.cluster.local:9096",
    "tenant_id": "velda-production",
    "headers": {
      "x-cluster-region": "us-east-1"
    }
  }
}
```

2. Reference that `etp_id` in `plugins.json`:

| Field | Type | Default | Valid Range / Constraints | Description |
| :--- | :---: | :---: | :--- | :--- |
| `etp_id` | `String` | **Required** | Non-empty string | Identifier of a `kind: "loki"` entry in `etp.json` (`EtpRegistry`). |
| `service_name` | `String` | `"velda-edge"` | Non-empty trimmed string | Value for the `service` stream label. |
| `flush_interval_ms` | `Integer (u64)` | `1000` | `100 ..= 60000` | Periodic flush ticker interval (clamped at $\ge 50\text{ms}$ runtime minimum). |
| `max_batch_size` | `Integer (usize)` | `1000` | `1 ..= 10000` | Validated batch limit configuration. |
| `request_timeout_ms` | `Integer (u64)` | `5000` | `100 ..= 60000` | HTTP/1.1 or HTTP/2 gRPC request completion timeout. |
| `labels` | `Object` | `{}` | Keys match `[a-zA-Z_][a-zA-Z0-9_]*`; non-empty values | Additional static Prometheus stream labels. |

---

## 4. Production Profiles

### Profile 1: HTTP/2 gRPC Protobuf (`loki_log_shipper` + `etp.json`)

```json
{
  "id": "loki_log_shipper",
  "config": {
    "etp_id": "loki-primary",
    "service_name": "velda-edge-production",
    "flush_interval_ms": 100,
    "max_batch_size": 1000,
    "request_timeout_ms": 2000,
    "labels": {
      "env": "production",
      "cluster": "us-east-1"
    }
  }
}
```

### Profile 2: Minimal HTTP/1.1 JSON Configuration

```json
{
  "id": "loki_log_shipper",
  "config": {
    "etp_id": "loki-http"
  }
}
```
