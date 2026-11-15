# OpenTelemetry Log Shipper Plugin (`otlp_log_shipper`)

High-performance, zero-allocation background telemetry exporter for Velda Edge. Subscribes to reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches across all application protocols (HTTP/1.1, HTTP/2, HTTP/3, gRPC, Raw L4 TCP/UDP) and exports standardized OpenTelemetry `ResourceLogs` directly to OpenTelemetry Collectors via **OTLP/gRPC** (binary Protocol Buffers over persistent HTTP/2) or **OTLP/HTTP** (JSON over persistent HTTP/1.1 Keep-Alive) strictly from isolated Shadow Logistics Workers.

---

## 1. Documentation Index

| Document | Purpose | Primary Audience |
| :--- | :--- | :--- |
| **[README.md](README.md)** | Subsystem overview, configuration contract, production profiles, quickstart verification | All Engineers, Operators |
| **[RUNBOOK.md](RUNBOOK.md)** | SRE On-Call Incident Triage Matrix (P0–P3), Mermaid decision flowchart, CLI diagnostic toolkit, emergency playbooks | On-Call SREs, Systems Engineers |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | Deep micro-architectural specification, memory geometry, Protobuf wire skeletons, SSSE3 SIMD PSHUFB hex stamping, physical boundaries | Systems Architects, Performance Engineers |
| **[BENCHMARK.md](BENCHMARK.md)** | First-principles 5-tier zero-bloat benchmark specification adhering to `/velda-benchmark` with 6-scenario comparative matrix | Performance Engineers, SRE Leads |

---

## 2. Overview & Core Invariants

The `otlp_log_shipper` plugin (`phase: "background"`) operates as an autonomous background companion service designed to format and export structured access logs without competing with frontline traffic workers:

* **Shadow Worker Execution (`phase: "background"`)**: Operates exclusively on background companion threads (`velda-shadow-{id}`) under Linux `SCHED_IDLE`. Traffic workers serving client HTTP requests never format strings, serialize JSON, or execute outbound network I/O.
* **Lock-Free SPSC Ring Drain**: Frontline traffic workers push 64-byte scalar `AccessLogRecord` structs into core-local Single-Producer Single-Consumer (SPSC) ring buffers without mutexes or lock contention.
* **Pre-Baked Wire Skeletons (Protobuf)**: In gRPC mode, static Resource, Scope, and log record header framing bytes are pre-computed into static byte skeletons. Record attributes and timestamps are stamped directly into memory via raw pointer arithmetic, eliminating Prost runtime AST heap allocations.
* **Pre-Baked Chunk Interleaving & SIMD Itoa (JSON)**: In HTTP mode, static JSON fragments, a 200-byte 2-digit ASCII lookup table (`DIGIT_PAIRS`), and SSSE3 `PSHUFB` vector instructions convert 128-bit trace and request IDs into 32 hex characters in parallel without scalar branching.
* **Persistent Transport Reuse**: Maintains persistent HTTP/2 connections for gRPC (Port 4317) or Keep-Alive HTTP/1.1 connections for JSON (Port 4318), avoiding per-flush TCP and TLS handshake overhead.
* **Fail-Open Autonomy**: If the downstream OpenTelemetry Collector hangs, crashes, or rejects payloads, the shipper logs warnings and backs off without blocking or stalling downstream client request serving.

```text
╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
║                          VELDA EDGE — OTLP LOG SHIPPER PIPELINE                                  ║
║                    (crates/velda-plugin: otlp_log_shipper::plugin)                               ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════╝

 [FRONTLINE TRAFFIC WORKERS (Cores 0..N-1)]     [SHADOW LOGISTICS WORKER (Core i)]
 (Serving Hot Path, Priority 1)                 (Linux SCHED_IDLE, Assigned Core i)
  velda-worker-0 (Core 0)                        
  velda-worker-1 (Core 1)                        
  ...                                            
  velda-worker-n (Core N-1)                      
        │                                                    │ (Periodic Ticker)
        ▼                                                    ▼
  ┌───────────┐  Zero-Lock SPSC Ring Drain   ┌──────────────────────────────────┐
  │ AccessLog │ ───────────────────────────► │  Drain Batches (max_batch_size)  │
  │  Record   │    (O(1) Ring Dequeue)       └────────────────┬─────────────────┘
  └───────────┘                                               │
                                                ┌─────────────┴─────────────┐
                                                │                           │
                                          [mode: "grpc"]              [mode: "http"]
                                                ▼                           ▼
                                     ┌─────────────────────┐     ┌─────────────────────┐
                                     │  proto/template.rs  │     │   json/template.rs  │
                                     │ Wire Skeletons (PB) │     │ Pre-Baked Envelope  │
                                     │ Raw Pointer Stamp   │     │ SIMD Itoa + PSHUFB  │
                                     │ (Zero AST Churn)    │     │ (LUT-accelerated)   │
                                     └──────────┬──────────┘     └──────────┬──────────┘
                                                │                           │
                                                ▼                           ▼
                                     ┌─────────────────────┐     ┌─────────────────────┐
                                     │   proto/client.rs   │     │    json/client.rs   │
                                     │ Persistent HTTP/2   │     │ Persistent HTTP/1.1 │
                                     │ 5-byte Framing      │     │ Keep-Alive (TCP/TLS)│
                                     └──────────┬──────────┘     └──────────┬──────────┘
                                                │                           │
                                                ▼                           ▼
                                    gRPC Port 4317 (Protobuf)    HTTP Port 4318 (JSON)
                                    ┌──────────────────────────────────────────────────┐
                                    │             OpenTelemetry Collector              │
                                    └──────────────────────────────────────────────────┘
```

---

## 3. Shadow Workload Weight & Core Affinity

Under the Velda Edge Workload Distribution Model, background logistics plugins are assigned a workload weight from 1 to 100 based on CPU intensity, memory footprint, cadence/wakeup rate, and failure blast radius:

| Physical Dimension | Metric Profile | Impact Analysis |
| :--- | :--- | :--- |
| **Assigned Workload Weight** | **70** | **High Background Workload** |
| **CPU Intensity** | High | SPSC ring batch drain, per-record Protobuf serialization, SSSE3 SIMD hex stamping (`PSHUFB`), HTTP/2 binary framing or JSON text interleaving, and TLS encryption. Under heavy traffic (e.g. 50K RPS), this produces sustained CPU activity on the shadow core. |
| **Memory Footprint** | ~512 KB – 2 MB | Pre-allocated Protobuf wire arena, JSON scratch formatting vectors, and HTTP/2 transport connection state. Zero ongoing heap churn. |
| **Cadence / Wakeup Rate** | High (50ms – 1000ms) | Sub-second flush interval (default: 100ms) generates frequent scheduler activity to maintain low log delivery latency. |
| **Failure Blast Radius** | P3 (Low Impact) | Access log loss only. Collector downtime, connection timeouts, or rejected payloads do not impact frontline client request serving. |

### Complementary Static Sharding Recommendation
* **Target Affinity**: Core 2 or 3.
* **Strict Anti-Colocation Rule**: NEVER co-locate on Core 0 with latency-critical `health_probe` (Weight: 1) or master traffic intake. NEVER co-locate on the same core as `loki_log_shipper` (Weight: 80) — both are heavy log shippers that will exhaust core L1/L2 cache capacity and TLS cryptographic hardware units.
* **Ideal Pairing**: Pair with sleep-bound rotators (`jwks_rotator`, `acme_cert_rotator`) or lightweight UDP exporters (`statsd_exporter`) on secondary cores.

---

## 4. 100% Explicit Configuration Contract

> [!IMPORTANT]
> **Strict Configuration Invariant**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. Execution phase is automatically inferred as `background` via `canonical_phase_for_id` based on `id: "otlp_log_shipper"`.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `protocol` | String | **Yes** | `"http"` or `"grpc"` | `"grpc"` | Transport protocol: `"grpc"` streams Protobuf over HTTP/2 (port 4317); `"http"` streams JSON over HTTP/1.1 (port 4318). |
| `endpoint` | String | **Yes** | Valid URI (`http://`, `https://`, `grpc://`, `grpcs://`) | `"grpc://127.0.0.1:4317"` | Target OpenTelemetry Collector ingestion endpoint. |
| `service_name` | String | **Yes** | Non-empty ASCII string (`^[a-zA-Z0-9_\-\.]+$`) | `"velda-edge-prod"` | Logical service identifier reported in OpenTelemetry Resource attributes (`service.name`). |
| `flush_interval_ms` | Integer (u64) | **Yes** | `100 ..= 60000` | `100` | Periodic timer interval for waking up shadow worker to drain SPSC rings and ship buffered records. |
| `max_batch_size` | Integer (usize) | **Yes** | `1 ..= 10000` | `1000` | Maximum number of log records grouped into a single exported payload. |
| `connect_timeout_ms`| Integer (u64) | **Yes** | `100 ..= 60000` | `1000` | TCP and TLS connection establishment timeout before aborting and retrying. |
| `request_timeout_ms`| Integer (u64) | **Yes** | `100 ..= 60000` | `1000` | Maximum round-trip time allowed for an HTTP/gRPC export request to complete. |
| `headers` | Object | **Yes** | Key-value map of string pairs (`{}` if empty) | `{}` | Custom headers sent with every export request (e.g. Bearer auth, tenant IDs). |

---

## 5. Production Configuration Profiles

### Profile 1: High-Throughput Production Profile (OTLP / gRPC Protobuf)
*Recommended for production clusters to minimize wire bandwidth and CPU usage via binary Protobuf framing.*

```json
{
  "id": "otlp_log_shipper",
  "config": {
    "protocol": "grpc",
    "endpoint": "grpc://otel-collector.monitoring.svc.cluster.local:4317",
    "service_name": "velda-edge-production",
    "flush_interval_ms": 100,
    "max_batch_size": 1000,
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 1000,
    "headers": {
      "authorization": "Bearer sec-tok-98471982734"
    }
  }
}
```

### Profile 2: Standard REST / JSON Profile (OTLP / HTTP Keep-Alive)
*Recommended for environments utilizing HTTP/JSON-based OpenTelemetry log aggregation pipelines.*

```json
{
  "id": "otlp_log_shipper",
  "config": {
    "protocol": "http",
    "endpoint": "http://otel-collector.monitoring.svc.cluster.local:4318/v1/logs",
    "service_name": "velda-edge-production",
    "flush_interval_ms": 100,
    "max_batch_size": 1000,
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 1000,
    "headers": {}
  }
}
```

### Profile 3: Secure Cross-Datacenter WAN Profile (gRPC + TLS)
*Recommended for cross-datacenter or cloud ingestion endpoints requiring transport security.*

```json
{
  "id": "otlp_log_shipper",
  "config": {
    "protocol": "grpc",
    "endpoint": "grpcs://otel.eu-west-1.aws.internal:4317",
    "service_name": "velda-edge-wan",
    "flush_interval_ms": 200,
    "max_batch_size": 2000,
    "connect_timeout_ms": 2000,
    "request_timeout_ms": 2000,
    "headers": {
      "x-api-key": "secret-collector-key"
    }
  }
}
```

---

## 6. Verification & Quickstart

### Step 1: Start OpenTelemetry Collector
Ensure an OpenTelemetry Collector instance is running and listening on port 4317 (gRPC) or port 4318 (HTTP).

### Step 2: Declare in `plugins.json`
Include the plugin block in `plugins.json`:

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "otlp_log_shipper",
      "config": {
        "protocol": "grpc",
        "endpoint": "grpc://127.0.0.1:4317",
        "service_name": "velda-edge-prod",
        "flush_interval_ms": 100,
        "max_batch_size": 1000,
        "connect_timeout_ms": 1000,
        "request_timeout_ms": 1000,
        "headers": {}
      }
    }
  ]
}
```

### Step 3: Run Velda Edge Gateway
```bash
rtk cargo run -p velda-edge
```

### Step 4: Verify Telemetry Stream
Generate downstream traffic and verify log batches reach the OpenTelemetry Collector:
```text
DEBUG otlp_log_shipper: Flushed OTLP log batch via gRPC successfully records=1000
```
