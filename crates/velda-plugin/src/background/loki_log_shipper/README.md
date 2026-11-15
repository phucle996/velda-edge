# Grafana Loki Log Shipper Plugin (`loki_log_shipper`)

High-performance, zero-allocation background telemetry exporter for Velda Edge. Subscribes to reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches across all application protocols (HTTP/1.1, HTTP/2, HTTP/3, gRPC, Raw L4 TCP/UDP), partitions records into protocol family streams (`protocol="http"`, `protocol="grpc"`, `protocol="raw"`), and ships them directly to Grafana Loki via **HTTP/2 gRPC Protobuf** (`/logproto.Pusher/Push` on port 9096) or **HTTP/1.1 JSON** (`/loki/api/v1/push` on port 3100) strictly from isolated Shadow Logistics Workers.

---

## 1. Documentation Index

| Document | Purpose | Primary Audience |
| :--- | :--- | :--- |
| **[README.md](README.md)** | Subsystem overview, configuration contract, production profiles, quickstart verification | All Engineers, Operators |
| **[RUNBOOK.md](RUNBOOK.md)** | SRE On-Call Incident Triage Matrix (P0–P3), Mermaid decision flowchart, CLI diagnostic toolkit, emergency playbooks | On-Call SREs, Systems Engineers |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | Deep micro-architectural specification, memory geometry, multi-stream partitioning, SIMD PSHUFB hex stamping, physical boundaries | Systems Architects, Performance Engineers |
| **[BENCHMARK.md](BENCHMARK.md)** | First-principles 5-tier zero-bloat benchmark specification adhering to `/velda-benchmark` with 6-scenario comparative matrix | Performance Engineers, SRE Leads |

---

## 2. Overview & Core Invariants

The `loki_log_shipper` plugin (`phase: "background"`) operates as an autonomous background companion service designed to format, compress, and export access logs without competing with frontline traffic workers:

* **Shadow Worker Execution (`phase: "background"`)**: Operates exclusively on background companion threads (`velda-shadow-{id}`) under Linux `SCHED_IDLE`. Traffic workers serving client HTTP requests never format strings, serialize JSON, or execute outbound network I/O calls.
* **Lock-Free SPSC Ring Drain**: Frontline traffic workers push 64-byte scalar `AccessLogRecord` structs into core-local Single-Producer Single-Consumer (SPSC) ring buffers without mutexes or lock contention.
* **Strict Bounded Label Cardinality**: Stream labels are strictly restricted to low-cardinality static metadata and protocol families (`protocol="http"`, `protocol="grpc"`, `protocol="raw"`). High-cardinality values (`request_id`, `ip_address`, `status`, `path`) are serialized inside the log entry line, preventing Loki ingester index explosion.
* **Zero-Allocation Multi-Stream Serialization**: Reuses persistent scratch vectors across flush cycles via `.clear()`, eliminating memory reallocations during Protobuf encoding or JSON formatting.
* **SIMD Hex Stamping & Division-Free LUT**: 128-bit Request IDs are converted into 32 lowercase hex characters via SSSE3 `PSHUFB` vector instructions. Decimal integers and timestamps are formatted using the 200-byte 2-digit lookup table (`DIGIT_PAIRS`).
* **Dual-Transport Flexibility**: Direct native support for persistent HTTP/2 gRPC (`/logproto.Pusher/Push` on port 9096) and persistent HTTP/1.1 Keep-Alive JSON (`/loki/api/v1/push` on port 3100) with multi-tenant `X-Scope-OrgID` routing.
* **Fail-Open Autonomy**: If Grafana Loki returns HTTP 429 rate limit errors or gRPC `RESOURCE_EXHAUSTED`, the shipper logs warnings and backs off without blocking or stalling downstream client request serving.

```text
╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
║                          VELDA EDGE — LOKI LOG SHIPPER PIPELINE                                  ║
║                    (crates/velda-plugin: loki_log_shipper::plugin)                               ║
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
  │ AccessLog │ ──────────────────────────► │  Drain Batches (max_batch_size)  │
  │  Record   │    (O(1) Ring Dequeue)       └────────────────┬─────────────────┘
  └───────────┘                                               │
                                                ┌─────────────┴─────────────┐
                                                │ Multi-Stream Partitioning │
                                                │ (HTTP, gRPC, Raw L4)      │
                                                └─────────────┬─────────────┘
                                                              │
                                                ┌─────────────┴─────────────┐
                                                │                           │
                                          [mode: "grpc"]              [mode: "http"]
                                                ▼                           ▼
                                     ┌─────────────────────┐     ┌─────────────────────┐
                                     │  proto/template.rs  │     │   json/template.rs  │
                                     │ Direct Wire Protobuf│     │ Pre-Baked Envelope  │
                                     │ 5-byte gRPC Framing │     │ SIMD Itoa + PSHUFB  │
                                     │ Scratch Buffer Reuse│     │ Persistent Scratch  │
                                     └──────────┬──────────┘     └──────────┬──────────┘
                                                │                           │
                                                ▼                           ▼
                                     ┌─────────────────────┐     ┌─────────────────────┐
                                     │   proto/client.rs   │     │    json/client.rs   │
                                     │ Persistent HTTP/2   │     │ Persistent HTTP/1.1 │
                                     │ X-Scope-OrgID gRPC  │     │ Keep-Alive (TCP/TLS)│
                                     └──────────┬──────────┘     └──────────┬──────────┘
                                                │                           │
                                                ▼                           ▼
                                    gRPC Port 9096 (Protobuf)    HTTP Port 3100 (JSON)
                                    ┌──────────────────────────────────────────────────┐
                                    │                 Grafana Loki                     │
                                    └──────────────────────────────────────────────────┘
```

---

## 3. Shadow Workload Weight & Core Affinity

Under the Velda Edge Workload Distribution Model, background logistics plugins are assigned a workload weight from 1 to 100 based on CPU intensity, memory footprint, cadence/wakeup rate, and failure blast radius:

| Physical Dimension | Metric Profile | Impact Analysis |
| :--- | :--- | :--- |
| **Assigned Workload Weight** | **80** | **Highest Background Workload** |
| **CPU Intensity** | High | SPSC ring batch drain, multi-stream log partitioning, SIMD hex stamping (`PSHUFB`), Snappy / Gzip compression, HTTP/2 binary framing or JSON text interleaving, and TLS encryption. Under heavy traffic (e.g. 50K RPS), this produces sustained CPU activity on the shadow core. |
| **Memory Footprint** | ~1 MB – 3 MB | Multi-stream value buffers (`http_values`, `grpc_values`, `raw_values`), Snappy compression buffers, and HTTP/2 transport connection state. Zero ongoing heap churn. |
| **Cadence / Wakeup Rate** | High (50ms – 1000ms) | Sub-second flush interval (default: 100ms) generates frequent scheduler activity to maintain low log delivery latency. |
| **Failure Blast Radius** | P3 (Low Impact) | Access log loss only. Loki rate limiting (HTTP 429), connection timeouts, or rejected payloads do not impact frontline client request serving. |

### Complementary Static Sharding Recommendation
* **Target Affinity**: Core 3 (or a dedicated secondary core).
* **Strict Anti-Colocation Rule**: NEVER co-locate on Core 0 with latency-critical `health_probe` (Weight: 1) or master traffic intake. NEVER co-locate on the same core as `otlp_log_shipper` (Weight: 70) — both are heavy log shippers that will exhaust core L1/L2 cache capacity and TLS cryptographic hardware units.
* **Ideal Pairing**: Pair with sleep-bound rotators (`jwks_rotator`, `acme_cert_rotator`) or lightweight UDP exporters (`statsd_exporter`) on secondary cores.

---

## 4. 100% Explicit Configuration Contract

> [!IMPORTANT]
> **Strict Configuration Invariant**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. Execution phase is automatically inferred as `background` via `canonical_phase_for_id` based on `id: "loki_log_shipper"`.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `protocol` | String | **Yes** | `"http"` or `"grpc"` | `"grpc"` | Transport protocol: `"grpc"` streams Protobuf over HTTP/2 (port 9096); `"http"` streams JSON over HTTP/1.1 (port 3100). |
| `endpoint` | String | **Yes** | Valid URI (`http://`, `https://`, `grpc://`, `grpcs://`) | `"grpc://127.0.0.1:9096"` | Ingestion endpoint URL of the target Grafana Loki instance. |
| `tenant_id` | String | **Yes** | Alphanumeric string (`""` if single-tenant) | `""` or `"velda-production"` | Multi-tenant organization identifier (`X-Scope-OrgID`). |
| `service_name` | String | **Yes** | Non-empty ASCII string (`^[a-zA-Z0-9_\-\.]+$`) | `"velda-edge-prod"` | Logical service identifier attached to the `service` stream label. |
| `flush_interval_ms` | Integer (u64) | **Yes** | `50 ..= 60000` | `100` | Periodic timer interval for waking up shadow worker to drain SPSC rings and ship buffered records. |
| `max_batch_size` | Integer (usize) | **Yes** | `1 ..= 10000` | `1000` | Maximum number of log records grouped into a single push payload. |
| `connect_timeout_ms`| Integer (u64) | **Yes** | `100 ..= 60000` | `1000` | TCP and TLS connection establishment timeout before aborting and retrying. |
| `request_timeout_ms`| Integer (u64) | **Yes** | `100 ..= 60000` | `1000` | Maximum round-trip time allowed for a push request to complete. |
| `labels` | Object | **Yes** | Key-value map of string pairs (`{}` if empty) | `{"env": "prod"}` | Static stream labels attached to every stream descriptor. |

---

## 5. Production Configuration Profiles

### Profile 1: High-Performance gRPC Protobuf Profile (Default Port 9096)
*Recommended for production clusters to minimize CPU usage, wire overhead, and memory churn.*

```json
{
  "id": "loki_log_shipper",
  "config": {
    "protocol": "grpc",
    "endpoint": "grpc://loki.logging.svc.cluster.local:9096",
    "tenant_id": "velda-production",
    "service_name": "velda-edge-production",
    "flush_interval_ms": 100,
    "max_batch_size": 1000,
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 1000,
    "labels": {
      "env": "production",
      "cluster": "us-east-1"
    }
  }
}
```

### Profile 2: Standard REST / JSON Profile (Default Port 3100)
*Recommended for environments utilizing standard HTTP/JSON-based Grafana Loki ingestion pipelines.*

```json
{
  "id": "loki_log_shipper",
  "config": {
    "protocol": "http",
    "endpoint": "http://loki.logging.svc.cluster.local:3100/loki/api/v1/push",
    "tenant_id": "velda-production",
    "service_name": "velda-edge-production",
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

### Profile 3: Secure Cross-Datacenter WAN Profile (gRPC + TLS)
*Recommended for cross-datacenter or cloud ingestion endpoints requiring transport security.*

```json
{
  "id": "loki_log_shipper",
  "config": {
    "protocol": "grpc",
    "endpoint": "grpcs://loki.monitoring.aws.internal:9096",
    "tenant_id": "velda-prod-wan",
    "service_name": "velda-edge-wan",
    "flush_interval_ms": 200,
    "max_batch_size": 2000,
    "connect_timeout_ms": 2000,
    "request_timeout_ms": 2000,
    "labels": {
      "region": "eu-west-1"
    }
  }
}
```

---

## 6. Verification & Quickstart

### Step 1: Start Grafana Loki
Ensure Grafana Loki is running and listening on port 9096 (gRPC) or port 3100 (HTTP).

### Step 2: Declare in `plugins.json`
Include the plugin block in `plugins.json`:

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "loki_log_shipper",
      "config": {
        "protocol": "grpc",
        "endpoint": "grpc://127.0.0.1:9096",
        "tenant_id": "velda-test",
        "service_name": "velda-edge",
        "flush_interval_ms": 100,
        "max_batch_size": 1000,
        "connect_timeout_ms": 1000,
        "request_timeout_ms": 1000,
        "labels": {
          "env": "dev"
        }
      }
    }
  ]
}
```

### Step 3: Run Velda Edge Gateway
```bash
rtk cargo run -p velda-edge
```

### Step 4: Verify Stream Ingestion in Grafana
Generate downstream traffic and query Grafana Loki via LogQL:
```text
{service="velda-edge", protocol="http"} |= "status=200"
```
Logs appear in real-time with full 128-bit trace and request IDs.
