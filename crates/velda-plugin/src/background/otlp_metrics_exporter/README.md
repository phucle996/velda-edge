# OpenTelemetry Metrics Exporter Plugin (`otlp_metrics_exporter`)

High-performance, zero-allocation background telemetry exporter for Velda Edge. Pushes compliant OpenTelemetry `ResourceMetrics` directly to an OpenTelemetry Collector via either **OTLP/gRPC** (binary Protobuf over persistent HTTP/2) or **OTLP/HTTP** (JSON over persistent HTTP/1.1) strictly from isolated Shadow Logistics Workers.

---

## 1. Documentation Index

| Document | Purpose | Primary Audience |
| :--- | :--- | :--- |
| **[README.md](README.md)** | Subsystem overview, configuration contract, production profiles, quickstart verification | All Engineers, Operators |
| **[RUNBOOK.md](RUNBOOK.md)** | SRE On-Call Incident Triage Matrix (P0–P3), Mermaid decision flowchart, CLI diagnostic toolkit, emergency playbooks | On-Call SREs, Systems Engineers |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | Deep micro-architectural specification, memory geometry, Protobuf in-place wire skeleton, disassembly proof, physical boundaries | Systems Architects, Performance Engineers |
| **[BENCHMARK.md](BENCHMARK.md)** | First-principles 5-tier zero-bloat benchmark specification adhering to `/velda-benchmark` with 6-scenario comparative matrix | Performance Engineers, SRE Leads |

---

## 2. Overview & Core Invariants

The `otlp_metrics_exporter` plugin (`phase: "background"`) operates as an autonomous background companion service designed to stream OpenTelemetry telemetry without competing with frontline traffic workers:

* **Shadow Worker Execution (`phase: "background"`)**: Operates exclusively on background companion threads (`velda-shadow-{id}`) under Linux `SCHED_IDLE`. Traffic workers serving client HTTP requests never execute serialization, Protobuf encoding, or outbound network calls.
* **Lock-Free Atomic Extraction**: Reads point-in-time snapshots from `velda-observability` via `MetricsStore::snapshot()` using relaxed atomic loads, completely eliminating locks, mutexes, and thread contention on the hot path.
* **Zero-Alloc In-Place Wire Skeleton (Protobuf)**: In gRPC mode, pre-renders the complete OpenTelemetry ResourceMetrics AST into a static binary wire skeleton once at startup, caching fixed-length byte offsets for timestamps and counters. Each flush cycle patches values directly into memory with zero dynamic heap allocations.
* **Pre-Baked Chunk Interleaving & SIMD Itoa (JSON)**: In HTTP mode, employs a pre-rendered JSON template, a 200-byte 2-digit ASCII lookup table (`DIGIT_PAIRS`), and 128-bit SSE2 vector itoa for timestamps, eliminating `serde_json` runtime parsing and dynamic string allocations.
* **Persistent Transport Reuse**: Maintains persistent HTTP/2 connections for gRPC (Port 4317) or Keep-Alive HTTP/1.1 connections for JSON (Port 4318), avoiding per-flush TCP and TLS handshake overhead.
* **Fail-Open Autonomy**: If the downstream OpenTelemetry Collector hangs, crashes, or rejects payloads, the exporter logs warnings and backs off without blocking or stalling downstream client request serving.

```text
╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
║                        VELDA EDGE — OTLP METRICS EXPORTER PIPELINE                               ║
║                  (crates/velda-plugin: otlp_metrics_exporter::plugin)                             ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════╝

 [FRONTLINE TRAFFIC WORKERS (Cores 0..N-1)]     [SHADOW LOGISTICS WORKER (Core i)]
 (Serving Hot Path, Priority 1)                 (Linux SCHED_IDLE, Assigned Core i)
  velda-worker-0 (Core 0)                        
  velda-worker-1 (Core 1)                        
  ...                                            
  velda-worker-n (Core N-1)                      
        │                                                    │ (Periodic Ticker)
        ▼                                                    ▼
  ┌───────────┐  Lock-Free Atomic Extraction ┌──────────────────────────────────┐
  │ Pipeline  │ ───────────────────────────► │      MetricsStore::snapshot()    │
  │ Counters  │    (Ordering::Relaxed)       │   (36 Pipelines, 288 Data Points)│
  └───────────┘                              └────────────────┬─────────────────┘
                                                              │
                                              ┌───────────────┴───────────────┐
                                              │                               │
                                        [mode: "grpc"]                  [mode: "http"]
                                              ▼                               ▼
                                   ┌─────────────────────┐         ┌─────────────────────┐
                                   │  proto/template.rs  │         │   json/template.rs  │
                                   │ Pre-Baked Wire      │         │ Pre-Baked Chunk     │
                                   │ Skeleton (Zero-Alloc│         │ Interleaving (JSON) │
                                   │ in-place stamping)  │         │ (LUT-accelerated)   │
                                   └──────────┬──────────┘         └──────────┬──────────┘
                                              │                               │
                                              ▼                               ▼
                                   ┌─────────────────────┐         ┌─────────────────────┐
                                   │   proto/client.rs   │         │    json/client.rs   │
                                   │ Persistent HTTP/2   │         │ Persistent HTTP/1.1 │
                                   │ 5-byte Framing      │         │ Keep-Alive (TCP/TLS)│
                                   └──────────┬──────────┘         └──────────┬──────────┘
                                              │                               │
                                              ▼                               ▼
                                  gRPC Port 4317 (Protobuf)        HTTP Port 4318 (JSON)
                                  ┌──────────────────────────────────────────────────────┐
                                  │               OpenTelemetry Collector                │
                                  └──────────────────────────────────────────────────────┘
```

---

## 3. Shadow Workload Weight & Core Affinity

Under the Velda Edge Workload Distribution Model, background logistics plugins are assigned a workload weight from 1 to 100 based on CPU intensity, memory footprint, cadence/wakeup rate, and failure blast radius:

| Physical Dimension | Metric Profile | Impact Analysis |
| :--- | :--- | :--- |
| **Assigned Workload Weight** | **40** | **Moderate Background Workload** |
| **CPU Intensity** | Moderate | Atomic vector extraction across 36 pipeline slots, in-place Protobuf stamping or 2-digit LUT JSON formatting, HTTP/2 binary framing or HTTP/1.1 serialization, and TLS cryptographic operations. |
| **Memory Footprint** | ~256 KB – 512 KB | Pre-allocated Protobuf wire template (32 KB), JSON scratch buffer (64 KB), and HTTP/2 transport connection state. Zero ongoing heap churn. |
| **Cadence / Wakeup Rate** | Configurable (1s – 3600s) | Operates on a coarse-grained interval (default: 10s or 15s), waking periodically without continuous CPU consumption. |
| **Failure Blast Radius** | P2 (Low Impact) | Telemetry loss only. Collector unreachable, rejected HTTP 4xx/5xx status, or network drops do not degrade client traffic serving. |

### Complementary Static Sharding Recommendation
* **Target Affinity**: Core 1, 2, or 3.
* **Anti-Colocation Rule**: Avoid co-locating on Core 0 when Core 0 is strictly dedicated to latency-critical `health_probe` (Weight: 1) and master traffic intake. Do not co-locate with heavy log shippers (`loki_log_shipper` or `otlp_log_shipper`) to avoid competing for memory bus bandwidth and TLS crypto hardware units.
* **Ideal Pairing**: Pair with sleep-bound rotators (`jwks_rotator`, `acme_cert_rotator`) or lightweight UDP exporters (`statsd_exporter`) on secondary cores.

---

## 4. 100% Explicit Configuration Contract

> [!IMPORTANT]
> **Strict Configuration Invariant**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. Execution phase is automatically inferred as `background` via `canonical_phase_for_id` based on `id: "otlp_metrics_exporter"`.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `protocol` | String | **Yes** | `"grpc"` or `"http"` | `"grpc"` | Transport protocol. `"grpc"` streams Protobuf over HTTP/2 (port 4317); `"http"` streams JSON over HTTP/1.1 (port 4318). |
| `endpoint` | String | **Yes** | Valid URI (`grpc://`, `grpcs://`, `http://`, `https://`) | `"grpc://127.0.0.1:4317"` | Target OpenTelemetry Collector ingestion endpoint. |
| `interval_secs` | Integer (u64) | **Yes** | `1 ..= 3600` | `10` | Periodic telemetry push interval in seconds. |
| `connect_timeout_ms` | Integer (u64) | **Yes** | `100 ..= 60000` | `5000` | TCP and TLS connection establishment timeout in milliseconds. |
| `request_timeout_ms` | Integer (u64) | **Yes** | `100 ..= 60000` | `10000` | Maximum round-trip timeout allowed for an export request to complete. |
| `service_name` | String | **Yes** | Non-empty ASCII string (`^[a-zA-Z0-9_\-\.]+$`) | `"velda-edge-prod"` | Logical service identifier reported in OpenTelemetry Resource attributes (`service.name`). |
| `headers` | Object | **Yes** | Key-value map of string pairs (`{}` if empty) | `{}` | Custom headers sent with every export request (e.g. Bearer auth, tenant ID). |

---

## 5. Production Configuration Profiles

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

### Profile 3: Secure Remote OTLP / gRPC with TLS
*Recommended for cross-datacenter or cloud ingestion endpoints.*

```json
{
  "id": "otlp_metrics_exporter",
  "config": {
    "protocol": "grpc",
    "endpoint": "grpcs://otel.eu-west-1.aws.internal:4317",
    "interval_secs": 15,
    "connect_timeout_ms": 5000,
    "request_timeout_ms": 10000,
    "service_name": "velda-edge-edge01",
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
      "id": "otlp_metrics_exporter",
      "config": {
        "protocol": "grpc",
        "endpoint": "grpc://127.0.0.1:4317",
        "interval_secs": 10,
        "connect_timeout_ms": 5000,
        "request_timeout_ms": 10000,
        "service_name": "velda-edge-prod",
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
Inspect logs to confirm successful periodic metric exports:
```text
INFO otlp_metrics_exporter: Starting OTLP metrics background exporter loop endpoint="grpc://127.0.0.1:4317" is_grpc=true interval_secs=10
DEBUG otlp_metrics_exporter: Flushed OTLP metrics batch via gRPC successfully
```
