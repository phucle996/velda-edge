# OpenTelemetry Trace Exporter Plugin (`otlp_trace_exporter`)

Asynchronous, fail-open background telemetry exporter for Velda Edge. Subscribes to reference-counted 64-byte [`velda_observability::SpanRecord`] batches drained from core-local Single-Producer Single-Consumer (SPSC) ring buffers across all L7 protocols (HTTP/1.1, HTTP/2, HTTP/3, gRPC) and exports standardized OpenTelemetry `ExportTraceServiceRequest` payloads to OpenTelemetry Collectors via **OTLP/gRPC** (direct Protobuf wire format over persistent HTTP/2) or **OTLP/HTTP** (JSON over persistent HTTP/1.1 Keep-Alive) strictly from isolated Shadow Logistics Workers.

---

## 1. Documentation Index

| Document | Purpose | Primary Audience |
| :--- | :--- | :--- |
| **[README.md](README.md)** | Subsystem overview, configuration contract, production profiles, quickstart verification | All Engineers, Operators |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | Deep-dive micro-architectural specification: 64-byte `SpanRecord` memory geometry, AVX2 non-temporal SPSC rings, zero-AST direct Protobuf wire splicing, lock-free catalog resolution | Systems Architects, Performance Engineers |
| **[BENCHMARK.md](BENCHMARK.md)** | First-principles 5-tier benchmark specification adhering to `/velda-benchmark` (`*[TBD]*` pending isolated testbed execution) | Performance Engineers, SRE Leads |
| **[RUNBOOK.md](RUNBOOK.md)** | SRE On-Call Incident Triage Matrix (P0–P3), Mermaid decision flowchart, diagnostic toolkit, recovery playbooks | On-Call SREs, Systems Engineers |

---

## 2. Overview & Core Invariants

The `otlp_trace_exporter` plugin acts as the dedicated background exporter for Velda Edge distributed spans without sharing locks, heap allocations, or network sockets with frontline traffic workers:

* **Shadow Worker Isolation**: Executes exclusively on background companion threads (`velda-shadow-{id}`) configured under Linux `SCHED_IDLE`. Frontline workers serving downstream client traffic never serialize spans, format Protobuf/JSON, or execute outbound collector network I/O.
* **Lock-Free SPSC Ring Handoff (`TraceStore`)**: Frontline workers record spans by writing a 64-byte scalar [`SpanRecord`] into a core-local SPSC ring buffer (`DEFAULT_TRACE_CORE_RING_CAPACITY = 4096` slots = 256 KB per core) using non-temporal streaming stores (`VMOVNTDQ` / `MOVNTDQ`) to avoid polluting L1 data caches.
* **Direct Wire Protobuf Serializer (gRPC Mode)**: `OtlpTraceProtoEnvelope` pre-bakes static `Resource` (`service.name`) and `InstrumentationScope` (`velda.edge.router`) framing bytes at initialization and encodes spans directly into a reusable byte buffer without constructing intermediate Prost AST object trees or heap-allocated `String` attributes.
* **Page-Aligned Zero-Copy Catalog Integration**: Route targets (`http.target`, span name `<method> <route>`) and upstream names (`net.peer.name`) are pulled lock-free as raw byte slices (`&[u8]`) directly from 4KB page-aligned [`velda_router::RouteCatalog`] and [`velda_upstream::UpstreamCatalog`] snapshots via `ArcSwapOption`.
* **Persistent Transport Reuse**:
  * **gRPC (`OtlpTracesGrpcClient`)**: Maintains a persistent HTTP/2 (`h2`) multiplexed channel to `/opentelemetry.proto.collector.trace.v1.TraceService/Export` (standard port `4317`) using 5-byte length-prefixed framing (`velda_grpc::encode_grpc_frame`).
  * **HTTP (`OtlpTraceHttpClient`)**: Maintains persistent HTTP/1.1 Keep-Alive TCP/TLS connections to `/v1/traces` (standard port `4318`).
* **Fail-Open Autonomy**: Collector outages, HTTP/gRPC errors, or export timeouts increment `failed_spans`, emit warning logs, and recover on subsequent flush ticks without stalling or blocking frontline traffic serving.

```text
╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
║                          VELDA EDGE — OTLP TRACE EXPORTER PIPELINE                               ║
║                    (crates/velda-plugin: otlp_trace_exporter::plugin)                            ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════╝

 [FRONTLINE TRAFFIC WORKERS (Cores 0..N-1)]     [SHADOW LOGISTICS WORKER (SCHED_IDLE)]
  velda-worker-0..N-1                            velda-shadow-{i}
         │                                                    │ (Periodic flush_interval_ms)
         ▼                                                    ▼
 ┌───────────────┐  Non-Temporal SPSC Ring   ┌───────────────────────────────────┐
 │  SpanRecord   │ ────────────────────────► │ TraceStore::flush_all_lanes()     │
 │ (64B Cacheline│  AVX2 VMOVNTDQ / sfence   │ -> subscriber.drain_batches(64)   │
 └───────────────┘                           └────────────────┬──────────────────┘
                                                              │
                   ┌──────────────────────────────────────────┴────────────────────────────────────────┐
                   │ (Atomic Arc<[SpanRecord]> Batch + Lock-Free RouteCatalog/UpstreamCatalog &[u8])   │
                   ▼                                                                                   ▼
      [protocol: "grpc"]                                                                  [protocol: "http"]
  ┌─────────────────────────┐                                                         ┌─────────────────────────┐
  │    proto/template.rs    │                                                         │    json/template.rs     │
  │ Pre-Baked Scope/Res Wire│                                                         │ Pre-Allocated String Buf│
  │ Direct Wire Byte Splice │                                                         │ itoa + Raw UTF-8 Blit   │
  └────────────┬────────────┘                                                         └────────────┬────────────┘
               ▼                                                                                   ▼
  ┌─────────────────────────┐                                                         ┌─────────────────────────┐
  │     proto/client.rs     │                                                         │     json/client.rs      │
  │ Persistent HTTP/2 (h2)  │                                                         │ HTTP/1.1 Keep-Alive     │
  │ 5B LPM gRPC Framing     │                                                         │ TCP / Rustls Transport  │
  └────────────┬────────────┘                                                         └────────────┬────────────┘
               ▼                                                                                   ▼
     gRPC Port 4317 (/TraceService/Export)                                               HTTP Port 4318 (/v1/traces)
               └──────────────────────────────────────────┬────────────────────────────────────────┘
                                                          ▼
                                            ┌───────────────────────────┐
                                            │  OpenTelemetry Collector  │
                                            └───────────────────────────┘
```

---

## 3. Shadow Workload Weight & Core Affinity

Under the Velda Edge Workload Distribution Model, `OtlpTraceExporterPlugin::weight()` declares a static weight of **`50`**:

| Physical Dimension | Profile | Operational Notes |
| :--- | :--- | :--- |
| **Declared Workload Weight** | **`50`** | Moderate background workload (`OtlpTraceExporterPlugin::weight()`) |
| **CPU Activity Source** | Batch serialization + network send | Draining SPSC rings, reading raw catalog slices, formatting Protobuf wire or JSON payloads, and flushing sockets every `flush_interval_ms`. |
| **Working Memory Buffers** | Pre-allocated scratch arenas | `proto_buf` / `json_buf` initialized at `32 * 1024` bytes (32 KiB) + `Arc<[SpanRecord]>` subscriber channel depth `256` (draining up to `64` batches per tick). |
| **Failure Blast Radius** | Telemetry loss only (`failed_spans`) | Downstream client serving remains unaffected when the collector drops connections or refuses payloads. |

---

## 4. Explicit Configuration Contract

> [!IMPORTANT]
> **Strict Configuration Invariant**: All parameters below must be explicitly present in `plugins.json` and pass `validate_otlp_trace_exporter_config`. Execution stage is fixed at compile time (`BackgroundPlugin`).

| Field | Type | Required | Constraints / Validation Rules | Example Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `protocol` | String | **Yes** | `"http"` or `"grpc"` (case-insensitive trimmed) | `"grpc"` | Export transport mode: `"grpc"` (Protobuf over HTTP/2) or `"http"` (JSON over HTTP/1.1). |
| `endpoint` | String | **Yes** | Must start with `"http://"`, `"https://"`, `"grpc://"`, or `"grpcs://"` | `"grpc://127.0.0.1:4317"` | Target OpenTelemetry Collector URI. |
| `service_name` | String | **Yes** | Non-empty string after trim | `"velda-edge-prod"` | OpenTelemetry Resource `service.name` attribute. |
| `flush_interval_ms` | Integer (`u64`) | **Yes** | `10 ..= 60_000` | `100` | Periodic flush interval (ms) for waking the background export loop. |
| `max_batch_size` | Integer (`usize`) | **Yes** | `1 ..= 10_000` | `1000` | Maximum number of `SpanRecord` entries drained per lane flush call. |
| `connect_timeout_ms` | Integer (`u64`) | **Yes** | `100 ..= 60_000` | `1000` | TCP/TLS connection establishment timeout (ms). |
| `request_timeout_ms` | Integer (`u64`) | **Yes** | `100 ..= 60_000` | `1000` | Maximum time (ms) allowed for a collector export request round-trip. |
| `headers` | Object | **Yes** | `HashMap<String, String>` (`{}` if none) | `{}` | Custom HTTP/gRPC headers attached to every export request (validated into `http::HeaderMap`). |

---

## 5. Configuration Profiles

### Profile 1: Local / Cluster OTLP/gRPC Collector (Port 4317)
```json
{
  "id": "otlp_trace_exporter",
  "config": {
    "protocol": "grpc",
    "endpoint": "grpc://otel-collector.monitoring.svc.cluster.local:4317",
    "service_name": "velda-edge-production",
    "flush_interval_ms": 100,
    "max_batch_size": 1000,
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 1000,
    "headers": {}
  }
}
```

### Profile 2: OTLP/HTTP JSON Collector (Port 4318)
```json
{
  "id": "otlp_trace_exporter",
  "config": {
    "protocol": "http",
    "endpoint": "http://otel-collector.monitoring.svc.cluster.local:4318/v1/traces",
    "service_name": "velda-edge-production",
    "flush_interval_ms": 100,
    "max_batch_size": 1000,
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 1000,
    "headers": {}
  }
}
```

### Profile 3: TLS-Encrypted HTTP Collector with Bearer Auth
```json
{
  "id": "otlp_trace_exporter",
  "config": {
    "protocol": "http",
    "endpoint": "https://otel.monitoring.internal:4318/v1/traces",
    "service_name": "velda-edge-wan",
    "flush_interval_ms": 200,
    "max_batch_size": 2000,
    "connect_timeout_ms": 2000,
    "request_timeout_ms": 2000,
    "headers": {
      "authorization": "Bearer <token>"
    }
  }
}
```

---

## 6. Verification & Testing

Run the plugin unit and integration test suites to verify direct wire encoding compliance against official `opentelemetry-proto` and end-to-end mock collector shipping:

```bash
rtk cargo test -p velda-plugin --lib otlp_trace_exporter
rtk cargo test -p velda-plugin --test otlp_trace_exporter_test
```
