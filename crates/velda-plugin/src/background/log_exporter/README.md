# Background Access Log Exporter Plugin (`log_exporter`)

## Architecture Overview

```text
  Frontline Traffic Workers (velda-worker-{0..N-1}, SCHED_OTHER, Zero IO / Zero Locks)
    └─► Request Completion ─► StatusSampling (3-ALU Weyl Gate: add/shr/cmp)
          │
          └─► keep == true ─► TelemetryRecord (256B, 4 Cache Lines, 0 heap ptrs)
                │               └─ non-temporal store (_mm256_stream) -> SpscTelemetryRing (2MB HugePage/core)
                ▼
  TelemetryBatchPool (64 Slots × 256 Records = 64KB Contiguous Pages, Multi-Buffer Rotation)
                │
                ▼ wait-free fan-out (leases = K subscribers, zero-copy borrow across targets)
  Shadow Logistics Worker (velda-shadow-{i}, SCHED_IDLE, pbww = 80):
  ┌────────────────────────────────────────────────────────────────────────────────────────┐
  │ LogExporterPlugin::run(shutdown) ──► JoinSet per LogTargetConfig:                      │
  │   ├─► mode == "local" ─► StdLogSink (stdout | stderr | persistent File, text|json|clf) │
  │   └─► mode == "etp"   ─► attach_etp(etp_id) ─► static EtpBackend dispatch:             │
  │         ├─ ClickHouse    (HTTP RowBinary / JSONEachRow + auto DDL ensure_table)        │
  │         ├─ Elasticsearch (HTTP _bulk NDJSON + gzip/none, Create Data Streams / Index)  │
  │         ├─ Loki          (HTTP JSON / gRPC Snappy Protobuf)                            │
  │         ├─ S3            (AWS SigV4 PUT .jsonl[.zst|.gz], daily kSigning cache)        │
  │         ├─ Kafka         (Native RecordBatch v2 Binary TCP + Zstd/Gzip + CRC32C)       │
  │         └─ Otlp          (OTLP Logs HTTP/Protobuf, HTTP/JSON, gRPC h2c/TLS)            │
  └────────────────────────────────────────────────────────────────────────────────────────┘
```

## 1. What This Plugin Does

`log_exporter` replaces legacy per-sink access log shippers with one unified multi-target exporter built on top of `velda-observability::TelemetryStore` and `velda-etp`:
- **Hybrid `local` + `etp` Multi-Sink Fan-Out (`targets`)**: A single `"log_exporter"` entry can simultaneously stream formatted logs to local files (`stdout`/`stderr`/`/abs/path`) and fan out in parallel across **6 remote storage/collector backends** (`ClickHouse`, `Elasticsearch`, `Grafana Loki`, `S3`, `Apache Kafka`, `OTLP Logs`) over isolated `JoinSet` tasks with zero redundant Frontline allocations.
- **Direct Wire Pointer Stamping & Hardware Streaming**: Binary/text formatters (`ClickHouse RowBinary`, `ES _bulk`, `Loki`, `Kafka RecordBatch v2`, `S3 NDJSON`) reserve capacity up front, prefetch upcoming `256 B` `TelemetryRecord` structs into L2 via `_MM_HINT_T1`, and write packed primitives directly with zero `core::fmt` calls on the steady-state loop.
- **Isolated Failure Domains per Target**: Each target owns its own subscriber queue (`drain_batches(max_drain_batches)`), exponential backoff (`100ms..2000ms`), and `30s` cool-down RAM guard. An outage in one backend (e.g., ClickHouse stopped) never blocks sibling sinks (`Elasticsearch`, `Kafka`, `Loki`, `S3`, `OTLP`, `Local`) or Frontline proxy latency.

## 2. Where It Sits in Velda Edge

- **Pipeline Stage**: `background` (`BackgroundPlugin`, scheduled onto `velda-shadow-{i}` under `SCHED_IDLE` via static LPT bin-packing with `pbww = 80`).
- **Worker Placement**:
  - Sampling (`StatusSampling`) and ring appends execute strictly lock-free on **Frontline Traffic Workers**.
  - Formatting, optional compression (`zstd` / `gzip`), AWS SigV4 signing, and HTTP/gRPC/TCP network transmission run strictly asynchronously inside **Shadow Logistics Workers**.
- **Supported Protocols**: `SupportedProtocols::empty()` (aggregates unified `TelemetryRecord` events across all L7 ingress pipelines).

## 3. Requirements & Prerequisites

- **Local Stream Mode (`mode: "local"`)**: Zero `velda-etp` dependencies. Writes to `"stdout"`, `"stderr"`, or a writable filesystem path.
- **External ETP Mode (`mode: "etp"`)**: Each entry requires a non-empty `etp_id` pointing to a `clickhouse`, `elasticsearch`, `loki`, `s3`, `kafka`, or `otlp` pool defined in `etp.json`. Non-log ETPs are rejected during `attach_etp()`.

## 4. Quick Start Configuration

```json
{
  "id": "log_exporter",
  "config": {
    "targets": [
      {
        "mode": "local",
        "target": "/var/log/velda/access.log",
        "format": "json",
        "flush_interval_ms": 100,
        "max_batch_size": 2000
      },
      {
        "mode": "etp",
        "etp_id": "ch-main",
        "format": "row_binary",
        "flush_interval_ms": 200,
        "max_batch_size": 8000
      }
    ]
  }
}
```

## 5. Documentation Directory

- [RUNBOOK.md](./RUNBOOK.md) — Complete `LocalLogTargetConfig` / `EtpLogTargetConfig` schema, `etp.json` recipes across all 6 backends, backoff/cooldown guards, and triage playbook.
- [ARCHITECTURE.md](./ARCHITECTURE.md) — `TelemetryRecord` 256B layout, `SpscTelemetryRing` + `TelemetryBatchPool`, ClickHouse `RowBinary` pointer stamping, Kafka RecordBatch v2, SigV4 daily key caching, and `JoinSet` isolation.
- [BENCHMARK.md](./BENCHMARK.md) — 2-KVM `tc netem` 5-tier comparison against Nginx, APISIX, Kong, and Envoy pushing to **6 real containerized backends** (`ClickHouse 24.8`, `Elasticsearch 8.17`, `Loki 3.3.2`, `Redpanda`, `SeaweedFS S3`, `OTel Collector v0.166.0`), isolated 10% sampling verification, partial-outage isolation, and `x86-64-v3` release ASM audit.
- [TEST.md](./TEST.md) — Executed 29+ unit test suites (`velda-plugin` + `velda-observability`) and end-to-end storage backend verification report.
