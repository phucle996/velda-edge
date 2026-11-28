# Background Metrics Exporter Plugin (`metrics_exporter`)

## Architecture Overview

```text
  Frontline Traffic Workers (velda-worker-{0..N-1}, SCHED_OTHER, Zero IO / Zero Locks)
    └─► Request Completion ─► PipelineSlot[pipe_idx]:
          ├─ load-before-or: if (status_mask & bit) == 0 { status_mask.fetch_or(bit) }
          ├─ status_counts[InternalStatus as usize].fetch_add(1, Relaxed)
          └─ counters[16].fetch_add(...)   [128B transport + duration + TTFB histogram]

  Shadow Logistics Worker (velda-shadow-{i}, SCHED_IDLE, pbww = 40):
  ┌────────────────────────────────────────────────────────────────────────────────────────┐
  │ MetricsExporterPlugin::run(shutdown) ──► JoinSet per MetricsTargetConfig:              │
  │   ├── mode == "pull" ─► PrometheusPullConfig (acquire_shadow_tcp, BufferPool, zero ETP)│
  │   └── mode == "push" ─► attach_etp(etp_id) ─┬─ OtlpMetricsProvider (HTTP Proto/JSON,  │
  │                                              │                      gRPC h2c/TLS)      │
  │                                              └─ StatsdMetricsProvider (DogStatsD /     │
  │                                                   Standard UDP, MTU batch + sendmmsg)  │
  └────────────────────────────────────────────────────────────────────────────────────────┘
```

## 1. What This Plugin Does

`metrics_exporter` consolidates Velda Edge's OpenMetrics pull endpoint and outbound OTLP/StatsD push telemetry into a single multi-target background plugin without touching Frontline request serving:
- **Hybrid Multi-Target Dispatch (`targets`)**: A single `"metrics_exporter"` entry supports any mixture of local Prometheus pull endpoints (`mode: "pull"`) and `velda-etp`-backed push sinks (`mode: "push"`). Multiple ETPs (`otlp-http-proto`, `otlp-http-json`, `otlp-grpc`, `statsd-dog`, `statsd-std`) attach in parallel via `BackgroundPlugin::etp_ids()` and `attach_etp()`.
- **Sparse `status_mask` Bitmask Aggregation**: Instead of serializing all `64 × 41 = 2,624` `[PipeKind × InternalStatus]` combinations, Frontline slots flip active status bits lazily (`load-before-or`) and Shadow Workers iterate set bits via hardware bit-scan (`tzcnt` + `blsr`) over `MetricsSnapshot` or `delta_since`.
- **Pre-Baked Zero-Alloc Formatting**: Pre-computes protocol headers, label blocks, protobuf `PrebakedDpFragment` binary skeletons, and MTU-packed StatsD templates once at startup; steady-state scrapes reuse `BufferPool` scratch memory and Linux pushes batch up to `64` UDP datagrams per `libc::sendmmsg` syscall.

## 2. Where It Sits in Velda Edge

- **Pipeline Stage**: `background` (`BackgroundPlugin`, scheduled strictly onto `velda-shadow-{i}` under `SCHED_IDLE` via static LPT bin-packing with `pbww = 40`).
- **Worker Placement**:
  - Metric recording executes wait-free on **Frontline Traffic Workers** directly inside `MetricsStore` (`velda-observability`) with zero JSON/protobuf serialization and zero socket I/O.
  - HTTP OpenMetrics scraping (`GET /metrics`, `GET /healthz`) and periodic OTLP/StatsD flushes run asynchronously inside `JoinSet` tasks on the assigned **Shadow Logistics Worker**.
- **Supported Protocols**: `SupportedProtocols::empty()` (aggregates telemetry across all ingress pipelines: `HTTP/1.1`, `HTTP/2`, `HTTP/3`, `gRPC`, `TCP`, `UDP`).

## 3. Requirements & Prerequisites

- **Prometheus Pull Mode (`mode: "pull"`)**: Zero `velda-etp` dependencies. Binds its management TCP/TLS socket (`listen`) directly via `acquire_shadow_tcp`.
- **Push Mode (`mode: "push"`)**: Requires matching entries in `etp.json` / `etp.bin` whose backend resolves to `EtpBackend::Otlp` or `EtpBackend::Statsd` (`velda-etp`). Non-metrics ETPs are rejected during `attach_etp()`.

## 4. Quick Start Configuration

```json
{
  "id": "metrics_exporter",
  "config": {
    "targets": [
      {
        "mode": "pull",
        "listen": "0.0.0.0:9090",
        "path": "/metrics",
        "protocol": "http",
        "scrape": { "timeout_ms": 5000, "max_concurrency": 32, "max_buffer_size_kb": 512 },
        "tls": { "mode": "none", "cert_file": null, "key_file": null, "client_ca_file": null },
        "cors": { "enabled": false, "allow_origins": [], "allow_methods": [], "allow_headers": [] },
        "auth": { "mode": "none", "token": null, "username": null, "password": null }
      },
      {
        "mode": "push",
        "etp_id": "otlp-http-proto",
        "interval_secs": 15,
        "request_timeout_ms": 5000,
        "service_name": "velda-edge",
        "temporality": "delta"
      }
    ]
  }
}
```

## 5. Documentation Directory

- [RUNBOOK.md](./RUNBOOK.md) — Complete `targets` schema, `etp.json` push recipes, backoff/cooldown behavior, and incident triage.
- [ARCHITECTURE.md](./ARCHITECTURE.md) — `PipelineSlot` cache layout, sparse `status_mask` + `delta_since` mechanics, pre-baked wire templates, and Linux `sendmmsg` transport.
- [BENCHMARK.md](./BENCHMARK.md) — 2-KVM `tc netem` 5-tier comparison against Nginx/HAProxy/APISIX/Kong/Envoy, isolated providers, scrape storm isolation, and `x86-64-v3` release ASM audit.
- [TEST.md](./TEST.md) — Executed Cargo unit/integration test suites and multi-protocol collector verification report.
