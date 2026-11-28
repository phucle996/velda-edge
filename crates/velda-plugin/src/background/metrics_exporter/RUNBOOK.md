# Operations & Configuration Runbook (`metrics_exporter`)

## 1. Configuration Parameter Reference (`MetricsExporterConfig`)

`metrics_exporter` is registered in `plugins.json` under `"id": "metrics_exporter"`. It takes a non-empty `targets` array discriminated by `"mode": "pull" | "push"`.

### 1.1 Pull Target Fields (`mode: "pull"` — Prometheus OpenMetrics Server)

| Field | Type | Constraints / Allowed Values | Purpose |
| :--- | :--- | :--- | :--- |
| `mode` | `string` | `"pull"` | Enables local HTTP/HTTPS scrape server (requires zero `etp.json` entry). |
| `listen` | `string` | Valid `SocketAddr` (`0.0.0.0:9090`) | Bind socket address opened via `acquire_shadow_tcp`. |
| `path` | `string` | Non-empty, begins with `/` (`"/metrics"`) | Scrape route. `/healthz` is always exposed concurrently without auth checks. |
| `protocol` | `string` | `"http"`, `"https"` | Must align with `tls.mode` (`"none"` vs `"tls"`/`"mtls"`). |
| `scrape.timeout_ms` | `u64` | `100..=120_000` | Read/write timeout and `Semaphore` wait deadline per scrape request. |
| `scrape.max_concurrency` | `usize` | `1..=1024` | Max simultaneous scrape connections and `BufferPool` initial slot count. |
| `scrape.max_buffer_size_kb` | `usize` | `8..=4096` | Pre-allocated scratch buffer capacity in KB (`buffer_bytes = kb * 1024`). |
| `tls.mode` | `string` | `"none"`, `"tls"`, `"mtls"` | TLS mode; `"tls"` requires `cert_file`+`key_file`, `"mtls"` adds `client_ca_file`. |
| `cors.enabled` | `bool` | `true` \| `false` | Enables CORS response headers and `OPTIONS` preflight handling. |
| `auth.mode` | `string` | `"none"`, `"bearer"`, `"basic"` | Auth guard on `path` (`token` for bearer, `username`+`password` for basic). |

### 1.2 Push Target Fields (`mode: "push"` — ETP OTLP / StatsD Exporters)

| Field | Type | Default | Constraint / Allowed Values | Purpose |
| :--- | :--- | :---: | :--- | :--- |
| `mode` | `string` | Required | `"push"` | Routes target through `velda-etp` (`OtlpEtp` or `StatsdEtp`). |
| `etp_id` | `string` | Required | Trimmed non-empty ID | References an `otlp` or `statsd` pool in `etp.json`. |
| `etp_kind` | `string \| null` | `null` | Expected backend kind | Optional compile-time assertion enforced during `velda-sync` / attach. |
| `interval_secs` | `u64 \| null` | `15` (OTLP) / `2` (StatsD) | `1..=3600` | Periodic flush interval on the Shadow Worker. |
| `request_timeout_ms` | `u64 \| null` | `5000` *(OTLP)* | `100..=60_000` | Per-export HTTP/gRPC client request timeout. |
| `service_name` | `string \| null` | `"velda-edge"` *(OTLP)* | Trimmed non-empty string | Baked into OTLP Resource `service.name` attribute. |
| `temporality` | `string \| null` | `"delta"` *(OTLP)* | `"delta"` (`1`), `"cumulative"` (`2`) | Sum temporality (`delta` uses `curr.delta_since(&prev)`). |
| `prefix` | `string \| null` | ETP `prefix` or `"velda"` *(StatsD)* | String | Metric prefix; empty config falls back to ETP `prefix`. |
| `dialect` | `string \| null` | ETP `flavor` or `"dogstatsd"` *(StatsD)* | `"dogstatsd"`, `"standard"` | `#tag:val` DogStatsD format vs dot-separated Graphite/Standard format. |
| `max_packet_size` | `usize \| null` | `1432` *(StatsD)* | `256..=65507` | MTU ceiling per UDP datagram packed by `StatsdBatchWriter`. |

---

## 2. Supported Metrics Sink Matrix (`velda-etp`)

| Target Kind | Transport & Framing | Temporality / Packaging |
| :--- | :--- | :--- |
| **Prometheus Pull (`mode: "pull"`)** | Direct HTTP/1.1 or HTTPS (`BufferPool` + `acquire_shadow_tcp`) | Cumulative OpenMetrics text + `GET /healthz` probe. |
| **OTLP HTTP/Protobuf (`etp kind: "otlp"`)** | HTTP/1.1 Keep-Alive `POST /v1/metrics` (`application/x-protobuf`) | Configurable `delta` (`1`) or `cumulative` (`2`). |
| **OTLP HTTP/JSON (`etp kind: "otlp"`)** | HTTP/1.1 Keep-Alive `POST /v1/metrics` (`application/json`) | Configurable `delta` (`1`) or `cumulative` (`2`). |
| **OTLP gRPC (`etp kind: "otlp"`)** | Native HTTP/2 `h2c`/TLS `MetricsService/Export` | Configurable `delta` (`1`) or `cumulative` (`2`). |
| **DogStatsD (`etp kind: "statsd"`)** | UDP batched up to `64` pkts/syscall (`libc::sendmmsg` on Linux) | Delta per interval, `#tag:value` encoding, MTU-packed (`1432 B` default). |
| **Standard StatsD (`etp kind: "statsd"`)** | UDP batched up to `64` pkts/syscall (`libc::sendmmsg` on Linux) | Delta per interval, dot-hierarchy counters/gauges, MTU-packed. |

---

## 3. Configuration Recipes

### 3.1 `etp.json` Multi-Sink Telemetry Pools

```json
{
  "schema_version": 1,
  "etps": [
    {
      "id": "otlp-http-proto",
      "kind": "otlp",
      "transport": "http",
      "encoding": "protobuf",
      "endpoints": [{ "address": "192.168.122.20:4318", "weight": 1 }],
      "metrics_path": "/v1/metrics",
      "pool": { "max_idle_per_endpoint": 8, "idle_timeout_secs": 90 }
    },
    {
      "id": "otlp-grpc",
      "kind": "otlp",
      "transport": "grpc",
      "endpoints": [{ "address": "192.168.122.20:4317", "weight": 1 }],
      "pool": { "max_idle_per_endpoint": 4, "idle_timeout_secs": 90 }
    },
    {
      "id": "statsd-dog",
      "kind": "statsd",
      "flavor": "dogstatsd",
      "prefix": "velda",
      "endpoints": [{ "address": "192.168.122.20:8125", "weight": 1 }],
      "pool": { "size": 4 }
    }
  ]
}
```

### 3.2 `plugins.json` Hybrid Prometheus + OTLP Proto + DogStatsD

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
      },
      {
        "mode": "push",
        "etp_id": "statsd-dog",
        "interval_secs": 2,
        "dialect": "dogstatsd",
        "max_packet_size": 1432
      }
    ]
  }
}
```

---

## 4. Resilience, Backoff & Runtime Signals

- **OTLP Export Backoff & Circuit Cooldown (`plugin.rs`)**:
  - Attempts `1..=10` apply exponential sleep `min(100ms << (attempt - 1), 2000ms)`.
  - On reaching `10` consecutive failures, enters a **30-second cool-down**, logs skip cycles at `debug`, resets failure counter, and retries cleanly.
  - Because `prev` snapshot is only updated on `Ok(())`, transient OTLP collector restarts accumulate into the next delta without silent counter gaps.
- **StatsD Socket Recovery (`wire.rs`)**:
  - Send failures trigger `lease.reconnect().await` on the in-place `Slot[idx]` and retry once before calling `lease.recreate_in_background()`; failed ticks preserve the prior snapshot baseline.
- **Internal Plugin Counters**: `flushed_count` tracks successful OTLP HTTP/gRPC flushes (or StatsD datagrams sent) and `failed_count` tracks failed export cycles.

---

## 5. Incident Triage Playbook

| Symptom | Root Cause | Mitigation |
| :--- | :--- | :--- |
| Config error `has no attached ETP for etp_id '...'` | Push `etp_id` absent from `etp.json` or mismatched `velda-sync` artifact. | Add/fix matching ETP entry in `etp.json` and run `velda-sync` before reloading `velda-edge`. |
| Config error `protocol 'https' requires tls.mode != 'none'` | Inconsistent pull TLS settings. | Use `protocol: "http"` + `tls.mode: "none"`, or configure `cert_file` / `key_file` for `"https"`. |
| StatsD UDP drops across VPN/overlay MTU | `max_packet_size: 1432` exceeds overlay tunnel MTU (WireGuard/GRE/VXLAN). | Reduce `max_packet_size` to `1200` (allowed `256..=65507`). |
| OTLP warns `OTLP metrics push in 30s cool-down` | Target OTLP collector failed > `10` consecutive export intervals. | Check `velda-obs` / OTLP receiver `:4317`/`:4318` health; counters continue accumulating safely in RAM until recovery. |
| Pull scrape timeout under heavy Prometheus fan-in | Low `scrape.max_concurrency` or undersized `scrape.timeout_ms`. | Increase `max_concurrency` (`<= 1024`) and size `max_buffer_size_kb` >= `256` KB. |
