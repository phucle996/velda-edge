# Operations & Configuration Runbook (`trace_exporter`)

## 1. Configuration Parameter Reference (`TraceExporterConfig`)

`trace_exporter` is registered in `plugins.json` under `"id": "trace_exporter"`. It requires a non-empty `targets` array (`Vec<TraceTargetConfig>`), where every entry exports to an OTLP pool declared in `etp.json`.

| Field | Type | Default | Constraint / Allowed Values | Purpose |
| :--- | :--- | :---: | :--- | :--- |
| `etp_id` | `string` | Required | Trimmed non-empty ID | Identifier of an `otlp` pool entry in `etp.json`. |
| `etp_kind` | `string \| null` | `null` | Expected ETP kind hint | Optional validation check enforced during `velda-sync` / attachment. |
| `service_name` | `string` | `"velda-edge"` | Trimmed non-empty string | Logical `service.name` resource attribute baked into spans. |
| `flush_interval_ms` | `u64` | `1000` | `10..=60_000` | Export interval in milliseconds on the Shadow Worker (`pbww = 50`). |
| `max_batch_size` | `usize` | `1000` | `1..=10_000` | Target upper batch sizing parameter per export configuration. |
| `request_timeout_ms` | `u64` | `5000` | `100..=60_000` | End-to-end HTTP/gRPC collector export deadline per batch push. |

---

## 2. Supported OTLP ETP Profiles (`velda-etp`)

| `etp.json` `settings.protocol` | Wire Endpoint & Headers | Serializer |
| :--- | :--- | :--- |
| `"http"` / `"http/protobuf"` | HTTP/1.1 Keep-Alive `POST {base_path}/v1/traces` (`application/x-protobuf`) | `OtlpTraceProtoEnvelope` (Zero-tree wire encoder) |
| `"json"` / `"http/json"` | HTTP/1.1 Keep-Alive `POST {base_path}/v1/traces` (`application/json`) | `OtlpTraceJsonEnvelope` (`itoa` + LUT hex IDs) |
| `"grpc"` | Native HTTP/2 `POST /opentelemetry.proto.collector.trace.v1.TraceService/Export` | `OtlpTraceProtoEnvelope` + 5B gRPC length frame |

Non-OTLP ETP kinds (e.g., `statsd`, `kafka`, `redis`) are rejected during `attach_etp()`.

---

## 3. High-Throughput `flush_interval_ms` & Sampling Sizing

Each `trace_exporter` subscriber drains up to **`64` batches × `256` records = `16,384` spans per tick** (`TelemetryBatchPool` holds `64` total slots of `64 KB`). Size `flush_interval_ms` against your sampled span rate $\Lambda_{\text{spans/s}} = \text{RPS} \times r_{\text{sample}}$:

$$\Lambda_{\text{spans/s}} \cdot \frac{\text{flush\_interval\_ms}}{1000} \le 16,384 \text{ spans/tick}$$

- **1%–10% Production Sampling (`listeners.json` `observability.sampling`)**: Default `flush_interval_ms: 1000` drains ~500–5,000 spans/tick comfortably under `50k RPS` with zero dropped batches.
- **Un-sampled `100%` Capture at High RPS (`> 20k RPS`)**: Lower `flush_interval_ms` to **`50–100 ms`** and pair with `OtlpProtocol::HttpProtobuf` or `Grpc` to match collector ingest rate while keeping RSS bounded.

---

## 4. Configuration Recipes

### 4.1 Collector Pools (`etp.json`)

```json
{
  "schema_version": 1,
  "revision": 1,
  "etps": [
    {
      "id": "otlp-http-proto",
      "kind": "otlp",
      "security": "plain",
      "pool": { "pool_size": 8, "connect_timeout_ms": 2000, "idle_timeout_ms": 30000 },
      "settings": { "host": "192.168.122.20", "port": 4318, "protocol": "http/protobuf" }
    },
    {
      "id": "otlp-grpc",
      "kind": "otlp",
      "security": "plain",
      "pool": { "pool_size": 8, "connect_timeout_ms": 2000, "idle_timeout_ms": 30000 },
      "settings": { "host": "192.168.122.20", "port": 4317, "protocol": "grpc" }
    }
  ]
}
```

### 4.2 Error-Biased Production Listener Sampling (`listeners.json`) + Trace Fan-Out (`plugins.json`)

In `listeners.json`, retain `1%` of successful requests and `100%` of server/upstream errors:
```json
"observability": {
  "sampling": {
    "default": 0.01,
    "statuses": {
      "internal_error": 1.0,
      "upstream_timeout": 1.0
    }
  }
}
```
And fan out sampled spans to both primary gRPC OTLP and secondary HTTP/Proto collectors in `plugins.json`:
```json
{
  "id": "trace_exporter",
  "config": {
    "targets": [
      {
        "etp_id": "otlp-grpc",
        "service_name": "velda-edge",
        "flush_interval_ms": 200,
        "max_batch_size": 1000,
        "request_timeout_ms": 5000
      },
      {
        "etp_id": "otlp-http-proto",
        "service_name": "velda-edge",
        "flush_interval_ms": 500,
        "max_batch_size": 1000,
        "request_timeout_ms": 5000
      }
    ]
  }
}
```

---

## 5. Backoff, Cooldown & RAM Guard (`plugin.rs`)

- **Exponential Backoff**: Consecutive OTLP transport/5xx errors `1..=10` trigger sleep `min(100ms << (attempt - 1), 2000ms)`.
- **30s Circuit Cooldown & RAM Guard**: After `10` consecutive failures, `trace_exporter` enters a **30-second cool-down**, actively calling `subscriber.drain_batches(64)` and discarding stale batches (`"Trace exporter in 30s cool-down; discarded batches to free RAM"`) so offline collectors never exhaust `TelemetryBatchPool` or wedge Data Plane workers.
- **Signals**: `shipped_spans` counts exported spans; `failed_spans` increments per failed flush batch.

---

## 6. Incident Triage Playbook

| Symptom | Root Cause | Mitigation |
| :--- | :--- | :--- |
| Config error `has no attached ETP for etp_id '...'` | Push `etp_id` absent from `etp.json` or `velda-sync` skipped. | Add matching `"otlp"` pool in `etp.json` and re-run `velda-sync`. |
| Collector shows zero spans while `RPS` is high | `observability.sampling.default` set to `0.0` or `trace_exporter` target omitted. | Inspect `listeners.json` sampling rates and verify collector `:4317`/`:4318` reachability. |
| Fewer than 100% spans arrive at `100%` rate under `>= 40k RPS` | Single `SCHED_IDLE` shadow worker + `TelemetryBatchPool` bounded-memory shedding when frontline cores saturate ~90%. | Switch production traffic to `0.01–0.10` Weyl sampling (`100%` delivery verified at `1%` & `10%`), or reduce `flush_interval_ms` (`50–100ms`). |
| Warning logs `Trace exporter in 30s cool-down` | Target OTLP collector down/refusing connections for `> 10` consecutive attempts. | Restore OTel Collector (`:4317`/`:4318`); exporter automatically reconnects after cooldown. |
