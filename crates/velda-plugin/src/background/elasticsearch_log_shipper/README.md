# Elasticsearch / OpenSearch Background Log Shipper (`elasticsearch_log_shipper`)

`elasticsearch_log_shipper` is a background observability plugin for **Velda Edge** that drains access telemetry batches from `velda_observability::TelemetryStore` and ships them to Elasticsearch or OpenSearch over the `_bulk` HTTP/1.1 API using pooled connections from [`velda-etp`](../../../velda-etp/) (`ElasticsearchEtp`).

```text
  Frontline Traffic Workers (velda-worker-*)
        │  lock-free push (TelemetryRecord)
        ▼
  velda_observability::TelemetryStore
        │  interval tick (flush_interval_ms) -> drain_batches(max_drain_batches)
        ▼
  Background Task: ElasticsearchLogShipperPlugin (pbww = 60, etp_id)
        │
        ├─► format_bulk_ndjson() ──► pointer-stamped NDJSON + prebaked action line
        ├─► optional GzEncoder (Compression::fast()) when compression = "gzip"
        └─► wire::push_bulk_payload(&etp)
              ├─► etp.acquire().await -> ElasticsearchLease (24B in-place Slot[idx])
              ├─► POST {base_path}/_bulk{?pipeline=...} HTTP/1.1 (keep-alive)
              ├─► streaming "\"errors\":true" scan across chunked / Content-Length body
              └─► on Io EOF: lease.reconnect().await once; on >=500/close: recreate_in_background()
```

---

## 1. What This Plugin Does

- **Out-of-Band Access Log Shipping**: Subscribes to `TelemetryStore` and periodically drains up to `max_drain_batches` telemetry batches without blocking Frontline request serving.
- **Zero-`serde_json` Hot Formatting**: Serializes each `TelemetryRecord` into Elasticsearch `_bulk` NDJSON (`action` line + document JSON line with `@timestamp` in epoch milliseconds) via direct pointer stamping (`format_bulk_ndjson`).
- **ETP Connection Reuse**: Acquires an exclusive RAII `ElasticsearchLease` from `EtpRegistry` (`ElasticsearchEtp`) rather than opening per-request TCP/TLS connections.
- **Compression & Ingest Pipelines**: Supports `gzip` (fast level) or `none` request body compression and optional `?pipeline=<name>` query routing.
- **Fault Isolation & RAM Guard**: Retries once in-place on transient I/O EOF (`lease.reconnect()`), recycles broken/5xx connections asynchronously (`lease.recreate_in_background()`), and enters a 30-second cool-down that drains and discards telemetry batches after more than 10 consecutive push failures to prevent unbounded memory growth.

---

## 2. Where It Sits in Velda Edge

| Attribute | Value |
| :--- | :--- |
| **Phase / Direction** | `Direction::Background` (`crates/velda-plugin/src/background/elasticsearch_log_shipper/`) |
| **Background Weight (`pbww`)** | `60` |
| **Supported Protocols** | `SupportedProtocols::empty()` (runs out-of-band, not inline in L7 request routing) |
| **ETP Dependency** | Requires an `elasticsearch` entry in `etp.json` matching `etp_id` (`EtpBackend::Elasticsearch`) |
| **Telemetry Source** | `velda_observability::TelemetryStore` (`global_telemetry_store()` by default) |

---

## 3. Requirements & Prerequisites

1. **ETP Pool Entry**: An `elasticsearch` entry must be defined in `etp.json` (containing `endpoint` / `host` / `port`, optional `base_path`, optional `index`, authentication via `api_key` / `basic_auth` / `bearer_token`, `pool` sizing, and optional `tls`).
2. **Elasticsearch / OpenSearch `_bulk` Endpoint**: Target cluster must accept `POST {base_path}/_bulk` with `Content-Type: application/x-ndjson` (and `Content-Encoding: gzip` when `compression` is `"gzip"`).
3. **Data Streams vs Standard Indices**: Use `"op_type": "create"` (the default) when targeting Elasticsearch Data Streams; `"index"` is available for standard indices.

---

## 4. Quick Start Configuration

### 4.1 `etp.json` (Connection & Auth)
```json
{
  "id": "es-primary",
  "kind": "elasticsearch",
  "settings": {
    "endpoint": "https://logs-es.internal:9200",
    "index": "velda-logs",
    "api_key": "dG9rZW4tZXhhbXBsZQ=="
  },
  "pool": {
    "min_idle": 2,
    "max_size": 8
  }
}
```

### 4.2 `plugins.json` (Shipper Behavior)
```json
{
  "id": "es_shipper",
  "kind": "elasticsearch_log_shipper",
  "enabled": true,
  "config": {
    "etp_id": "es-primary",
    "index": "velda-logs",
    "op_type": "create",
    "compression": "gzip",
    "max_drain_batches": 64,
    "flush_interval_ms": 1000,
    "request_timeout_ms": 5000
  }
}
```

---

## 5. Documentation Directory

- **[RUNBOOK.md](./RUNBOOK.md)**: Complete parameter tables (`plugins.json` & `etp.json`), production presets, observability counters, and incident triage playbook.
- **[ARCHITECTURE.md](./ARCHITECTURE.md)**: Internal pipeline, `EtpPageArena` / `ElasticsearchLease` memory model, pointer-stamping NDJSON formatter, and HTTP/1.1 streaming scanner.
- **[BENCHMARK.md](./BENCHMARK.md)**: Standardized 6-scenario x 5-tier performance benchmark specification.
- **[TEST.md](./TEST.md)**: Verified `cargo test` unit and integration test results.
