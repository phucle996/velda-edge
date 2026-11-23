# Operations & Configuration Runbook: `elasticsearch_log_shipper`

This runbook covers configuration parameters, deployment presets, observability signals, and incident triage for `elasticsearch_log_shipper`.

---

## 1. Configuration Parameter Reference

`elasticsearch_log_shipper` enforces a strict separation between **shipper behavior** (`plugins.json`) and **transport/credentials/pool** (`etp.json`).

### 1.1 Plugin Config (`plugins.json` -> `config`)

| Field | Type | Default | Constraints / Validation | Description |
| :--- | :--- | :--- | :--- | :--- |
| `etp_id` | `String` | *(required)* | Must not be empty or whitespace-only | Identifier of the `elasticsearch` entry in `EtpRegistry` (`etp.json`). |
| `index` | `String` | `"velda-logs"` | Non-empty; must not contain `/`, `*`, `?`, `"`, `<`, `>`, `\|`, space, `,`, `#`, `:`, `\`, `\n`, `\r` | Target Elasticsearch/OpenSearch index or Data Stream name. Note: in `run()`, if `config.index.trim()` is empty (not possible via JSON validation, only programmatic struct construction), it falls back to `etp.config.index` or `"velda-logs"`. |
| `op_type` | `EsBulkOpType` | `"create"` | `"create"` \| `"index"` | Bulk action operation (`{"create":{"_index":"..."}}` vs `{"index":{"_index":"..."}}`). |
| `pipeline` | `Option<String>` | `null` | If `Some`, non-empty and must not contain `&`, `?`, space, `\r`, `\n` | Optional Elasticsearch ingest pipeline appended as `?pipeline=<name>`. |
| `compression` | `EsCompression` | `"gzip"` | `"gzip"` \| `"none"` | Wire compression applied to `_bulk` request bodies (`Content-Encoding: gzip` when enabled). |
| `max_drain_batches` | `usize` | `64` | `1..=4096` | Maximum `TelemetryBatch` items drained from `TelemetryStore` per tick. |
| `flush_interval_ms` | `u64` | `1000` | `10..=60000` | Interval in milliseconds between drain/flush ticks. |
| `request_timeout_ms` | `u64` | `5000` | `100..=120000` | End-to-end HTTP/1.1 `_bulk` write + response drain timeout per attempt. |

### 1.2 Referenced ETP Entry (`etp.json`, `kind: "elasticsearch"`)

| Field | Location | Default | Description |
| :--- | :--- | :--- | :--- |
| `id` | Top-level | *(required)* | Must match `etp_id` in `plugins.json`. |
| `kind` | Top-level | *(required)* | Must be `"elasticsearch"`. |
| `settings.endpoint` / `url` | `settings` | `http(s)://{host}:{port}` | Full URL; path component becomes `base_path`. |
| `settings.index` | `settings` | `null` | Optional fallback index stored on `ElasticsearchEtpConfig`. |
| `settings.api_key` | `settings` | `null` | Sent as `Authorization: ApiKey <key>`. Takes precedence over `basic_auth` and `bearer_token`. |
| `settings.basic_auth` | `settings` | `null` | `user:pass` (auto-base64 encoded) or pre-encoded base64 token; or `settings.username`/`user` + `settings.password`. |
| `settings.bearer_token` / `token` | `settings` | `null` | Sent as `Authorization: Bearer <token>`. |
| `pool` | Top-level | ETP defaults | `EtpPoolSettings` (baseline slots, max slots, acquire timeout). |
| `tls` | Top-level | `null` | Optional `EtpTlsConfig` for HTTPS / mTLS. |

---

## 2. Recommended Presets

### 2.1 Production Data Stream (Elastic Cloud / ES 8.x)
```json
{
  "etp_id": "es-prod",
  "index": "logs-velda.edge-default",
  "op_type": "create",
  "pipeline": "velda-edge-enrich",
  "compression": "gzip",
  "max_drain_batches": 256,
  "flush_interval_ms": 500,
  "request_timeout_ms": 10000
}
```
- **Why**: Data Streams require `create` (append-only). `gzip` reduces cross-AZ / WAN egress while `Compression::fast()` keeps Shadow worker CPU low.

### 2.2 High-Volume Local OpenSearch (Low-Latency LAN)
```json
{
  "etp_id": "opensearch-local",
  "index": "edge-access",
  "op_type": "index",
  "compression": "none",
  "max_drain_batches": 512,
  "flush_interval_ms": 250,
  "request_timeout_ms": 3000
}
```
- **Why**: On local 10GbE/25GbE links, skipping gzip saves CPU cycles when network bandwidth is not the bottleneck.

### 2.3 Minimal Development Setup
```json
{
  "etp_id": "es-dev"
}
```
- Uses defaults: `index = "velda-logs"`, `op_type = "create"`, `compression = "gzip"`, `max_drain_batches = 64`, `flush_interval_ms = 1000`, `request_timeout_ms = 5000`.

---

## 3. Observability Signals

### 3.1 Atomic Plugin Counters
- `shipped_records: Arc<AtomicU64>`: Incremented by `encoded` (number of `TelemetryRecord` items serialized and accepted with HTTP `200..300`).
- `failed_pushes: Arc<AtomicU64>`: Incremented by `1` on each failed `_bulk` push (non-2xx HTTP status, I/O error, parse error, or timeout).

### 3.2 Structured Tracing Logs
- **WARN** `"Elasticsearch _bulk reported partial item errors"`: HTTP 2xx received, but streaming scan detected `"errors":true` in the response body (records are still counted in `shipped_records`).
- **WARN** `"Elasticsearch _bulk returned non-2xx status"`: HTTP status outside `200..300`; increments `consecutive_failures`.
- **WARN** `"Failed to push batch to Elasticsearch"`: Transport/timeout/protocol error from `wire::push_bulk_payload`.
- **WARN** `"Elasticsearch shipper failed 10 consecutive times; entering 30s cool-down"`: Triggered when `consecutive_failures > 10` (i.e., on the 11th consecutive failure).
- **DEBUG** `"Elasticsearch log shipper in 30s cool-down; discarded batches"`: Drains and discards telemetry batches during cool-down to protect RAM.
- **INFO** `"Elasticsearch cool-down ended, resuming batch pushes"`: Resumes normal shipping after 30 seconds.

---

## 4. Incident Triage Playbook

| Symptom | Likely Root Cause | Resolution |
| :--- | :--- | :--- |
| Startup error: `has no attached ElasticsearchEtp for etp_id '...'` | `etp_id` missing from `etp.json` or `wire_plugin_etps` not executed. | Verify matching `id` in `etp.json` and ensure `kind == "elasticsearch"`. |
| Startup error: `requires an 'elasticsearch' ETP ... got '...'` | `etp_id` points to a non-Elasticsearch ETP (e.g. `loki` or `s3`). | Fix `etp_id` in `plugins.json` or `kind` in `etp.json`. |
| `400 Bad Request` on Data Stream index | `op_type` set to `"index"` instead of `"create"`. | Set `"op_type": "create"` in `plugins.json`. |
| `401` / `403` responses & cool-down loops | Invalid/expired `api_key`, `basic_auth`, or `bearer_token` in `etp.json`. | Rotate credential in `etp.json` and trigger config reload. |
| Partial item errors (`"errors":true` warnings) | Mapping conflict or ingest `pipeline` failure on specific documents. | Inspect Elasticsearch cluster logs / ingest pipeline `pipeline` configuration. |
| Repeated `Timeout` errors | `request_timeout_ms` too low for `max_drain_batches` size or overloaded ES cluster. | Lower `max_drain_batches`, enable `"compression": "gzip"`, or raise `request_timeout_ms`. |
