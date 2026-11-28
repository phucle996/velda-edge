# Operations & Configuration Runbook (`log_exporter`)

## 1. Configuration Parameter Reference (`LogExporterConfig`)

`log_exporter` is configured in `plugins.json` under `"id": "log_exporter"` with a non-empty `targets` array discriminated by `"mode": "local" | "etp"`.

### 1.1 Local Target Fields (`mode: "local"`)

| Field | Type | Constraints / Allowed Values | Purpose |
| :--- | :--- | :--- | :--- |
| `mode` | `string` | `"local"` | Selects local stream/file output (no `etp.json` entry allowed). |
| `target` | `string` | `"stdout"`, `"stderr"`, or `/abs/path` | Persistent stream or append-only file descriptor (`O_CREAT \| O_APPEND`). |
| `format` | `string` | `"text"`, `"json"`, `"clf"` | Line formatting mode (`"clf"` renders Combined/Common Log Format). |
| `flush_interval_ms` | `u64` | `50..=60_000` | Batch drain interval on Shadow Worker. |
| `max_batch_size` | `usize` | `1..=10_000` | Target record count cap per flush cycle. |

### 1.2 ETP Target Fields (`mode: "etp"`)

Every ETP target requires `etp_id` referencing an entry in `etp.json`. Supported backends: `clickhouse`, `elasticsearch`, `loki`, `s3`, `kafka`, `otlp`.

| Parameter | Applicable Backends | Default | Constraints / Allowed Values | Purpose |
| :--- | :--- | :---: | :--- | :--- |
| `etp_id` | All `etp` | Required | Trimmed non-empty string | Target ETP ID in `etp.json`. |
| `etp_kind` | All `etp` | `None` | Matching backend kind | Optional `velda-sync` assertion guard. |
| `flush_interval_ms` | All `etp` | `1000` *(S3: `10_000`)* | CH/Loki/Kafka/OTLP: `100..=60_000`; ES: `10..=60_000`; S3: `100..=600_000` | Export interval on Shadow Worker. |
| `request_timeout_ms` | All `etp` | `5000` *(S3: `10_000`)* | CH/Kafka/Loki/OTLP: `100..=60_000`; ES/S3: `100..=120_000` | End-to-end export timeout. |
| `max_batch_size` | CH, Loki, Kafka, OTLP | CH/Kafka: `8000`; Loki/OTLP: `1000` | CH/Kafka: `1..=50_000`; Loki/OTLP: `1..=10_000` | Max records packed per batch payload. |
| `max_drain_batches` | Elasticsearch, S3 | ES: `64`; S3: `256` | `1..=4096` | Max pooled `256`-record batches drained per cycle. |
| `database` | ClickHouse | ETP `database` (`"default"`) | String | Destination ClickHouse DB for `velda_access_logs`. |
| `format` | ClickHouse \| Kafka | CH: `"row_binary"`; Kafka: `"json"` | CH: `"row_binary"` \| `"json"`; Kafka: `"json"` \| `"text"` \| `"binary"` | Wire encoding format. |
| `compression` | ES \| Kafka \| S3 | ES: `"gzip"`; Kafka/S3: `"zstd"` | ES: `"gzip"` \| `"none"`; Kafka/S3: `"zstd"` \| `"gzip"` \| `"none"` | Payload wire/object compression codec. |
| `index` / `op_type` / `pipeline` | Elasticsearch | ETP index / `"create"` / `None` | `op_type`: `"create"` \| `"index"`; valid ES index chars | `_bulk` action metadata and ingest pipeline query parameter. |
| `service_name` / `labels` | Loki \| OTLP *(svc only)* | `"velda-edge"` / `{}` | Non-empty `service_name`; Prometheus-valid `[a-zA-Z_][a-zA-Z0-9_]*` label keys | Stream / Resource identification attributes. |
| `prefix` | S3 | `"velda-logs"` | String | S3 object key prefix (`year=YYYY/month=MM/...`). |
| `topic` / `acks` / `partitioner` / `metadata_refresh_ms` | Kafka | Topic req. / `"leader"` / `"round_robin"` / `300_000` | Valid Kafka topic `<=249` chars; `acks`: `"none"\|"0"`, `"leader"\|"1"`, `"all"\|"-1"`; `partitioner`: `"round_robin"\|"route_id"\|"peer_ip"`; refresh `1_000..=3_600_000` | Kafka Produce durability, routing, and broker metadata refresh. |

---

## 2. Production Multi-Sink Recipes (`etp.json` + `plugins.json`)

### 2.1 `etp.json` (ClickHouse + Elasticsearch + Loki + S3 + Kafka + OTLP)

```json
{
  "schema_version": 1,
  "revision": 1,
  "etps": [
    {
      "id": "ch-main",
      "kind": "clickhouse",
      "security": "plain",
      "pool": { "pool_size": 4, "connect_timeout_ms": 2000, "idle_timeout_ms": 30000 },
      "settings": { "host": "192.168.122.20", "port": 8123, "database": "default", "user": "default", "password": "" }
    },
    {
      "id": "es-main",
      "kind": "elasticsearch",
      "security": "plain",
      "pool": { "pool_size": 4, "connect_timeout_ms": 2000, "idle_timeout_ms": 30000 },
      "settings": { "host": "192.168.122.20", "port": 9200, "index": "velda-access-logs" }
    },
    {
      "id": "loki-http",
      "kind": "loki",
      "security": "plain",
      "pool": { "pool_size": 4, "connect_timeout_ms": 2000, "idle_timeout_ms": 30000 },
      "settings": { "host": "192.168.122.20", "port": 3100, "protocol": "http" }
    },
    {
      "id": "s3-main",
      "kind": "s3",
      "security": "plain",
      "pool": { "pool_size": 4, "connect_timeout_ms": 2000, "idle_timeout_ms": 30000 },
      "settings": {
        "host": "192.168.122.20",
        "port": 9002,
        "region": "us-east-1",
        "bucket": "velda-logs",
        "access_key_id": "minioadmin",
        "secret_access_key": "minioadmin",
        "force_path_style": true
      }
    },
    {
      "id": "kafka-main",
      "kind": "kafka",
      "security": "plain",
      "pool": { "pool_size": 4, "connect_timeout_ms": 2000, "idle_timeout_ms": 30000 },
      "settings": { "brokers": ["192.168.122.20:9092"], "client_id": "velda-edge" }
    },
    {
      "id": "otlp-http-proto",
      "kind": "otlp",
      "security": "plain",
      "pool": { "pool_size": 4, "connect_timeout_ms": 2000, "idle_timeout_ms": 30000 },
      "settings": { "host": "192.168.122.20", "port": 4318, "protocol": "http/protobuf" }
    }
  ]
}
```

### 2.2 `plugins.json` (7-Target Multi-Sink Fan-Out)

```json
{
  "id": "log_exporter",
  "config": {
    "targets": [
      { "mode": "local", "target": "/var/log/velda/access.log", "format": "json", "flush_interval_ms": 100, "max_batch_size": 2000 },
      { "mode": "etp", "etp_id": "ch-main", "format": "row_binary", "flush_interval_ms": 200, "max_batch_size": 8000 },
      { "mode": "etp", "etp_id": "es-main", "index": "velda-access-logs", "op_type": "create", "compression": "gzip", "max_drain_batches": 64, "flush_interval_ms": 200 },
      { "mode": "etp", "etp_id": "loki-http", "service_name": "velda-edge", "flush_interval_ms": 200, "max_batch_size": 1000 },
      { "mode": "etp", "etp_id": "s3-main", "prefix": "velda-logs", "compression": "zstd", "max_drain_batches": 256, "flush_interval_ms": 1000 },
      { "mode": "etp", "etp_id": "kafka-main", "topic": "velda-logs", "format": "json", "compression": "zstd", "acks": "leader", "partitioner": "round_robin", "flush_interval_ms": 200, "max_batch_size": 8000 },
      { "mode": "etp", "etp_id": "otlp-http-proto", "service_name": "velda-edge", "flush_interval_ms": 200, "max_batch_size": 1000 }
    ]
  }
}
```

---

## 3. Backoff, Cool-Down Guard & Operational Tuning

- **Circuit Breaker (`plugin.rs`)**: Up to `10` consecutive export errors apply exponential backoff (`min(100ms << (attempt - 1), 2000ms)`), then enter a **30-second cool-down** where the failing task drains and discards batches cleanly (`"Log provider in 30s cool-down; discarded batches to protect memory"`) so other targets in the same `JoinSet` never stall.
- **High-RPS 100% Logging vs Sampling**: Use `observability.sampling` in `listeners.json` (`default: 0.1` or `default: 0.01` + `statuses: {"internal_error": 1.0}`) for high-throughput WAN clusters (`>40k RPS`). Each backend at `10%` handles `~5,000` records/s cleanly with `flush_interval_ms = 100–200 ms` and `0` dropped batches.

---

## 4. Incident Triage Playbook

| Symptom | Root Cause | Mitigation |
| :--- | :--- | :--- |
| Config error `has no attached ETP for etp_id '...'` | Target `etp_id` omitted from `etp.json` or `velda-sync` skipped. | Declare matching ETP in `etp.json` and run `velda-sync`. |
| ClickHouse returns HTTP 500 or schema error | Pre-existing mismatched `velda_access_logs` table missing/extra columns. | Verify 15-column `DESCRIBE TABLE default.velda_access_logs` (or `DROP TABLE` once so `ensure_table` recreates it cleanly). |
| S3 `PUT` returns `403` or host mismatch on MinIO/SeaweedFS | Missing `"force_path_style": true` for raw IP S3 endpoints. | Set `"force_path_style": true` inside S3 `settings` in `etp.json`. |
| Loki HTTP pushes dropped / 429 rate-limited | Loki default `ingestion_rate_mb` (`4MB` default) exceeded at high batch rate. | Raise Loki `limits_config.ingestion_rate_mb` / `per_stream_rate_limit`, or enable listener Weyl sampling. |
| Elasticsearch `_bulk` returns `"errors": true` on existing index | Target index uses immutable Datastream / `"create"` conflict vs upsert. | Set `"op_type": "index"` for non-datastream regular indices, or `"create"` for ES Data Streams. |
