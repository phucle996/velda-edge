# Operations & Configuration Runbook: `clickhouse_log_shipper`

---

## 1. Configuration Parameter Reference

### 1.1 Plugin Config (`plugins.json` -> `config`)

| Field | Type | Default | Constraints / Validation | Description |
| :--- | :--- | :--- | :--- | :--- |
| `etp_id` | `String` | *(required)* | Must not be empty | Identifier of the `clickhouse` entry in `EtpRegistry` (`etp.json`). |
| `database` | `String` | `""` | Optional | Overrides `etp.config.database` when non-empty. |
| `format` | `String` | `"row_binary"` | `"row_binary"` \| `"rowbinary"` \| `"json"` \| `"json_each_row"` | ClickHouse wire format. `row_binary` is recommended for highest throughput. |
| `flush_interval_ms` | `u64` | `1000` | `100..=60000` | Periodic flush interval in milliseconds. |
| `max_batch_size` | `usize` | `8000` | `1..=50000` | Maximum records per HTTP `INSERT` payload. |
| `request_timeout_ms` | `u64` | `5000` | `100..=60000` | HTTP request timeout in milliseconds. |

### 1.2 Referenced ETP Entry (`etp.json`, `kind: "clickhouse"`)

| Field | Location | Default | Description |
| :--- | :--- | :--- | :--- |
| `id` | Top-level | *(required)* | Must match `etp_id`. |
| `kind` | Top-level | *(required)* | Must be `"clickhouse"`. |
| `settings.endpoint` | `settings` | *(required)* | ClickHouse HTTP endpoint (e.g. `http://192.168.122.20:8123`). |
| `settings.database` | `settings` | `"default"` | Default target database. |
| `settings.username` / `password` | `settings` | `"default"` / `""` | ClickHouse HTTP basic/header auth credentials. |

---

## 2. Recommended Presets

### 2.1 High-Throughput Production (`RowBinary`)
```json
{
  "etp_id": "ch-prod",
  "format": "row_binary",
  "flush_interval_ms": 500,
  "max_batch_size": 16000,
  "request_timeout_ms": 5000
}
```

### 2.2 Debug / Readable (`JSONEachRow`)
```json
{
  "etp_id": "ch-obs",
  "database": "default",
  "format": "json_each_row",
  "flush_interval_ms": 500,
  "max_batch_size": 4000,
  "request_timeout_ms": 5000
}
```

---

## 3. Incident Triage Playbook

| Symptom | Likely Root Cause | Resolution |
| :--- | :--- | :--- |
| `Code: 516 Authentication failed` | Wrong `username` or `password` in `etp.json`. | Update `settings.username` / `settings.password` in `etp.json`. |
| Startup DDL failure | User lacks `CREATE TABLE` privilege on target `database`. | Grant `CREATE TABLE, INSERT` on `<database>.velda_access_logs`. |
| Timeout on large batches | `max_batch_size` exceeds ClickHouse insert speed within `request_timeout_ms`. | Increase `request_timeout_ms` or reduce `max_batch_size`. |
