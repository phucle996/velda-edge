# Operations & Configuration Runbook: `s3_log_shipper`

---

## 1. Configuration Parameter Reference

### 1.1 Plugin Config (`plugins.json` -> `config`)

| Field | Type | Default | Constraints / Validation | Description |
| :--- | :--- | :--- | :--- | :--- |
| `etp_id` | `String` | *(required)* | Must not be empty or whitespace-only | Identifier of the `s3` entry in `EtpRegistry` (`etp.json`). |
| `prefix` | `String` | `"velda-logs"` | Must not contain `".."` | Key prefix inside the bucket (leading/trailing `/` trimmed). Empty string writes directly under `year=YYYY/...`. |
| `compression` | `S3Compression` | `"zstd"` | `"zstd"` \| `"gzip"` \| `"none"` | Object body compression (`jsonl.zst`, `jsonl.gz`, or `jsonl`). |
| `max_drain_batches` | `usize` | `256` | `1..=4096` | Maximum `TelemetryBatch` items drained per tick. |
| `flush_interval_ms` | `u64` | `10000` | `100..=600000` | Interval in milliseconds between upload cycles. |
| `request_timeout_ms` | `u64` | `10000` | `100..=120000` | End-to-end HTTP/1.1 `PUT` + response drain timeout. |

### 1.2 Referenced ETP Entry (`etp.json`, `kind: "s3"`)

| Field | Location | Default | Description |
| :--- | :--- | :--- | :--- |
| `id` | Top-level | *(required)* | Must match `etp_id`. |
| `kind` | Top-level | *(required)* | Must be `"s3"`. |
| `settings.endpoint` | `settings` | *(required)* | S3 endpoint URL (e.g. `https://s3.us-east-1.amazonaws.com` or MinIO/SeaweedFS URL). |
| `settings.bucket` | `settings` | *(required)* | Target S3 bucket name. |
| `settings.region` | `settings` | `"us-east-1"` | AWS signing region for SigV4. |
| `settings.access_key_id` | `settings` | *(required)* | AWS / S3 access key ID. |
| `settings.secret_access_key` | `settings` | *(required)* | AWS / S3 secret access key. |
| `settings.session_token` | `settings` | `null` | Optional STS temporary security token (`x-amz-security-token`). |
| `settings.addressing_style` | `settings` | `"path"` (when custom endpoint) | `"path"` (`/{bucket}/{key}`) or `"virtual"` (`/{key}` with bucket in `Host`). |

---

## 2. Recommended Presets

### 2.1 Production Cold Archive (AWS S3 / R2, Zstd)
```json
{
  "etp_id": "s3-prod",
  "prefix": "edge-logs/prod",
  "compression": "zstd",
  "max_drain_batches": 1024,
  "flush_interval_ms": 15000,
  "request_timeout_ms": 20000
}
```

### 2.2 Local S3-Compatible Object Store (MinIO / SeaweedFS, Gzip)
```json
{
  "etp_id": "s3-local",
  "prefix": "velda-logs",
  "compression": "gzip",
  "max_drain_batches": 256,
  "flush_interval_ms": 5000,
  "request_timeout_ms": 10000
}
```

---

## 3. Observability Signals

- `shipped_records: Arc<AtomicU64>`: Incremented by `encoded` on HTTP `200..300` `PUT` responses.
- `failed_pushes: Arc<AtomicU64>`: Incremented by `1` on non-2xx responses, I/O errors, or timeouts.
- **Cool-Down**: After `consecutive_failures > 10`, logs a `WARN` and discards drained batches for 30 seconds before retrying.

---

## 4. Incident Triage Playbook

| Symptom | Likely Root Cause | Resolution |
| :--- | :--- | :--- |
| Startup error: `has no attached S3Etp` | Missing `etp_id` in `etp.json` or wrong `kind`. | Ensure matching `"id"` with `"kind": "s3"` in `etp.json`. |
| `403 SignatureDoesNotMatch` | Wrong `region`, `secret_access_key`, or virtual-host vs path addressing mismatch. | Verify `region` and `addressing_style` (`"path"` for MinIO/SeaweedFS/local S3). |
| `404 NoSuchBucket` | Target bucket was not created. | Create the bucket before starting `velda-edge`. |
| Repeated `Timeout` errors | `max_drain_batches` too large for `request_timeout_ms` or slow object store. | Increase `request_timeout_ms` or reduce `flush_interval_ms` / `max_drain_batches`. |
