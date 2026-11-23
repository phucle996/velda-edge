# S3-Compatible Object Storage Background Log Shipper (`s3_log_shipper`)

`s3_log_shipper` is a background observability plugin for **Velda Edge** that drains access telemetry batches from `velda_observability::TelemetryStore`, formats them into NDJSON, compresses them (`zstd`, `gzip`, or `none`), signs requests with **AWS Signature Version 4 (SigV4)**, and uploads objects via HTTP/1.1 `PUT` using pooled connections from [`velda-etp`](../../../velda-etp/) (`S3Etp`).

```text
  Frontline Traffic Workers (velda-worker-*)
        │  lock-free push (TelemetryRecord)
        ▼
  velda_observability::TelemetryStore
        │  interval tick (flush_interval_ms) -> drain_batches(max_drain_batches)
        ▼
  Background Task: S3LogShipperPlugin (pbww = 65, etp_id)
        │
        ├─► format_ndjson() ──► pointer-stamped NDJSON into reusable payload_buf
        └─► wire::upload_batch(&mut wire_state, &payload_buf)
              ├─► compress_object() (zstd | gzip | none)
              ├─► Hive key: {prefix}/year=YYYY/month=MM/day=DD/hour=HH/velda-{ts}-{seq}.{ext}
              ├─► SigV4Signer::sign_put_headers() (SHA-256 + cached daily signing key)
              ├─► etp.acquire().await -> S3Lease (24B in-place Slot[idx])
              └─► HTTP/1.1 PUT (reconnect once on EOF; recreate_in_background on >=500/close)
```

---

## 1. What This Plugin Does

- **Out-of-Band Object Storage Archival**: Ships access logs asynchronously to AWS S3, MinIO, SeaweedFS, Cloudflare R2, or Ceph RGW without touching Frontline request serving.
- **Hive-Style Time Partitioning**: Writes objects under `{prefix}/year=YYYY/month=MM/day=DD/hour=HH/velda-{unix_secs}-{seq}.jsonl{.zst|.gz|}` for direct querying by Athena, Trino, ClickHouse, or DuckDB.
- **Zero-SDK AWS SigV4 Signing**: Computes SHA-256 payload digests and HMAC-SHA256 signatures via `ring::digest` / `ring::hmac` with daily signing-key caching (`SigV4Signer`).
- **ETP Connection Reuse**: Acquires an exclusive `S3Lease` from `EtpRegistry` (`S3Etp`) supporting path-style and virtual-hosted-style addressing plus optional STS `x-amz-security-token`.
- **30s Cool-Down RAM Guard**: After >10 consecutive push failures, enters a 30-second cool-down that drains and discards telemetry batches to cap memory usage.

---

## 2. Where It Sits in Velda Edge

| Attribute | Value |
| :--- | :--- |
| **Phase / Direction** | `Direction::Background` (`crates/velda-plugin/src/background/s3_log_shipper/`) |
| **Background Weight (`pbww`)** | `65` |
| **Supported Protocols** | `SupportedProtocols::empty()` |
| **ETP Dependency** | Requires an `s3` entry in `etp.json` matching `etp_id` (`EtpBackend::S3`) |
| **Telemetry Source** | `velda_observability::TelemetryStore` |

---

## 3. Requirements & Prerequisites

1. **ETP Pool Entry**: An `s3` entry in `etp.json` containing `endpoint` / `bucket` / `region` / `access_key_id` / `secret_access_key` (and optional `session_token`, `addressing_style`, `tls`).
2. **Target Bucket**: The destination bucket must already exist and grant `s3:PutObject` to the configured credentials.

---

## 4. Quick Start Configuration

### 4.1 `etp.json`
```json
{
  "id": "s3-archive",
  "kind": "s3",
  "settings": {
    "endpoint": "http://192.168.122.20:9005",
    "bucket": "velda-logs",
    "region": "us-east-1",
    "access_key_id": "velda",
    "secret_access_key": "veldasecret",
    "addressing_style": "path"
  },
  "pool": { "min_idle": 1, "max_size": 4 }
}
```

### 4.2 `plugins.json`
```json
{
  "id": "s3_log_shipper",
  "enabled": true,
  "config": {
    "etp_id": "s3-archive",
    "prefix": "velda-logs",
    "compression": "zstd",
    "max_drain_batches": 256,
    "flush_interval_ms": 10000,
    "request_timeout_ms": 10000
  }
}
```

---

## 5. Documentation Directory

- **[RUNBOOK.md](./RUNBOOK.md)**: Complete parameter tables (`plugins.json` & `etp.json`), presets, observability signals, and incident triage.
- **[ARCHITECTURE.md](./ARCHITECTURE.md)**: `S3WireState`, `S3Lease` in-place slot borrowing, SigV4 daily key caching, and HTTP/1.1 keep-alive framing.
- **[BENCHMARK.md](./BENCHMARK.md)**: Standardized 6-scenario x 5-tier performance benchmark specification.
- **[TEST.md](./TEST.md)**: Verified `cargo test` unit and integration test results.
