# Verification Report (`log_exporter`)

## 1. Executed Test, Lint & Verification Commands

All unit, clippy, and formatting checks below exited cleanly with status `0`:

```bash
cargo fmt --check
cargo clippy -p velda-plugin --all-targets --all-features -- -D warnings
cargo test -p velda-plugin -- log_exporter
cargo test -p velda-observability
```

## 2. Verified Passing Test Suites (`29` `log_exporter` tests + `28` `velda-observability` tests)

| Crate / Module | Passed | Failed | Scope & Key Verified Cases |
| :--- | :---: | :---: | :--- |
| `validation` (`log_exporter`) | 2 | 0 | `test_validate_mixed_local_and_etp_targets`, `test_reject_empty_targets_and_missing_etp_id`. |
| `providers::local` | 3 | 0 | `test_format_std_log_payload_all_modes` (`text`, `json`, `clf`), `test_file_sink_persistent_append`, `test_stdout_stderr_sink_init`. |
| `providers::clickhouse` | 4 | 0 | `test_row_binary_serialization`, `test_row_binary_full_serialization`, `test_json_each_row_serialization`, `test_json_url_escape_and_max_row`. |
| `providers::elasticsearch` | 1 | 0 | `test_bulk_ndjson_formatting`. |
| `providers::loki` | 4 | 0 | `test_loki_json_empty_batch`, `test_loki_json_formatter_hex32_and_ipv6`, `test_loki_proto_decode`, `test_push_request_prost_roundtrip`. |
| `providers::s3` | 2 | 0 | `test_sigv4_signing_determinism_and_cache`, `test_unix_secs_to_utc_ymdhms`. |
| `providers::kafka` | 9 | 0 | `test_format_single_record_binary`, `test_format_single_record_json`, `test_format_single_record_text_{http,grpc}`, `test_crc32c_standard_vectors`, `test_zigzag_varint`, `test_produce_request_framing`, `test_sasl_plain_payload`, `test_compress_records_all_codecs` (`none`, `gzip`, `zstd`). |
| `providers::otlp` | 4 | 0 | `test_otlp_json_schema`, `test_max_log_record_fits_reserve`, `test_otlp_proto_severity`, `test_otlp_proto_wire_template`. |
| `velda-observability` | 28 | 0 | Sampling Weyl gates, `TelemetryRecord` 256B CL0 alignment, `test_multi_exporter_fanout`, `test_slow_subscriber_eviction_preserves_rotating_pool_slots`. |

## 3. End-to-End Real Backend Integration Verification (2-KVM `tc netem`)

Verified live via `velda-sync` + `velda-edge` against real containerized storage servers on `192.168.122.20` (`SC01`–`SC03`):

- **ClickHouse `24.8-alpine` (`:8123`, `RowBinary`)**: Auto-creates `default.velda_access_logs` (`15` columns) via `ensure_table` (note: resolved legacy `peer_port` DDL column mismatch in `clickhouse/wire.rs`), inserts `39,344 / 392,546` (`10.023%` lossless at `10%` sampling) and `104,578` rows under concurrent All-7 saturation.
- **Elasticsearch `8.17.0` (`:9200`, `_bulk` + `gzip`)**: Indexes `39,793 / 396,981` (`10.024%`) documents verified with `_refresh` + `_count`.
- **Grafana Loki `3.3.2` (`:3100`, JSON Push)**: Verified `40,471 / 403,686` (`10.025%`) lines via LogQL `count_over_time` query.
- **SeaweedFS S3 (`:9002`, SigV4 `force_path_style`)**: Uploads `9` Zstd-compressed `.jsonl.zst` objects (`821,166 B` at `10%`; `11` objects / `2.20 MB` in All-7 `100%`) verified via `list-type=2`.
- **Redpanda `v24.3.1` (`:9092`, Kafka RecordBatch v2 + Zstd)**: Produces `40,297 / 401,987` (`10.024%`) offsets verified across 3 topic partitions via `rpk topic describe -p`.
- **OTel Collector Contrib `v0.166.0` (`:4318`, OTLP HTTP/Proto)**: Accepts `40,907 / 408,091` (`10.024%`) log records (`0` refused).
- **Partial Outage Isolation (`SC03`)**: Stopping ClickHouse (`docker stop clickhouse`) left surviving sinks conserving identical `39,010` records across ES, Kafka, OTLP, and Loki + `9` S3 `.zst` objects while holding Frontline proxy throughput at `48,327.13 RPS` (`0` errors).
