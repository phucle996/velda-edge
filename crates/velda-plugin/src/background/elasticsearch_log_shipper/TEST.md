# Verification & Test Report: `elasticsearch_log_shipper`

This document records the verified unit and integration test suites for `elasticsearch_log_shipper`.

---

## 1. Test Coverage Overview

1. **Configuration Validation (`validation.rs`)**:
   - Verifies defaults (`index = "velda-logs"`, `op_type = Create`, `compression = Gzip`, `max_drain_batches = 64`, `flush_interval_ms = 1000`, `request_timeout_ms = 5000`).
   - Verifies explicit configuration parsing (`etp_id`, `index`, `op_type`, `pipeline`, `compression`, `max_drain_batches`, `flush_interval_ms`, `request_timeout_ms`).
   - Verifies rejection of empty `etp_id`, forbidden index characters (`/`, `*`, `?`, `"`, `<`, `>`, `|`, space, `,`, `#`, `:`), invalid `pipeline` values, and out-of-range numeric bounds.
2. **NDJSON Pointer-Stamping Formatter (`format.rs`)**:
   - Verifies prebaked `{"create":{"_index":"velda-logs"}}` action line, `@timestamp` epoch-ms conversion, IPv4-mapped IPv6 formatting (`192.168.1.10`), and JSON string escaping for `url`.
3. **End-to-End & ETP Registry Integration (`crates/velda-plugin/tests/s3_es_shipper_test.rs`)**:
   - Verifies full `TelemetryStore` -> `ElasticsearchLogShipperPlugin` -> `ElasticsearchEtp` -> mock HTTP `_bulk` server delivery (`test_elasticsearch_shipper_end_to_end`).
   - Verifies `EtpRegistry` wiring (`etp_id` attachment) and in-place `reconnect()` recovery on transient EOF (`test_elasticsearch_etp_registry_wiring_and_reconnect_recovery`).

---

## 2. Verified Test Execution

### Commands Run
```bash
cargo test -p velda-plugin --lib elasticsearch_log_shipper
cargo test -p velda-plugin --test s3_es_shipper_test elasticsearch
```

### Observed Results

#### Unit Tests (`velda-plugin --lib elasticsearch_log_shipper`)
- `background::elasticsearch_log_shipper::validation::tests::test_validate_elasticsearch_config_valid_and_defaults` ... **ok**
- `background::elasticsearch_log_shipper::format::tests::test_bulk_ndjson_formatting` ... **ok**
- `background::elasticsearch_log_shipper::validation::tests::test_validate_elasticsearch_config_rejections` ... **ok**
- **Summary**: `3 passed; 0 failed; 0 ignored` (523 filtered out).

#### Integration Tests (`velda-plugin --test s3_es_shipper_test elasticsearch`)
- `test_elasticsearch_shipper_end_to_end` ... **ok**
- `test_elasticsearch_etp_registry_wiring_and_reconnect_recovery` ... **ok**
- **Summary**: `2 passed; 0 failed; 0 ignored` (4 filtered out).
