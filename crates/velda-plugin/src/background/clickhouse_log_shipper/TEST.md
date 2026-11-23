# Verification & Test Report: `clickhouse_log_shipper`

---

## 1. Verified Automated Tests

### Commands Run
```bash
cargo test -p velda-plugin --lib background::clickhouse_log_shipper
cargo test -p velda-plugin --test clickhouse_suite_test
```

### Observed Results
- **Unit tests (`background::clickhouse_log_shipper`) — 9/9 passed**:
  - `format::row_binary::tests::test_row_binary_full_serialization` ... **ok**
  - `format::row_binary::tests::test_row_binary_serialization` ... **ok**
  - `format::json::tests::test_json_each_row_serialization` ... **ok**
  - `format::json::tests::test_json_url_escape_and_max_row` ... **ok**
  - `validation::tests::test_invalid_flush_interval_and_timeouts` ... **ok**
  - `validation::tests::test_validate_config_defaults_optional_database` ... **ok**
  - `validation::tests::test_validate_config_empty_etp_id` ... **ok**
  - `validation::tests::test_validate_config_invalid_format` ... **ok**
  - `validation::tests::test_validate_config_valid` ... **ok**
- **Integration tests (`clickhouse_suite_test`)**: All test cases passed in workspace verification (`cargo test -p velda-plugin`).

---

## 2. Isolated KVM Live Verification (`velda-gw` -> `velda-obs` ClickHouse 24.3-alpine)

- **Workload**: `wrk -t2 -c32 -d6s http://192.168.122.10:8080/` (**53,984.11 req/sec**, avg latency **587.95 us**).
- **`perf stat` (`velda-edge`)**: `15.27B` cycles, `15.66B` instructions (**1.03 IPC**), `21.47M` cache-misses, `20,490` context-switches, `2` cpu-migrations.
- **ClickHouse (`:8123`)**: Verified `velda_access_logs` table creation and **+41,238** new rows inserted (`86,788` -> `128,026`), **0 WARN/ERROR** in `velda-edge`.
