# Verification & Test Report: `otlp_log_shipper`

---

## 1. Verified Automated Tests

### Commands Run
```bash
cargo test -p velda-plugin --lib background::otlp_log_shipper
cargo test -p velda-plugin --test otlp_suite_test --test otlp_chaos_suite_test
```

### Observed Results
- **Unit tests (`background::otlp_log_shipper`) — 6/6 passed**:
  - `json::template::tests::test_otlp_json_schema` ... **ok**
  - `proto::template::tests::test_max_log_record_fits_reserve` ... **ok**
  - `proto::template::tests::test_otlp_proto_wire_template` ... **ok**
  - `proto::template::tests::test_otlp_proto_severity` ... **ok**
  - `validation::tests::test_valid_otlp_log_shipper_config` ... **ok**
  - `validation::tests::test_invalid_otlp_log_shipper_config` ... **ok**
- **Integration tests**: `otlp_suite_test` (**10/10** passed, including shared `OtlpEtp` across all 3 OTLP plugins) and `otlp_chaos_suite_test` (**7/7** passed).

---

## 2. Isolated KVM Live Verification (`velda-gw` -> `velda-obs` OTel Collector `0.120.0`)

- **Workload**: `wrk -t2 -c32 -d6s http://192.168.122.10:8080/` (**54,362.72 req/sec**, avg latency **614.16 us**).
- **`perf stat` (`velda-edge`)**: `14.95B` cycles, `15.64B` instructions (**1.05 IPC**), `23.53M` cache-misses, `22,704` context-switches, `3` cpu-migrations.
- **OTel Collector (`:4317` gRPC)**: Decoded `ResourceLog` and `LogRecord` batches cleanly, **0 WARN/ERROR** in `velda-edge`.
