# Verification & Test Report: `loki_log_shipper`

---

## 1. Verified Automated Tests

### Commands Run
```bash
cargo test -p velda-plugin --lib background::loki_log_shipper
cargo test -p velda-plugin --test loki_suite_test --test loki_chaos_test
```

### Observed Results
- **Unit tests (`background::loki_log_shipper`) — 6/6 passed**:
  - `json::template::tests::test_loki_json_empty_batch` ... **ok**
  - `json::template::tests::test_loki_json_formatter_hex32_and_ipv6` ... **ok**
  - `proto::model::tests::test_push_request_prost_roundtrip` ... **ok**
  - `proto::template::tests::test_loki_proto_decode` ... **ok**
  - `validation::tests::test_valid_loki_log_shipper_config` ... **ok**
  - `validation::tests::test_invalid_loki_log_shipper_config` ... **ok**
- **Integration tests**: `loki_suite_test` (**7/7** passed) and `loki_chaos_test` (**5/5** passed).

---

## 2. Isolated KVM Live Verification (`velda-gw` -> `velda-obs` Grafana Loki 3.3.2)

- **Workload**: `wrk -t2 -c32 -d6s http://192.168.122.10:8080/` (**54,703.41 req/sec**, avg latency **571.06 us**).
- **`perf stat` (`velda-edge`)**: `15.78B` cycles, `16.46B` instructions (**1.04 IPC**), `22.54M` cache-misses, `27,853` context-switches, `0` cpu-migrations.
- **Loki (`:3100`)**: Stream `{env="kvm-bench", protocol="http", service="velda-edge"}` queried back with live access log entries, **0 WARN/ERROR** in `velda-edge`.
