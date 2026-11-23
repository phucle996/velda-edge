# Verification & Test Report: `otlp_trace_exporter`

---

## 1. Verified Automated Tests

### Commands Run
```bash
cargo test -p velda-plugin --lib background::otlp_trace_exporter
cargo test -p velda-plugin --test otlp_trace_exporter_test --test otlp_suite_test
```

### Observed Results
- **Unit tests (`background::otlp_trace_exporter`) — 4/4 passed**:
  - `json::template::tests::json_encode` ... **ok**
  - `proto::template::tests::wire_encode_decode` ... **ok**
  - `validation::tests::test_valid_otlp_trace_exporter_config` ... **ok**
  - `validation::tests::test_invalid_otlp_trace_exporter_config` ... **ok**
- **Integration tests**: `otlp_trace_exporter_test` (**4/4** passed) and `otlp_suite_test` (**10/10** passed).

---

## 2. Isolated KVM Live Verification (`velda-gw` -> `velda-obs` OTel Collector `0.120.0`)

- **Workload**: `wrk -t2 -c32 -d6s http://192.168.122.10:8080/` (**51,723.89 req/sec**, avg latency **642.85 us**).
- **`perf stat` (`velda-edge`)**: `14.96B` cycles, `15.00B` instructions (**1.00 IPC**), `23.17M` cache-misses, `22,726` context-switches, `1` cpu-migration.
- **OTel Collector (`:4317` gRPC)**: Decoded `ResourceSpans` and `Span` batches cleanly, **0 WARN/ERROR** in `velda-edge`.
