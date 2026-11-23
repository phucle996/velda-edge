# Verification & Test Report: `otlp_metrics_exporter`

---

## 1. Verified Automated Tests

### Commands Run
```bash
cargo test -p velda-plugin --lib background::otlp_metrics_exporter
cargo test -p velda-plugin --test otlp_suite_test --test otlp_metrics_chaos_test
```

### Observed Results
- **Unit tests (`background::otlp_metrics_exporter`) — 6/6 passed**:
  - `json::template::tests::test_otlp_json_template_reuse` ... **ok**
  - `json::template::tests::test_format_otlp_metrics_json` ... **ok**
  - `proto::template::tests::test_format_otlp_metrics_proto` ... **ok**
  - `proto::template::tests::test_proto_template_reuse` ... **ok**
  - `validation::tests::test_valid_otlp_metrics_exporter_config` ... **ok**
  - `validation::tests::test_invalid_otlp_metrics_exporter_config` ... **ok**
- **Integration tests**: `otlp_suite_test` (**10/10** passed) and `otlp_metrics_chaos_test` (**4/4** passed).

---

## 2. Isolated KVM Live Verification (`velda-gw` -> `velda-obs` OTel Collector `0.120.0`)

- **Workload**: `wrk -t2 -c32 -d6s http://192.168.122.10:8080/` (**52,037.37 req/sec**, avg latency **666.04 us**).
- **`perf stat` (`velda-edge`)**: `14.76B` cycles, `14.94B` instructions (**1.01 IPC**), `23.90M` cache-misses, `15,743` context-switches, `0` cpu-migrations.
- **OTel Collector (`:4317` gRPC)**: Decoded `ResourceMetrics` and `Metric` batches cleanly, **0 WARN/ERROR** in `velda-edge`.
