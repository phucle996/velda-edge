# Verification & Test Report: `statsd_exporter`

---

## 1. Verified Automated Tests

### Commands Run
```bash
cargo test -p velda-plugin --lib background::statsd_exporter
cargo test -p velda-plugin --test statsd_exporter_test --test statsd_exporter_chaos_test
```

### Observed Results
- **Unit tests (`background::statsd_exporter`) — 2/2 passed**:
  - `validation::tests::test_valid_statsd_exporter_config_and_defaults` ... **ok**
  - `validation::tests::test_invalid_statsd_exporter_config` ... **ok**
- **Integration tests**: `statsd_exporter_test` (**7/7** passed) and `statsd_exporter_chaos_test` (**5/5** passed).

---

## 2. Isolated KVM Live Verification (`velda-gw` -> `velda-obs` `prom/statsd-exporter:v0.28.0`)

- **Workload**: `wrk -t2 -c32 -d6s http://192.168.122.10:8080/` (**52,946.94 req/sec**, avg latency **623.27 us**).
- **`perf stat` (`velda-edge`)**: `15.32B` cycles, `15.67B` instructions (**1.02 IPC**), `19.37M` cache-misses, `17,127` context-switches, `0` cpu-migrations.
- **StatsD Exporter (`:8125` / `:9102`)**: `statsd_exporter_events_total{type="counter"} = 18162`, `type="gauge" = 1393`, DogStatsD tags parsed cleanly, **0 WARN/ERROR** in `velda-edge`.
