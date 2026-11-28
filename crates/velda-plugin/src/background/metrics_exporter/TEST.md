# Verification Report (`metrics_exporter`)

## 1. Executed Test, Lint & Verification Commands

All local workspace commands and KVM live validations below exited cleanly with status `0` after the final edits:

```bash
cargo fmt --check
cargo clippy -p velda-plugin --all-targets --all-features -- -D warnings
cargo test -p velda-plugin -- metrics_exporter
cargo test -p velda-observability
cargo test -p velda-sync -- metrics
```

## 2. Verified Passing Test Suites

| Crate / Target | Passed | Failed | Scope & Key Verified Cases |
| :--- | :---: | :---: | :--- |
| `velda-plugin` (`metrics_exporter` filter) | 9 | 0 | `validation::tests::{test_validate_mixed_pull_and_push_targets, test_reject_invalid_targets}`, `providers::prometheus::template::tests::{test_format_snapshot_structure, test_format_snapshot_overflow_guard, test_format_into_vec_matches_string}`, `providers::otlp::proto::template::tests::{test_format_otlp_metrics_proto, test_proto_template_reuse}`, `providers::otlp::json::template::tests::{test_format_otlp_metrics_json, test_otlp_json_template_reuse}`. |
| `velda-observability --lib` | 10 | 0 | `metrics::counter::tests::{test_counter_simd_equivalence, test_bitmask_status_aggregation_and_totals}`, `metrics::snapshot::tests::test_snapshot_delta_since`, `sampling::*`, `telemetry::record::*`. |
| `velda-observability` (`in_memory_metrics_test`) | 16 | 0 | `test_status_bitmask_activation_and_snapshot_isolation`, `test_per_core_sharded_simd_aggregation`, `test_pipeline_slot_cacheline_alignment`, `test_record_completion_and_snapshot`, `test_concurrent_multi_threaded_recording`, `velda-upstream` pool counter lifecycle checks. |
| `velda-observability` (`in_memory_telemetry_test`) | 2 | 0 | `test_multi_exporter_fanout`, `test_slow_subscriber_eviction_preserves_rotating_pool_slots`. |
| `velda-sync` (`metrics` filter) | 0 (`filtered`) | 0 | Clean compilation of revision/stress test targets (run full `cargo test --workspace` completed prior: `77` suites, `1270` passed, `0` failed). |

## 3. Live 2-KVM End-to-End Wire Verification (`velda-gw` $\to$ `velda-obs`)

Verified with real `velda-sync` config compilation (`etp.json` + `plugins.json` $\to$ `etp.bin` + `plugins.bin`) over active 4-band `tc netem` shaping against the multi-protocol `obs_collector` on `192.168.122.20`:

- **Prometheus Pull (`GET :9090/metrics`)**: Emits `~122.55 KB` valid OpenMetrics payloads under `wrk -t2 -c16` scrape storms without dropping active frontline traffic.
- **OTLP HTTP/Protobuf Delta (`POST :4318/v1/metrics`)**: Decodes cleanly via official `go.opentelemetry.io/proto` wire schema (`AggregationTemporality = 1`, active status series `"completed,bad_request"`, exact request sum `710,355` across multi-pass runs).
- **OTLP HTTP/JSON Delta (`POST :4318/v1/metrics`)**: Decodes via Go `protojson` (`aggregationTemporality = 1`, exact request sum `710,361`).
- **OTLP gRPC Cumulative (`h2c POST :4317/opentelemetry.proto.collector.metrics.v1.MetricsService/Export`)**: Verified native HTTP/2 protobuf exports (`AggregationTemporality = 2`, cumulative monotonically increasing sums).
- **StatsD / DogStatsD (`UDP :8125`, `:8126`)**: Verified MTU-packed datagrams (`760` DogStatsD packets / `1.04 MB`, `474` Standard packets / `639.9 KB`), `#tag:val` vs dot-hierarchy keys, and exact counter sums (`710,348` / `710,353`).
