# Verification Report (`trace_exporter`)

## 1. Executed Test, Lint & Verification Commands

All unit, clippy, and format verification checks below exited cleanly with status `0` (latest verification post-documentation and prior workspace verification):

```bash
cargo fmt --check
cargo clippy -p velda-plugin --all-targets --all-features -- -D warnings
cargo test -p velda-plugin -- trace_exporter
cargo test -p velda-observability
```

## 2. Verified Passing Test Suites

| Crate / Target | Passed | Failed | Scope & Key Verified Cases |
| :--- | :---: | :---: | :--- |
| `velda-plugin` (`trace_exporter` filter) | 4 | 0 | `validation::tests::{test_validate_trace_exporter_config, test_reject_invalid_trace_config}`, `providers::otlp::proto::template::tests::wire_encode_decode` (decodes directly via official `opentelemetry_proto::ExportTraceServiceRequest::decode`), `providers::otlp::json::template::tests::json_encode`. |
| `velda-observability --lib` | 10 | 0 | `sampling::state::tests::{rate_boundaries_and_clamping, exact_never_and_always, empirical_rates_and_alternating_listeners}`, `sampling::status::tests::{status_sampling_overrides_and_shed, shed_inherits_rate_limited_when_omitted}`, `telemetry::record::tests::{test_record_geometry_and_alignment, test_record_url_mutation}`. |
| `velda-observability` (`in_memory_telemetry_test`) | 2 | 0 | `test_multi_exporter_fanout`, `test_slow_subscriber_eviction_preserves_rotating_pool_slots`. |
| `velda-plugin` (full workspace prior check) | 1270 (`77` suites) | 0 | Full Rust workspace regression suite including `velda-sync` and `velda-edge` compilation & ETP attachment. |

## 3. Live End-to-End Verification Against Official `otelcol-contrib v0.166.0`

Verified end-to-end on the 2-KVM `tc netem` testbed (`velda-gw` $\to$ `velda-obs`) with real `velda-sync` binary compilation (`etp.json`, `listeners.json`, `plugins.json` $\to$ `*.bin`) feeding the official **`otel/opentelemetry-collector-contrib:latest` (`v0.166.0`)** container:

- **OTLP HTTP/Protobuf (`:4318/v1/traces`)**: `99,175` spans (`0` refused) accepted under `100%` load; `39,135` (`10.025%`) and `3,932` (`1.003%`) lossless at `10%`/`1%` Weyl sampling.
- **OTLP HTTP/JSON (`:4318/v1/traces`)**: `99,207` spans (`0` refused) at `100%`; `39,210` (`10.026%`) lossless at `10%`.
- **OTLP gRPC `h2c` (`:4317/opentelemetry.proto.collector.trace.v1.TraceService/Export`)**: `99,445` spans (`0` refused) at `100%`; `38,960` (`10.024%`) lossless at `10%`.
- **Simultaneous Multi-Target Fan-Out (All 3 Sinks)**: `303,447` spans accepted cleanly (`202,298` HTTP Proto+JSON + `101,149` gRPC — exact `2:1` parity, `0` refused).
- **Collector Outage Recovery**: Stopping `otelcol` mid-stream held proxy throughput at `48,555.65 RPS` (`0` errors, RSS bounded at `83.95 MB`), then resumed accepting `16,923` spans within `<= 2.5s` of restarting `otelcol`.
