# StatsD Exporter — Test Suite & Verification Specification

This document details the test strategy, verification matrix, and automated test catalog for the **StatsD / DogStatsD Metrics Exporter Plugin**.

---

## 1. Test Strategy & Coverage Matrix

| Test Category | Target Component | Verification Objective | Source File |
| :--- | :--- | :--- | :--- |
| **Unit Testing** | `fast_write_u64`, `fast_write_i64` | 2-digit LUT, boundary values (`0`, `9`, `42`, `u64::MAX`, negative `i64`) | `tests/statsd_exporter_test.rs` |
| **Unit Testing** | `format_statsd_snapshot` (DogStatsD) | Metric prefix, tag syntax (`#downstream:`, `status:`), gauge/counter types | `tests/statsd_exporter_test.rs` |
| **Unit Testing** | `format_statsd_snapshot` (Standard) | Dot-notation formatting, zero `#tags`, status suffix (`.2xx`, `.5xx`) | `tests/statsd_exporter_test.rs` |
| **Unit Testing** | `StatsdBatchWriter` | Strict MTU bounding, zero line slicing across datagram boundaries | `tests/statsd_exporter_test.rs` |
| **Integration Testing**| `StatsdExporterPlugin` | End-to-end background loop streaming to live UDP receiver socket | `tests/statsd_exporter_test.rs` |
| **Hardware Profiling** | Disassembly & RDTSC | Machine cycles for fast path, multi-digit LUT, and complete snapshots | `tests/statsd_exporter_machine_code_test.rs` |
| **Co-existence Testing**| Frontline vs Shadow Worker | A/B comparative traffic throughput under active UDP streaming | `tests/statsd_exporter_machine_code_test.rs` |

---

## 2. Test Execution Commands

### Execute Unit & Integration Tests
```bash
cargo test -p velda-plugin --test statsd_exporter_test
```

### Execute Hardware RDTSC Cycle & A/B Benchmark Test
```bash
cargo test -p velda-plugin --test statsd_exporter_machine_code_test --release -- --nocapture
```

### Execute Automated Analysis Harness
```bash
crates/velda-plugin/src/background/statsd_exporter/harness/run_machine_code_analysis.sh
```

---

## 3. Automated Invariant Assertions

1. **Zero Dynamic Allocation**: Scratch buffers are pre-allocated once and reused across all ticks.
2. **Strict MTU Limit**: Packets emitted by `StatsdBatchWriter` never exceed `max_packet_size` bytes.
3. **Zero Partial Metric Lines**: Every UDP datagram terminates cleanly with a newline character (`\n`).
4. **Clean Socket Deallocation**: Invokes `velda_core::global_socket_manager().release_shadow_udp()` upon termination.
