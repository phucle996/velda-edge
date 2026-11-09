# Prometheus Telemetry Exporter Plugin (`prometheus_exporter`) — Test Specification

This document details the automated unit test catalog, end-to-end integration test suite, OpenMetrics compliance assertions, and reproduction instructions for the **`prometheus_exporter`** background plugin in Velda Edge.

---

## 1. Test Architecture & Coverage Overview

The test harness exercises both the serialization formatter (`formatter.rs`) and the shadow management server (`server.rs`), verifying lock-free telemetry formatting, concurrency bounds, authentication, and buffer capacity guards.

```text
┌────────────────────────────────────────────────────────────────────────┐
│               PROMETHEUS EXPORTER TEST VERIFICATION SUITE              │
├───────────────────────────────────┬────────────────────────────────────┤
│ Unit Tests (formatter.rs)         │ Integration Tests (tests/)         │
├───────────────────────────────────┼────────────────────────────────────┤
│ • test_fast_write_u64_boundary    │ • test_exporter_basic_metrics      │
│ • test_fast_write_i64             │ • test_exporter_bearer_auth        │
│ • test_format_snapshot_structure  │ • test_exporter_cors_preflight     │
│ • test_format_snapshot_overflow   │ • test_exporter_concurrency_limit  │
└───────────────────────────────────┴────────────────────────────────────┘
```

---

## 2. Test Catalog & Assertion Specification

### 2.1 Serialization Formatter Unit Tests (`formatter.rs`)

| Test Function | Target Module | Scope & Input Values | Verification Assertions | Invariant Validated |
| :--- | :--- | :--- | :--- | :---: |
| `test_fast_write_u64_boundary` | `formatter.rs` | Values: `0`, `9`, `10`, `99`, `100`, `999`, `1234`, `u64::MAX` | Buffer matches exact decimal ASCII string; zero trailing garbage | 2-Digit LUT (`DIGIT_PAIRS`) Boundary Correctness |
| `test_fast_write_i64` | `formatter.rs` | Values: `0`, `123`, `-1`, `-456`, `i64::MIN`, `i64::MAX` | Prepends `-` for negative values; matches standard integer output | Signed Integer Zero-Alloc Serialization |
| `test_format_snapshot_structure` | `formatter.rs` | Populated `MetricsSnapshot` (pipelines, pool metrics, histograms) | Generates valid `# HELP` and `# TYPE` headers, cumulative histogram buckets `le="..."`, and trailing newline | OpenMetrics / Prometheus 0.0.4 Standard Compliance |
| `test_format_snapshot_overflow_guard` | `formatter.rs` | Snapshot rendered into buffer with `max_buffer_size_bytes = 100` | Returns `Err(PluginError::BufferOverflow)` immediately; halts writes | Adversarial Memory Exhaustion Protection |

---

### 2.2 Shadow Server Integration Tests (`tests/prometheus_exporter_test.rs`)

| Test Function | Target Interface | Test Scenario | Verification Assertions | Invariant Validated |
| :--- | :--- | :--- | :--- | :---: |
| `test_exporter_basic_metrics_and_healthz` | HTTP/1.1 Plaintext | Sends `GET /metrics` and `GET /healthz` | `/metrics` returns `200 OK` with `Content-Type: text/plain`; `/healthz` returns `200 OK OK\n` | Scrape & Liveness Multiplexing |
| `test_exporter_bearer_auth` | HTTP/1.1 Auth | Requests with missing, invalid, and correct `Authorization: Bearer <token>` | Missing/invalid returns `401 Unauthorized`; valid returns `200 OK` | Constant-Time Zero-Alloc Auth Verification |
| `test_exporter_cors_preflight_and_headers` | HTTP/1.1 CORS | Sends `OPTIONS /metrics` with `Origin: https://grafana.internal` | Returns `204 No Content` with `Access-Control-Allow-Origin: https://grafana.internal` | W3C CORS Specification Compliance |
| `test_exporter_concurrency_limiting` | Semaphore Bounding | Dispatches concurrent scrape connections exceeding `max_concurrency` | Surplus connections receive `503 Service Unavailable`; zero task leakage | Anti-DoS Semaphore Bounding |

---

## 3. Reproduction & Execution Instructions

### 3.1 Run All Prometheus Exporter Tests
```bash
# Execute unit tests inside the formatter module
cargo test -p velda-plugin background::prometheus_exporter

# Execute full shadow server integration tests
cargo test -p velda-plugin --test prometheus_exporter_test -- --nocapture
```

### 3.2 Live Manual Verification via `curl`
```bash
# Start background server on local port 9090
cargo run -p velda-edge

# Verify OpenMetrics text output
curl -s http://127.0.0.1:9090/metrics | head -n 30

# Verify liveness probe
curl -i http://127.0.0.1:9090/healthz

# Verify CORS Preflight
curl -i -X OPTIONS \
  -H "Origin: https://grafana.internal" \
  -H "Access-Control-Request-Method: GET" \
  http://127.0.0.1:9090/metrics
```
