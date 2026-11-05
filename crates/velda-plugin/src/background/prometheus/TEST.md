# Prometheus Telemetry Exporter Plugin (`prometheus`) — Test Verification Matrix

This document defines the test suite coverage, invariants verification, and edge-case validation for the **Prometheus Exporter** plugin.

---

## 1. Test Coverage Overview

The test harness exercises both the serialization formatter (`formatter.rs`) and the management network server (`server.rs`).

| Module | Test Target | Verification Objectives | Status |
| :--- | :--- | :--- | :---: |
| **`formatter::tests`** | `test_fast_write_u64_boundary` | 0, 1-digit (9), 2-digit (10, 99), 3-digit (100), 4-digit (1234), `u64::MAX` boundary checks | **PASS** |
| **`formatter::tests`** | `test_fast_write_i64` | Negative sign handling, zero, positive integers | **PASS** |
| **`formatter::tests`** | `test_format_snapshot_structure` | OpenMetrics HELP/TYPE lines, metric keys, pool metrics, histogram buckets | **PASS** |
| **`formatter::tests`** | `test_format_snapshot_overflow_guard` | Strict `max_buffer_size_bytes` enforcement returning `PluginError::BufferOverflow` | **PASS** |
| **`server::tests`** | `test_exporter_plaintext_scrape` | HTTP/1.1 `GET /metrics` returns 200 OK with valid Prometheus text body | **PASS** |
| **`server::tests`** | `test_exporter_healthz_endpoint` | HTTP/1.1 `GET /healthz` returns 200 OK `OK\n` | **PASS** |
| **`server::tests`** | `test_exporter_authentication_rejection` | Unauthorized scrape requests receive 401 Unauthorized with `WWW-Authenticate` header | **PASS** |
| **`server::tests`** | `test_exporter_cors_preflight` | `OPTIONS` requests receive 204 No Content with configured `Access-Control-*` headers | **PASS** |
| **`server::tests`** | `test_exporter_concurrency_limit` | Exceeding `max_concurrent_scrapes` triggers HTTP 503 or graceful rejection | **PASS** |

---

## 2. Invariants & Compliance Verification

1. **OpenMetrics Compliance**:
   - Each metric family contains exactly one `# HELP` and `# TYPE` header preceding sample values.
   - Histogram series strictly format cumulative `_bucket{le="..."}`, followed by `_count`.
   - Trailing newline `\n` is maintained on all metric lines.
2. **Deterministic Overflow Protection**:
   - Buffer bounds checking prevents runaway heap growth if the buffer exceeds the configured threshold.
3. **Pure Integer Integrity**:
   - Status counters, connection counters, and histogram bucket counts strictly reflect pre-aggregated 64-bit integer values without floating-point conversions.
