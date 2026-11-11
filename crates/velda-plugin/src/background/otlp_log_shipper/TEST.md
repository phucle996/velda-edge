# Automated Test Suite & Quality Assurance Specification: OpenTelemetry Log Shipper (`otlp_log_shipper`)

This document specifies the automated testing matrix, unit tests, integration tests, machine code profiling harnesses, and failure recovery invariant tests for the `otlp_log_shipper` background logging plugin.

---

## 1. Test Architecture & Execution Levels

The testing strategy is structured into three orthogonal verification tiers:
1. **Tier 1: Schema & Serializer Conformance Tests**: Validates that generated JSON and Protocol Buffer payloads strictly adhere to OpenTelemetry Collector schemas.
2. **Tier 2: Multi-Protocol End-to-End Integration Tests (`otlp_suite_test.rs`)**: Runs live mock HTTP (port 4318) and gRPC (port 4317) servers, validates persistent Keep-Alive connection reuse, and tests transparent reconnect resilience when connections close.
3. **Tier 3: Machine Code & Hardware Cycle Harness (`otlp_log_shipper_machine_code_test`)**: Measures hardware RDTSC execution cycles, asserts zero heap allocations during JSON formatting, and validates multi-threaded frontline worker co-existence.

---

## 2. Test Catalog & Coverage Matrix

| Test Suite File | Test Identifier | Target Functionality | Verification Criteria |
| :--- | :--- | :--- | :--- |
| [`otlp_suite_test.rs`](../../../tests/otlp_suite_test.rs) | `test_otlp_log_shipper_batching_and_send` | OTLP/HTTP batching and export | Verifies batching of multiple `AccessLogRecord` instances into valid JSON and transmission to mock HTTP collector |
| [`otlp_suite_test.rs`](../../../tests/otlp_suite_test.rs) | `test_otlp_logs_and_traces_grpc_export` | OTLP/gRPC binary Protobuf export | Verifies binary Protobuf encoding, 5-byte gRPC framing, and transmission over HTTP/2 |
| [`machine_code_test.rs`](./harness/machine_code_test.rs) | `test_runtime_machine_code_execution_cycles` | Real hardware cycle consumption | Uses Intel/AMD `RDTSC` instructions to assert JSON < 600 cycles/rec and Protobuf < 10,000 cycles/rec in release mode |
| [`machine_code_test.rs`](./harness/machine_code_test.rs) | `test_ab_otlp_log_shipper_hot_path_impact` | Multi-core frontline co-existence | 4 simulated frontline worker threads emit records at full speed while shadow worker drains without lock contention |

---

## 3. Reproduction Instructions

### 3.1 Executing the Standard Integration Suite
```bash
cargo test -p velda-plugin --test otlp_suite_test
```

### 3.2 Executing the Hardware Machine Code Cycle Profiler
```bash
cargo test --release -p velda-plugin --test otlp_log_shipper_machine_code_test -- --nocapture
```

### 3.3 Running Disassembly & Symbol Verification Script
```bash
bash crates/velda-plugin/src/background/otlp_log_shipper/harness/run_machine_code_analysis.sh
```

---

## 4. Invariant Verification & Negative Assertions

1. **Transparent Reconnect Invariant**: When the OpenTelemetry Collector drops the TCP socket while idle, `OtlpLogClient` re-establishes a fresh connection immediately without dropping pending batches.
2. **Collector Down Fail-Open**: If the collector endpoint is unreachable, the background task increments `failed_pushes`, drops stale batches to avoid unbounded memory accumulation, and allows frontline request serving to continue unabated.
