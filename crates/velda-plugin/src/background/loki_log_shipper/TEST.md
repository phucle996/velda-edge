# Automated Test Suite & Quality Assurance Specification: Grafana Loki Log Shipper (`loki_log_shipper`)

This document specifies the automated testing matrix, unit tests, integration tests, machine code profiling harnesses, and failure recovery invariant tests for the `loki_log_shipper` background logging plugin.

---

## 1. Test Architecture & Execution Levels

The testing strategy is structured into three orthogonal verification tiers:
1. **Tier 1: Multi-Protocol Stream Partitioning Tests**: Validates that access logs are partitioned accurately into `http`, `grpc`, and `raw` buckets and conform to Loki `/loki/api/v1/push` syntax.
2. **Tier 2: End-to-End Push Integration Tests (`test_loki_log_shipper_multi_protocol_end_to_end`)**: Spawns an in-memory mock Loki server, drains records from `LogStore`, sends push requests over HTTP/1.1, and asserts that multi-tenancy headers (`X-Scope-OrgID`) and stream labels match expectations.
3. **Tier 3: Machine Code & Hardware Cycle Harness (`loki_log_shipper_machine_code_test`)**: Measures hardware RDTSC execution cycles, asserts zero heap allocations during multi-stream formatting, and validates multi-threaded frontline worker co-existence.

---

## 2. Test Catalog & Coverage Matrix

| Test Suite File | Test Identifier | Target Functionality | Verification Criteria |
| :--- | :--- | :--- | :--- |
| [`plugin.rs`](./plugin.rs) | `test_loki_log_shipper_multi_protocol_end_to_end` | Multi-protocol end-to-end export | Verifies HTTP, gRPC, and Raw partitioning, `X-Scope-OrgID` header, and persistent connection reuse |
| [`machine_code_test.rs`](./harness/machine_code_test.rs) | `test_runtime_machine_code_execution_cycles` | Real hardware cycle consumption | Uses Intel/AMD `RDTSC` instructions to assert Loki multi-stream formatting < 600 cycles/rec in release mode |
| [`machine_code_test.rs`](./harness/machine_code_test.rs) | `test_ab_loki_log_shipper_hot_path_impact` | Multi-core frontline co-existence | 4 simulated frontline worker threads emit records at full speed while shadow worker drains without lock contention |

---

## 3. Reproduction Instructions

### 3.1 Executing the Standard Integration Suite
```bash
cargo test -p velda-plugin test_loki_log_shipper_multi_protocol_end_to_end
```

### 3.2 Executing the Hardware Machine Code Cycle Profiler
```bash
cargo test --release -p velda-plugin --test loki_log_shipper_machine_code_test -- --nocapture
```

### 3.3 Running Disassembly & Symbol Verification Script
```bash
bash crates/velda-plugin/src/background/loki_log_shipper/harness/run_machine_code_analysis.sh
```

---

## 4. Invariant Verification & Negative Assertions

1. **Empty Batch Suppression**: When no new records have been emitted to `LogStore`, `format_loki_push_payload` returns `0` and the plugin completely suppresses HTTP transmission, avoiding empty network chatter.
2. **Cardinality Safety Invariant**: High-cardinality fields are strictly banned from stream labels. Any attempt to introduce per-request identifiers into labels fails configuration validation at sync time.
