# Automated Test Suite & Quality Assurance Specification: Grafana Loki Log Shipper (`loki_log_shipper`)

This document specifies the automated testing matrix, unit tests, integration suites, machine code profiling harnesses, and failure recovery invariant tests for the `loki_log_shipper` background logging plugin across both **HTTP JSON** and **gRPC Protobuf** transports.

---

## 1. Test Architecture & Execution Levels

The testing strategy is structured into four orthogonal verification tiers:
1. **Tier 1: Low-Level Codec & LUT Unit Tests**: Validates division-free integer formatting, SSSE3 SIMD 32-character lowercase hex Request ID generation, and branchless RFC 5952 IPv6 stack formatting.
2. **Tier 2: Zero-Allocation Formatter Tests**: Asserts that `LokiJsonFormatter` persistent scratch buffers and `LokiProtoFormatter` direct wire Protobuf encoders execute without dynamic heap allocations during steady-state cycles.
3. **Tier 3: End-to-End Push Integration Tests (`tests/loki_suite_test.rs`)**:
   - **HTTP/1.1 JSON Push**: Spawns mock HTTP server, validates multi-stream partitioning (`http`, `grpc`, `raw`), `X-Scope-OrgID` tenant header, 32-hex request IDs, and RFC 5952 zero-compressed IPs.
   - **HTTP/2 gRPC Push**: Spawns mock HTTP/2 gRPC server on `/logproto.Pusher/Push`, validates 5-byte canonical framing `[0x00, len_u32_be]`, `logproto.PushRequest` decode parity, and `x-scope-orgid` metadata.
4. **Tier 4: Machine Code & Hardware Cycle Harness (`loki_log_shipper_machine_code_test`)**: Measures hardware RDTSC execution cycles and validates multi-threaded frontline worker co-existence.

---

## 2. Test Catalog & Coverage Matrix

| Test Location | Test Function | Target Functionality | Verification Criteria |
| :--- | :--- | :--- | :--- |
| [`lut.rs`](./lut.rs) | `test_fast_write_hex32_exact_32_chars` | 128-bit Request ID hex formatting | Exactly 32 lowercase hex characters matching `(ts << 64) \| id` |
| [`lut.rs`](./lut.rs) | `test_fast_write_ipv6_rfc5952` | Branchless IPv6 zero-compression | Correct `::1`, loopback, and zero-run compression on `[u8; 40]` stack |
| [`json/template.rs`](./json/template.rs) | `test_loki_json_formatter_scratch_zero_alloc` | Persistent scratch buffer reuse | 0 new heap allocations across repeated flush cycles |
| [`proto/template.rs`](./proto/template.rs) | `test_proto_wire_formatter_decodes_cleanly` | Direct wire Protobuf encoder | Decodes cleanly via Prost `PushRequest::decode` with matching labels and entries |
| [`tests/loki_suite_test.rs`](../../../../tests/loki_suite_test.rs) | `test_loki_log_shipper_multi_protocol_end_to_end` | HTTP JSON end-to-end delivery | HTTP/1.1 POST to `/loki/api/v1/push`, `X-Scope-OrgID`, multi-stream |
| [`tests/loki_suite_test.rs`](../../../../tests/loki_suite_test.rs) | `test_loki_log_shipper_grpc_end_to_end` | gRPC Protobuf end-to-end delivery | HTTP/2 POST to `/logproto.Pusher/Push`, 5-byte framing, `x-scope-orgid` |
| [`tests/loki_chaos_test.rs`](../../../../tests/loki_chaos_test.rs) | `test_chaos_request_id_boundaries` | Request ID extreme boundary values | id=0, id=u64::MAX, alternating bit patterns, 32-hex lowercase check |
| [`tests/loki_chaos_test.rs`](../../../../tests/loki_chaos_test.rs) | `test_chaos_ip_addresses_rfc4291_and_rfc5952` | RFC 4291 IPv4 & RFC 5952 IPv6 chaos | 0.0.0.0, 255.255.255.255, ::1, ::, zero-run compression, full 8-hextet |
| [`tests/loki_chaos_test.rs`](../../../../tests/loki_chaos_test.rs) | `test_chaos_protocol_combinations_multi_stream_partitioning` | 7 interleaved protocols partition | 700 records across H1/H2/H3/gRPC/Raw partitioned into exact 3 buckets |
| [`tests/loki_chaos_test.rs`](../../../../tests/loki_chaos_test.rs) | `test_chaos_round_trip_decoding_and_syntax_validation` | Full round-trip syntax validation | serde_json deep AST validation and Prost PushRequest binary decode |
| [`tests/loki_chaos_test.rs`](../../../../tests/loki_chaos_test.rs) | `test_chaos_stress_scratch_buffers_10k_records` | Scratch buffer stress testing | 10,000 records across 100 cycles, 0 reallocations, 100% data integrity |
| [`harness/machine_code_test.rs`](./harness/machine_code_test.rs) | `test_runtime_machine_code_execution_cycles` | Hardware execution latency | RDTSC cycle benchmark verifying multi-stream formatting speed |
| [`harness/machine_code_test.rs`](./harness/machine_code_test.rs) | `test_ab_loki_log_shipper_hot_path_impact` | Frontline/Shadow co-existence | 4 frontline workers emit records while shadow worker drains without lock contention |

---

## 3. Reproduction Instructions

### 3.1 Executing the Chaos & Extreme Edge-Case Suite
```bash
RUSTUP_TOOLCHAIN=stable cargo test -p velda-plugin --test loki_chaos_test -- --nocapture
```

### 3.2 Executing the Full Loki Test Suite
```bash
RUSTUP_TOOLCHAIN=stable cargo test -p velda-plugin --test loki_suite_test
```

### 3.2 Executing Codec Unit Tests
```bash
RUSTUP_TOOLCHAIN=stable cargo test -p velda-plugin loki_log_shipper
```

### 3.3 Executing Hardware Machine Code Cycle Profiler
```bash
RUSTUP_TOOLCHAIN=stable cargo test --release -p velda-plugin --test loki_log_shipper_machine_code_test -- --nocapture
```

### 3.4 Monorepo Verification Commands
```bash
RUSTUP_TOOLCHAIN=stable cargo check
RUSTUP_TOOLCHAIN=stable cargo clippy -p velda-plugin --all-targets -- -D warnings
RUSTUP_TOOLCHAIN=stable cargo fmt --check
```

---

## 4. Invariant Verification & Negative Assertions

1. **Empty Batch Suppression**: When no new records have been emitted to `LogStore`, formatting returns `0` and network transmission is completely bypassed.
2. **Cardinality Safety Invariant**: High-cardinality fields (`request_id`, `peer_ip`) are strictly serialized into log line text, preserving bounded Loki index cardinality.
3. **Explicit Configuration Invariant**: Validated at sync time; unknown protocols or missing mandatory fields fail fast with `PluginError::Config`.
