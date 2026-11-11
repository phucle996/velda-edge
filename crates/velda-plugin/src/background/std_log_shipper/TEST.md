# Automated Test Suite & Quality Assurance Specification: Standard Log Shipper (`std_log_shipper`)

This document specifies the automated testing matrix, unit tests, integration tests, machine code profiling harnesses, and chaos test suites for the `std_log_shipper` background logging plugin.

---

## 1. Test Architecture & Execution Levels

The testing strategy is structured into four orthogonal verification tiers:
1. **Tier 1: Unit & Codec Tests**: Verifies division-free integer formatting, hex serialization, IP address conversion, JSON field escaping, and text formatting.
2. **Tier 2: Multi-Protocol End-to-End Integration Tests**: Spawns in-memory `LogStore`, registers `StdLogShipperPlugin`, injects HTTP/1, HTTP/2, HTTP/3, gRPC, and Raw L4 records, and validates output byte sequences.
3. **Tier 3: Machine Code & Hardware Cycle Harness (`std_log_shipper_machine_code_test`)**: Measures hardware RDTSC execution cycles, asserts zero heap allocations, and validates multi-threaded frontline worker co-existence.
4. **Tier 4: Chaos & Extreme Edge-Case Suite (`std_log_shipper_chaos_test`)**: Stress-tests scalar boundary conditions, 32-char hex formatting, RFC 5952 IPv6 compression permutations, combinatorial protocol matrices, 100% JSON round-trip deserialization, and 10,000-record persistent file sink stress.

---

## 2. Test Catalog & Coverage Matrix

| Test Suite File | Test Identifier | Target Functionality | Verification Criteria |
| :--- | :--- | :--- | :--- |
| [`std_log_shipper_test.rs`](../../../tests/std_log_shipper_test.rs) | `test_std_log_shipper_formatting_all_protocols` | Multi-protocol format serialization | Asserts exact field matching across HTTP, gRPC, and Raw L4 records in both JSON and Text modes |
| [`std_log_shipper_test.rs`](../../../tests/std_log_shipper_test.rs) | `test_std_log_shipper_from_plugin_config` | Validation and instantiation from config | Proves that declarative `PluginConfig` parses accurately into typed runtime entities |
| [`std_log_shipper_test.rs`](../../../tests/std_log_shipper_test.rs) | `test_std_log_shipper_background_subscription_lifecycle` | End-to-end background execution | Emits 10 records across SPSC rings, verifies asynchronous flush and graceful shutdown via watch channel |
| [`machine_code_test.rs`](./harness/machine_code_test.rs) | `test_runtime_machine_code_execution_cycles` | Real hardware cycle consumption | Uses Intel/AMD `RDTSC` instructions to assert JSON < 500 cycles/rec and Text < 500 cycles/rec in release mode |
| [`machine_code_test.rs`](./harness/machine_code_test.rs) | `test_ab_std_log_shipper_hot_path_impact` | Multi-core frontline co-existence | 4 simulated frontline worker threads emit records at full speed while shadow worker drains without lock contention |
| [`std_log_shipper_chaos_test.rs`](../../../tests/std_log_shipper_chaos_test.rs) | `test_chaos_request_id_boundaries_and_bit_patterns` | Extreme Request ID bit patterns | Verifies `0`, `u64::MAX`, alternating bit patterns format into exact 32 lowercase hex characters |
| [`std_log_shipper_chaos_test.rs`](../../../tests/std_log_shipper_chaos_test.rs) | `test_chaos_timestamps_duration_and_counters` | Timestamp, duration & counter extremes | Tests `ts=0`, `u32::MAX`, Year 2038, `dur=0..u32::MAX`, 32-bit rx/tx counters round-trip in JSON |
| [`std_log_shipper_chaos_test.rs`](../../../tests/std_log_shipper_chaos_test.rs) | `test_chaos_ip_addresses_rfc4291_rfc5952` | IP address boundary permutations | Verifies `0.0.0.0`, `255.255.255.255`, `::1`, `::`, multiple zero runs, tie-breaking, full 8-hextets |
| [`std_log_shipper_chaos_test.rs`](../../../tests/std_log_shipper_chaos_test.rs) | `test_chaos_combinatorial_protocols_and_statuses` | Combinatorial protocol cross-product | Evaluates all 7 protocols, 10 HTTP methods, 17 gRPC statuses, 4 strategies, 5 completions |
| [`std_log_shipper_chaos_test.rs`](../../../tests/std_log_shipper_chaos_test.rs) | `test_chaos_syntax_roundtrip_parsing_all_17_keys` | 100% JSON schema conformance | Deserializes via `serde_json::from_str`, asserting all 17 keys are present with exact typing |
| [`std_log_shipper_chaos_test.rs`](../../../tests/std_log_shipper_chaos_test.rs) | `test_chaos_persistent_file_sink_stress_10000_records` | High-load persistent sink stress | Streams 10,000 records in 100 batches to a persistent `File` descriptor, asserting zero truncation |

---

## 3. Reproduction Instructions

### 3.1 Executing the Standard Test Suite
```bash
cargo test -p velda-plugin --test std_log_shipper_test
```

### 3.2 Executing the Chaos & Extreme Edge-Case Suite
```bash
cargo test -p velda-plugin --test std_log_shipper_chaos_test -- --nocapture
```

### 3.3 Executing the Hardware Machine Code Cycle Profiler
```bash
cargo test --release -p velda-plugin --test std_log_shipper_machine_code_test -- --nocapture
```

### 3.4 Running Disassembly & Symbol Verification Script
```bash
bash crates/velda-plugin/src/background/std_log_shipper/harness/run_machine_code_analysis.sh
```

---

## 4. Invariant Verification & Negative Assertions

1. **Zero Runtime Panic Guarantee**: Formatting functions use saturating arithmetic and bounds-checked slice writing; no malformed record field can trigger an unhandled panic.
2. **Buffer Capacity Preservation**: Pre-allocated output buffers (`out: &mut Vec<u8>`) are never deallocated between flush cycles; `clear()` is invoked rather than reallocating memory.
3. **Graceful Shutdown Invariant**: Sending `shutdown_tx.send(true)` guarantees that any log records currently remaining in per-core SPSC ring buffers are completely drained and flushed prior to process exit.
4. **100% Syntactic Integrity**: Output byte streams adhere strictly to JSON and extended text specifications across all scalar permutations without delimiter displacement or escaping anomalies.
