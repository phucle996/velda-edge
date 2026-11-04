# Standard Stream gRPC Access Log Plugin (`std_grpc_access_log`) — Test Specification

---

## 1. Test Architecture

The `std_grpc_access_log` test suite validates functional correctness, vector register copy integrity, status-aware sampling precision, SPSC ring buffer lock-free semantics, and single-syscall batch draining across four test dimensions:

```mermaid
flowchart TD
    subgraph UnitCatalog["Engine Unit Tests (crates/velda-plugin/src/post_response/std_grpc_access_log/)"]
        U1["test_grpc_record_memory_layout (256B, 64B align)"]
        U2["test_grpc_record_json_serialization"]
        U3["test_grpc_record_error_text_serialization"]
        U4["test_grpc_simd_copy_integrity (AVX2, SSE2, SWAR)"]
        U5["test_grpc_sampling_numeric_and_names (0..16, OK)"]
        U6["test_grpc_sampling_invalid_key_rejected"]
        U7["test_grpc_spsc_ring_buffer_fifo"]
        U8["test_grpc_ring_buffer_saturation_fail_open"]
        U9["test_grpc_access_log_extracts_rpc_fields"]
        U10["test_grpc_error_with_message_extracted"]
    end

    subgraph IntegrationCatalog["Integration Test Suite (crates/velda-plugin/tests/std_grpc_access_log_test.rs)"]
        I1["test_grpc_status_aware_sampling_exact_codes_and_names"]
        I2["test_grpc_simd_record_register_harmony"]
        I3["test_grpc_post_response_pipeline_execution"]
        I4["test_grpc_unsampled_status_invisible_on_hot_path"]
        I5["test_grpc_ring_buffer_saturation_drops_fail_open"]
        I6["test_grpc_plugin_instantiate_from_config_success"]
        I7["test_grpc_plugin_instantiate_rejects_file_target"]
        I8["test_grpc_plugin_instantiate_rejects_wildcard_status"]
    end

    subgraph SyncCatalog["Config Schema Validation (crates/velda-sync/src/post_sync/plugin/post_response/std_grpc_access_log.rs)"]
        S1["test_valid_std_grpc_access_log"]
        S2["test_grpc_wildcard_rejected"]
        S3["test_grpc_file_target_rejected"]
        S4["test_grpc_status_code_out_of_range"]
    end
```

---

## 2. Reproduction Runbook

### Run All `std_grpc_access_log` Tests
```bash
# 1. Unit tests
cargo test -p velda-plugin --lib post_response::std_grpc_access_log

# 2. Integration tests
cargo test -p velda-plugin --test std_grpc_access_log_test

# 3. Declarative config validation tests
cargo test -p velda-sync --lib post_sync::plugin::post_response::std_grpc_access_log
```
