# Resource Limit Plugin (`resource_limit`) — Test Specification

This document describes the test methodology, automated unit test suites, adversarial security validation, and boundary verification for the **`resource_limit`** pre-route plugin.

---

## 1. Test Architecture & Scope

The `resource_limit` test suite validates three core functional areas:
1. **URI Boundary Enforcement (`plugin.rs`)**:
   - Standard path + query length enforcement (`max_uri_length`).
   - Absolute-form URI authority overflow defense (`http://long-authority.com/path`).
   - Validating `414 URI Too Long` response payload, `Content-Type`, and `Cache-Control: no-store`.
2. **Header Ingress Protection & Evasion Defense (`plugin.rs`)**:
   - Header count thresholds (`max_headers_count`).
   - Cumulative wire framing byte thresholds (`max_header_bytes`).
   - Single header value length thresholds (`max_single_header_bytes`).
   - Header Name Bombing defense (oversized header names with small values).
   - Multi-Value Header Fragmentation Bypass defense (split cookies or custom headers).
   - Validating `431 Request Header Fields Too Large` response payload and `Cache-Control: no-store`.
3. **Declarative Configuration & Semantic Validation (`velda-sync`)**:
   - Range validation for all 4 parameters (`max_uri_length`, `max_headers_count`, `max_header_bytes`, `max_single_header_bytes`).
   - Invariant checking: `max_single_header_bytes <= max_header_bytes`.
   - Rejection of incomplete or malformed JSON objects.

---

## 2. Automated Test Catalog

| Suite / File | Test Function | Purpose / Assertion |
| :--- | :--- | :--- |
| `plugin.rs` | `test_normal_request_within_limits_passes` | Verifies clean request within limits returns `Action::Continue`. |
| `plugin.rs` | `test_uri_too_long_returns_414_with_cpdos_defense` | Verifies oversized path+query returns 414 with `Cache-Control: no-store`. |
| `plugin.rs` | `test_absolute_form_uri_authority_overflow_returns_414` | Verifies absolute-form URI authority overflow is caught and returns 414. |
| `plugin.rs` | `test_headers_count_exceeded_returns_431` | Verifies header count exceeding threshold returns 431. |
| `plugin.rs` | `test_total_header_bytes_exceeded_returns_431` | Verifies cumulative wire framing byte size exceeding threshold returns 431. |
| `plugin.rs` | `test_single_header_bytes_exceeded_returns_431` | Verifies single value exceeding threshold returns 431. |
| `plugin.rs` | `test_header_name_bombing_defense_returns_431` | Verifies oversized header name with small value returns 431. |
| `plugin.rs` | `test_multi_value_header_fragmentation_bypass_returns_431` | Verifies fragmented multi-value headers aggregating beyond limit return 431. |
| `plugin.rs` | `test_from_config_custom_and_defaults` | Verifies deserialization and instantiation from JSON configuration. |
| `velda-sync` | `test_valid_resource_limits` | Verifies valid configuration parsing in `velda-sync`. |
| `velda-sync` | `test_single_header_larger_than_total_rejected` | Rejects configs where `max_single_header_bytes > max_header_bytes`. |
| `velda-sync` | `test_out_of_bounds_uri_length_rejected` | Rejects out-of-bounds `max_uri_length` values. |
| `velda-sync` | `test_missing_required_fields_rejected` | Rejects configs missing mandatory threshold fields. |

---

## 3. Running Test Suites

Execute all tests for the `resource_limit` plugin and sync validator:

```bash
# Run unit tests for velda-plugin resource_limit
cargo test -p velda-plugin --lib pre_route::resource_limit

# Run unit tests for velda-sync resource_limit validator
cargo test -p velda-sync --lib post_sync::plugin::pre_route::resource_limit

# Run full monorepo test suite (1066 tests)
cargo test
```
