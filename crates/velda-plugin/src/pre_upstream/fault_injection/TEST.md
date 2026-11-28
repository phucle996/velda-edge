# Verification Report (`fault_injection`)

## Executed Test Command

```bash
cargo test -p velda-plugin -- fault_injection
```

Observed result: `13 passed; 0 failed; 0 ignored; 0 measured; 414 filtered out` (`Doc-tests velda_plugin`: `0 passed; 0 failed`).

---

## Verified Unit Tests

| Test Case | Module | What It Verifies | Status |
| :--- | :--- | :--- | :--- |
| `test_ip_range_matcher_single_and_merged_cidrs` | `condition.rs` | Single CIDR lookup and merging of adjacent/disjoint IPv4 CIDRs | **PASSED** |
| `test_extract_overrides_clamps_delay_and_validates_abort_status` | `condition.rs` | Extracts `x-fault-delay-ms` (clamped) & `x-fault-abort-status`; ignores `completed`/unknown | **PASSED** |
| `test_valid_fault_injection_with_default_scope` | `validation.rs` | Default `scope` parsing (`max_override_delay_ms = 5000`) & valid `InternalStatus` | **PASSED** |
| `test_valid_fault_injection_with_scope` | `validation.rs` | Scoped IP + header match + override validation | **PASSED** |
| `test_unscoped_override_rejected` | `validation.rs` | Rejects `allow_header_override: true` without `client_ips` or `header_match` | **PASSED** |
| `test_invalid_or_completed_internal_status_rejected` | `validation.rs` | Rejects `"completed"` and invalid `InternalStatus` names | **PASSED** |
| `test_delay_exceeds_max_rejected` | `validation.rs` | Rejects `fixed_delay_ms > 60_000` | **PASSED** |
| `test_noop_and_unknown_fields_rejected` | `validation.rs` | Rejects no-op fault configs and unknown JSON fields (`deny_unknown_fields`) | **PASSED** |
| `test_abort_config_validation` | `plugin.rs` | Enforces `0..=100` probability and aborted `InternalStatus` constraint | **PASSED** |
| `test_delay_config_validation_and_bounds` | `plugin.rs` | Validates delay probability and `[min_ms, max_ms]` uniform bounds | **PASSED** |
| `test_scope_bypass_and_matrix_actions` | `plugin.rs` | Verifies scope bypass, `Action::Delay(..., Action::Status(..))`, and `x-velda-fault` bypass retention | **PASSED** |
| `test_dynamic_override_hygiene_and_protocol_support` | `plugin.rs` | Verifies `ALL_HTTP \| GRPC` support, dynamic overrides, and control-header stripping | **PASSED** |
| `test_from_config_complete_with_scope` | `plugin.rs` | End-to-end `FaultInjectionPlugin::from_config` compilation | **PASSED** |
