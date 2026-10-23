# Canary Traffic Splitting Plugin (`canary`) — Verification & Test Matrix

This document details the test matrix, security edge case validations, and unit test coverage for the **`canary`** plugin.

---

## 1. Unit Test Coverage Matrix

| Test Suite / Function | Verification Focus | Expected Result |
| :--- | :--- | :--- |
| `test_canary_rule_based_override_and_operators` | Evaluates all 5 byte-level operators: `Equals`, `Suffix`, `Prefix`, `Contains`, `Exists`. | All matching conditions route to canary upstream (`payments-v2`). |
| `test_canary_rule_based_override_and_operators` (Spoof Sanitization) | Non-matching request with client-supplied `x-velda-canary: true` routed to baseline. | Canary header is stripped (`req.headers.remove`). |
| `test_priority_hash_sources_user_defined` | Evaluates Priority Fallback order: Header $\to$ Cookie $\to$ Query $\to$ Client IP. | Consistent deterministic percentage returned for each layer without dropping stickiness. |
| `test_composite_hash_consistency` | Compound key evaluation (`header:x-tenant-id` + `client_ip`). | Deterministic, reproducible percentage on identical request attributes. |
| `test_canary_from_config_multi_sources` | Deserialization and validation from raw `PluginConfig`. | Pre-compiles headers and operators cleanly. |
| `test_valid_canary_config_multi_sources` (`velda-sync`) | Verifies valid configuration schema with multi-source list. | Passes validation (`Ok(cfg)`). |
| `test_bare_header_rejected_to_prevent_hardcoded_defaults` (`velda-sync`) | Attempting to pass bare `"header"` or `"cookie"` without target name. | Rejected with explicit error (`SyncError::Validation`). |
| `test_control_character_in_header_value_rejected` (`velda-sync`) | Header value with `\r\n` CRLF injection characters. | Rejected with control character validation error. |

---

## 2. Security Test Scenarios

### Scenario A: Canary Marker Header Spoofing Prevention
```rust
let mut req_spoof = L7Request::new(
    Method::GET,
    Uri::from_static("/v1/pay"),
    Version::HTTP_11,
    {
        let mut h = HeaderMap::new();
        h.insert("x-velda-canary", HeaderValue::from_static("true"));
        h
    },
    Body::Empty,
);
let mut target_base = Arc::from("payments-v1");
plugin.on_upstream_request(&mut req_spoof, &mut target_base);
assert_eq!(target_base.as_ref(), "payments-v1");
assert!(
    !req_spoof.headers.contains_key("x-velda-canary"),
    "Spoofed canary marker must be stripped on baseline path!"
);
```

### Scenario B: Anti-Spoofing Client IP Resolution
```rust
// Verified that leftmost XFF manipulation does not control hash when x-real-ip is authoritative
let mut headers = HeaderMap::new();
headers.insert("x-forwarded-for", HeaderValue::from_static("spoofed.ip, 192.168.1.10"));
headers.insert("x-real-ip", HeaderValue::from_static("10.0.0.1"));
let req = L7Request::new(Method::GET, Uri::from_static("/"), Version::HTTP_11, headers, Body::Empty);
assert_eq!(extract_authoritative_client_ip(&req), Some("10.0.0.1"));
```

---

## 3. Test Execution Verification

```bash
# Plugin Unit Tests
cargo test -p velda-plugin canary -- --nocapture

# Config & Schema Validation Tests
cargo test -p velda-sync canary -- --nocapture

# Monorepo Clean Build & Test Verification
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
All **1,074** monorepo tests pass with **0 warnings**.
