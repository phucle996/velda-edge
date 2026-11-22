# Web Application Firewall Lite (`waf_lite`) — Test Matrix & Verification Report

All unit and validation tests for `waf_lite` are executed via:
```bash
cargo test -p velda-plugin -- waf_lite
```

## Test Coverage Summary (19 / 19 Tests Passing)

### 1. Scanner & Anti-Evasion (`scanner.rs` — 8 Tests)
- `test_percent_decode_and_case_folding`: Validates single-pass `%2e%2e%2f` decoding and ASCII lowercasing.
- `test_double_and_triple_percent_decoding_anti_evasion`: Validates nested `%252e` and `%25252e` decoding.
- `test_form_urlencoded_plus_to_space_conversion`: Validates `+` $\to$ space normalization on query strings.
- `test_whitespace_and_inline_comment_collapsing`: Validates `UNION/*obfuscated*/SELECT` folding.
- `test_single_line_sql_comments_collapsing`: Validates `UNION--comment\nSELECT` folding.
- `test_prevents_comment_wrapping_and_parameter_smuggling_bypasses`: Verifies Stage A catches `/*<script>*/` and `/*${jndi:...}*/`, while comment folding stops at `&`, `<`, `$`.
- `test_empty_matcher_allows_everything`: Verifies empty patterns result in no-op pass-through.
- `test_config_driven_patterns_only`: Verifies WYCIWIS behavior (unconfigured patterns are not matched).

### 2. Plugin Hot Path (`plugin.rs` — 9 Tests)
- `test_waf_blocks_path_traversal_with_waf_blocked_reason`: Verifies `Action::Reject(RejectReason::WafBlocked)` on path traversal.
- `test_waf_blocks_double_percent_encoded_traversal`: Verifies nested percent-encoded traversal rejection.
- `test_waf_blocks_sqli_in_query_with_plus_and_comments`: Verifies SQLi rejection in query strings.
- `test_waf_blocks_xss_in_query`: Verifies XSS rejection in query strings.
- `test_waf_scans_body_within_32kb_window`: Verifies leading body inspection within 32 KB.
- `test_waf_caps_body_scan_at_32kb`: Verifies payload bytes past `32,768` bytes are skipped (anti-DoS cap).
- `test_waf_allows_clean_request_and_ignores_unconfigured_patterns`: Verifies clean requests and unconfigured strings pass.
- `test_waf_empty_patterns_always_continues`: Verifies empty patterns always continue.
- `test_from_config_deserialization`: Verifies JSON deserialization into `WafLitePlugin`.

### 3. Configuration Validation (`validation.rs` — 2 Tests)
- `test_valid_config_normalizes_and_deduplicates`: Verifies deterministic `sort_unstable` + `dedup` normalization.
- `test_rejects_empty_pattern_entry`: Verifies empty/whitespace pattern rejection.
