# HTTP Method Filter (`method_filter`) — Test Matrix & Verification Report

All unit and validation tests for `method_filter` are executed via:
```bash
cargo test -p velda-plugin -- method_filter
```

## Test Coverage Summary (7 / 7 Tests Passing)

### 1. Plugin Hot Path (`plugin.rs` — 4 Tests)
- `test_allows_configured_standard_methods_and_blocks_others`: Verifies 16-bit bitmask permits configured `GET`, `POST`, `OPTIONS` and rejects `DELETE`, `TRACE`, `CONNECT` with `RejectReason::MethodNotAllowed`.
- `test_blocks_unknown_custom_verbs_when_only_standard_methods_configured`: Verifies `PROPFIND` (bit `0` `HttpMethod::Unknown`) is rejected when not explicitly configured.
- `test_allows_explicit_custom_webdav_methods`: Verifies explicit `PROPFIND` passes via fallback while `MKCOL` is rejected.
- `test_from_config_deserialization`: Verifies JSON deserialization via `PluginConfig`.

### 2. Configuration Validation (`validation.rs` — 3 Tests)
- `test_valid_config_normalizes_and_deduplicates`: Verifies uppercase normalization, sorting, and deduplication.
- `test_rejects_empty_allowed_methods`: Verifies empty `allowed_methods: []` is rejected.
- `test_rejects_invalid_method_token`: Verifies RFC 9110 invalid tokens (whitespace) are rejected.
