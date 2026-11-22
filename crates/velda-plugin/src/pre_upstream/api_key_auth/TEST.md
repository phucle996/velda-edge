# API Key Authentication (`api_key_auth`) — Test Matrix & Verification Report

All unit and validation tests for `api_key_auth` are executed via:
```bash
cargo test -p velda-plugin api_key_auth
```

## Test Coverage Summary (5 / 5 Tests Passing)

### 1. Plugin Hot Path (`plugin.rs` — 2 Tests)
- `test_valid_key_overwrites_headers_and_caches_per_core`: Verifies `k * 4096` `CoreCacheArena` alignment, `hide_credentials` removal, direct overwrite of spoofed `x-user-id` header via `inject_headers`, and positive cache hit on subsequent request.
- `test_invalid_expired_and_oversized_keys_rejected`: Verifies missing header, wrong key (rejected without polluting `CoreCacheArena`), and expired key (`expires_at < coarse_sec()`) all return `Action::Reject(RejectReason::Unauthorized)`.

### 2. Configuration Validation (`validation.rs` — 3 Tests)
- `test_valid_config_normalizes_header_and_slots`: Verifies `header_name` lowercase normalization and power-of-2 rounding of `cache_slots_per_core`.
- `test_rejects_oversized_key_over_64_bytes`: Verifies keys `> 64` bytes are rejected at config load time.
- `test_rejects_empty_keys_and_duplicate_keys`: Verifies empty `keys: []` and duplicate keys are rejected.
