# JA4 TLS Filter (`ja4_filter`) — Test Matrix & Verification Report

All unit and integration tests for the JA4 chain are executed via:
```bash
cargo test -p velda-core
cargo test -p velda-tls -- ja4
cargo test -p velda-plugin -- ja4_filter
```

## Test Coverage Summary

### 1. Core Cache-Line & Deterministic ID (`velda-core/src/context.rs`)
- Compile-time assertion `assert!(std::mem::size_of::<RequestState>() <= 64)` verified (`64` bytes).
- `type_sizes` and `zero_alloc_hot_path` in `crates/velda-core/tests/layout_and_alloc.rs`: Passing.

### 2. TLS ClientHello JA4 Extractor (`velda-tls/src/server/ja4.rs` — 2 Tests)
- `test_ja4_extraction_with_grease_filtering`: Verifies GREASE removal, cipher/extension sorting, ALPN/SNI/supported_versions extraction, and parity with `velda_core::ja4_fingerprint_id()`.
- `test_ja4_invalid_and_truncated_inputs`: Verifies empty, plaintext HTTP, and truncated records safely return `0`.

### 3. Pre-Upstream Plugin (`velda-plugin/src/pre_upstream/ja4_filter/plugin.rs` — 2 Tests)
- `test_ja4_filter_denies_matching_string_and_hex`: Verifies matching canonical 36-char JA4 strings and `0x` hex IDs return `Action::Reject(RejectReason::Forbidden)`, non-matching IDs return `Action::Continue`, and sentinel `0` passes.
- `test_ja4_filter_large_list_binary_search`: Verifies `len > 16` binary search branch accurately matches and misses across 64 entries.
