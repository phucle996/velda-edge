# JA4 TLS Filter (`ja4_filter`) — Test Matrix & Verification Report

All unit, integration, and live loopback probe checks for the JA4 filter chain are executed via:
```bash
cargo test -p velda-core
cargo test -p velda-tls -- ja4
cargo test -p velda-plugin -- ja4_filter bot_detection
```

## Test Coverage Summary

### 1. Core Cache-Line & Deterministic ID (`velda-core/src/context.rs`)
- Compile-time assertion verifies `std::mem::size_of::<RequestState>() == 64` bytes.
- `crates/velda-core/tests/layout_and_alloc.rs` (`5` + `6` + `3` integration tests) and `106` `velda-core` unit tests passing.

### 2. TLS ClientHello JA4 Extractor (`velda-tls/src/server/ja4.rs` + Integration Tests)
Executed via `cargo test -p velda-tls -- ja4`:
- `server::ja4::tests::test_ja4_extraction_with_grease_filtering` (`velda-tls/src/server/ja4.rs`): Verifies GREASE removal, cipher/extension sorting, ALPN/SNI/supported_versions extraction, and parity with `velda_core::ja4_fingerprint_id()`.
- `server::ja4::tests::test_ja4_invalid_and_truncated_inputs` (`velda-tls/src/server/ja4.rs`): Verifies empty, plaintext HTTP, and truncated records safely return `0`.
- `server::ja4::tests::test_ja4_multi_tls_record_fragmentation_parity` (`velda-tls/src/server/ja4.rs`): Verifies split multi-record TLS `0x16` handshakes reassemble to the identical 64-bit JA4 ID.
- `test_accept_with_ja4_handles_tiny_tcp_fragments`: Verifies `TlsServerEngine::accept` extracts non-zero `ja4` under tiny TCP segments.
- `test_quic_handshake_extracts_non_zero_ja4`: Verifies QUIC TLS `ClientHello` handshake extraction produces non-zero `ja4`.

### 3. Pre-Upstream Plugin (`velda-plugin/src/pre_upstream/ja4_filter/plugin.rs` — 2 Tests)
- `test_ja4_filter_denies_matching_string_and_hex`: Verifies matching canonical 36-char JA4 strings and `0x` hex IDs return `Action::Reject(RejectReason::Forbidden)`, non-matching IDs return `Action::Continue`, and sentinel `0` passes.
- `test_ja4_filter_large_list_binary_search`: Verifies `len > 16` binary search branch accurately matches and misses across 64 entries.

### 4. Direct KVM JA4 Request Probes (`ja4_filter/bot/`)
Executed from `velda-obs` directly against `velda-gw` (`192.168.122.10:8443`) using [`probe_ja4.py`](bot/probe_ja4.py):
- Verifies `ja4_filter` drops a denied TLS JA4 immediately with `403 Forbidden` even when spoofing full Chrome HTTP headers (`User-Agent`, `Sec-Fetch-*`, `Accept-Language`).
- Verifies a cipher-shifted TLS `ClientHello` produces a distinct JA4 hash and passes `ja4_filter` (`200 OK`).
