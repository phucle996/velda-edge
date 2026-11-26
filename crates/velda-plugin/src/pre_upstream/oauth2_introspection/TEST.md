# OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`) — Test Verification Report

Verification report for the redesigned `crates/velda-plugin/src/pre_upstream/oauth2_introspection` subsystem and `velda-edge` async cold-path pipeline integration.

---

## 1. Verification Commands & Observed Results

```bash
cargo test -p velda-plugin -- oauth2_introspection
```

Observed output:

```text
running 19 tests
test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 435 filtered out
```

Full crate & workspace verification checks:

- `cargo test -p velda-plugin --lib`: `454 passed; 0 failed`
- `cargo test -p velda-sync --lib`: `72 passed; 0 failed`
- `cargo test -p velda-edge --lib`: `55 passed; 0 failed`
- `cargo clippy -p velda-plugin -p velda-sync -p velda-edge --all-targets --all-features -- -D warnings`: exited `0` (`0` warnings)

---

## 2. Verified Test Catalog

### 2.1 End-to-End Hybrid Warm/Cold Flow, TLS, Discovery & Single-Flight ([plugin.rs](plugin.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_discovery_hosts_chunked_cold_miss_warm_hit_and_header_semantics` | Resolves `idp.internal.local` via `HostsFileSource` (`velda-discovery`), yields on cold miss, decodes HTTP/1.1 `Transfer-Encoding: chunked`, preserves client headers when optional claim is absent, hides token header, and serves request #2 synchronously via `try_on_upstream_request` with `0` extra IdP calls. |
| `test_https_tls_introspection_endpoint` | Establishes real `https://` TLS 1.3 connection validated against custom `tls.ca_pem` using `velda_tls::client::connect` over `tokio::net::TcpStream`. |
| `test_single_flight_deduplicates_concurrent_cold_misses` | Fires `16` concurrent async tasks carrying identical unseen token; `InFlightCoordinator` coalesces misses cleanly (`<= 2` IdP calls instead of 16). |
| `test_zero_ttl_disables_positive_cache_for_instant_logout_revocation` | Verifies `cache_ttl_seconds: 0` bypasses positive cache so IdP logout revocation (`active: false`) takes effect immediately while storing `401` into L3 negative cache. |
| `test_insufficient_scope_and_idp_unavailable_fallback` | Verifies `403 Forbidden` (`insufficient_scope`) caching on L3 and fail-closed (`502 BadGateway`) vs fail-open (`Action::Continue`) on unreachable IdP. |

### 2.2 Dynamic 10%-RAM L3 SIMD Cache & Stale Window ([cache.rs](cache.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_stale_refresh_window_clamps_to_10_percent_and_3_to_10_secs` | Verifies `compute_stale_refresh_secs` returns `0` for `0..=3`, `3` for `4..=30`, `5` for `50`, and `10` for `>= 100`. |
| `test_10_percent_ram_ceiling_geometry_and_bounded_eviction` | Simulates `32 MiB` container RAM (`10% = 3,355,440 B`), verifies initial allocation fits cleanly under budget, then inserts `5,000` distinct tokens verifying shard doubling halts at `max_groups_per_shard` with O(1) clock-hand eviction. |
| `test_unified_simd_lookup_stale_revalidation_and_negative_flip` | Verifies Hit $\to$ Stale $\to$ single-flight `mark_revalidating` deduplication $\to$ immediate `store_negative` logout flip. |
| `test_swar_and_simd_parity` | Verifies bit-exact parity between SIMD (`AVX2`/`SSE2`/`NEON`) and 64-bit zero-borrow SWAR `TagLine` scanners. |

### 2.3 Async HTTP/HTTPS Framing & RFC 7519 Parsing ([client.rs](client.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_chunked_decode_incremental_and_complete` | Verifies incremental partial chunk buffering and complete decoding with chunk extensions. |
| `test_numeric_date_float_and_nested_claims` | Verifies floating-point `exp` floor, `nbf` ceil, and dot-path nested claim extraction (`profile.tenant`). |

### 2.4 Configuration Schema Validation & Scope Matcher ([validation.rs](validation.rs), [scope.rs](scope.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_valid_oauth2_introspection_config_and_zero_ttl` | Verifies IPv6 `[::1]` HTTPS URL acceptance, header lowercasing, and `cache_ttl_seconds: 0` acceptance. |
| `test_unknown_fields_and_legacy_max_cache_entries_rejected` | Enforces `#[serde(deny_unknown_fields)]` and rejects removed `max_cache_entries` field. |
| `test_invalid_url_and_auth_method_rejected` | Rejects unsupported URI schemes (`ftp://`) and auth methods. |
| `test_empty_sources_and_duplicate_target_headers_rejected` | Enforces at least one token source and rejects case-insensitive duplicate target headers. |
| `test_empty_required_always_passes`, `test_missing_granted_fails_when_required`, `test_matching_scopes`, `test_partial_token_does_not_match` | Verifies RFC 6749 space-delimited token matching without substring false positives (`read` vs `reader`). |
