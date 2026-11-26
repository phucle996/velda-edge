# JWT Authentication Plugin (`jwt_auth`) — Test Verification Report

Verification report for `crates/velda-plugin/src/pre_upstream/jwt_auth` and its companion `jwks_rotator` cross-plugin integration.

---

## 1. Verification Commands & Observed Results

```bash
cargo test -p velda-plugin -- pre_upstream::jwt_auth
cargo test -p velda-plugin -- jwks_rotator
```

Observed output:

```text
running 22 tests
test result: ok. 22 passed; 0 failed; 0 ignored; 0 measured; 428 filtered out

running 9 tests
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 441 filtered out
```

Full crate & workspace verification checks:

- `cargo fmt --check`: exited `0`
- `cargo test -p velda-plugin --lib`: `450 passed; 0 failed`
- `cargo test -p velda-sync --lib`: `72 passed; 0 failed`
- `cargo clippy -p velda-plugin -p velda-sync -p velda-edge --all-targets --all-features -- -D warnings`: exited `0` (`0` warnings)

---

## 2. Verified Test Catalog

### 2.1 L1D/L3 Contiguous SIMD Cache ([cache.rs](cache.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_control_header_and_bucket_cacheline_geometry` | Verifies `size_of::<JwtCache>() == 64`, `align_of::<JwtCache>() == 64`, `offset_of!(BucketGroup, tags) == 0`, and 64-byte alignment across all allocated shards and L3 bucket groups. |
| `test_disabled_cache_allocates_zero_shards` | Verifies `cache_ttl_secs == 0` sets `shards = None` (`0` heap shard allocation) and returns `CacheLookup::Miss`. |
| `test_positive_and_negative_unified_simd_lookup_and_epoch_invalidation` | Verifies unified single-pass SIMD lookup across Positive (`Valid`), `Unauthorized` (`401`), and `Forbidden` (`403`) entries, TTL expiry, and instant invalidation on `key_epoch` increment. |
| `test_64_way_bucket_saturation_and_clock_eviction` | Verifies 64-way group saturation (`80` inserts into a single group) evicts cleanly via O(1) clock-hand replacement. |
| `test_swar_parity_against_simd_and_adjacent_borrow_edges` | Verifies bit-exact parity between SIMD (`AVX2`/`SSE2`/`NEON`) and 64-bit zero-borrow SWAR (`pack_msbs_u64`), including adjacent byte borrow boundaries and all-64-lane (`u64::MAX`) saturation. |
| `test_plausible_jwt_shape_guard` | Verifies fast 2-dot structural filter rejects empty, dotless, single-dot, empty-segment, and 4-segment strings. |

### 2.2 Contiguous 8-Slot Key Table & Epoch Store ([table.rs](table.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_long_uuid_and_sha256_thumbprint_kids_match_without_truncation` | Verifies 36-byte UUIDv4 and 47-byte SHA-256 thumbprint `kid` strings match accurately (`MAX_KID_BYTES = 128`) and overlong `kid`s (`> 128B`) return `Err` instead of truncating. |
| `test_static_key_matches_arbitrary_idp_kid_only_when_unpaired` | Verifies unpaired static key mode accepts IdP tokens carrying arbitrary `kid` headers while multi-key rotated tables reject unknown `kid`s. |
| `test_shared_store_epoch_increments_on_update` | Verifies `NoActiveKeys` on empty boot store and monotonic `epoch` increment on `SharedJwksStore::update`. |

### 2.3 Cryptographic Verification & Claims Engine ([crypto.rs](crypto.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_hs256_full_verification` | Verifies HS256 signature validation, `iss`/`aud`/`exp` checks, nested dot-path claim projection, array joining, and `assert_claims`. |
| `test_eddsa_ed25519_spki_pem_verification` | Verifies end-to-end `EdDSA` (`Ed25519`) X.509 SPKI PEM key parsing and constant-time signature verification. |
| `test_float_and_malformed_nbf_never_bypass` | Verifies RFC 7519 floating-point future `nbf` (`2500000000.5` $\to$ `TokenNotBefore`) and non-numeric `nbf` (`"invalid"` $\to$ `InvalidNumericDate`) never bypass validation. |
| `test_signature_length_and_kid_rotation_errors` | Verifies fail-fast `SignatureLengthMismatch` before crypto math and multi-slot `kid` selection / `UnknownKid` rejection. |

### 2.4 Schema & Cross-Plugin Dependency Validation ([validation.rs](validation.rs), [factory.rs](../../factory.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_valid_hs256_config` | Verifies symmetric config validation and lowercase header name normalization. |
| `test_valid_rs256_static_and_rotator_null_pem` | Verifies both static 2048-bit RSA PEM validation and `public_key_pem: null` acceptance for rotator mode. |
| `test_corrupted_pem_and_unknown_fields_rejected` | Verifies invalid ASN.1 DER PEM and unknown JSON fields (`#[serde(deny_unknown_fields)]`) fail fast at sync time. |
| `test_hs256_missing_secret_and_invalid_alg_rejected` | Verifies missing `secret_key` and `"alg": "NONE"` are rejected. |
| `test_asymmetric_jwt_null_pem_requires_jwks_rotator_dependency` | Verifies `validate_plugin_dependencies` rejects asymmetric `public_key_pem: null` without a companion `jwks_rotator` and accepts valid pairs. |
| `test_wire_plugin_dependencies_null_pem_without_rotator_fails` | Verifies runtime `wire_plugin_dependencies` enforces the same companion rotator invariant. |

### 2.5 End-to-End Plugin Hot Path & Companion JWKS Rotator ([plugin.rs](plugin.rs), `jwks_rotator`)

| Test Case | Scope Verified |
| :--- | :--- |
| `test_on_upstream_request_missing_token` | Verifies `401 Unauthorized` + `WWW-Authenticate: Bearer realm="velda-edge"` when token is absent. |
| `test_valid_token_cache_hit_and_missing_optional_claim_preserves_client_headers` | Verifies L3 SIMD Positive Cache hit, overwrite of present mapped claims, and preservation of existing client headers (e.g. `accept-language`) when optional claims are absent. |
| `test_negative_and_forbidden_caching_plus_key_rotation_invalidation` | Verifies `403 Forbidden` caching on `assert_claims` failure and immediate token invalidation when `SharedJwksStore` rotates keys. |
| `test_http2_split_cookie_headers_and_query_extraction` | Verifies RFC 9113 split HTTP/2 `cookie` header scanning via `get_all_prehashed` and URI query fallback. |
| `test_malformed_multibyte_utf8_auth_header_does_not_panic` | Verifies multibyte UTF-8 boundary safety on `Authorization` prefix slicing. |
| `test_extract_http_body_decodes_chunked_transfer_encoding` | Verifies `jwks_rotator` decodes HTTP/1.1 `Transfer-Encoding: chunked` JWKS responses (including chunk extensions). |
| `test_parse_okp_ed25519_jwks` & `test_parse_single_rsa_jwks_strips_leading_zero_modulus` | Verifies RFC 8037 `OKP`/`Ed25519` JWK ingestion and leading-`0x00` RSA modulus normalization. |
