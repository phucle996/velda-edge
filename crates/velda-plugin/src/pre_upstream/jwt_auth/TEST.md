# JWT Authentication Plugin (`jwt_auth`) — Test Verification Report

Verification report for the unified `crates/velda-plugin/src/pre_upstream/jwt_auth` subsystem, `velda-etp` provider pools, and `velda-sync` ETP validation.

---

## 1. Verification Commands & Observed Results

```bash
cargo fmt --check
cargo test -p velda-etp
cargo test -p velda-plugin -- pre_upstream::jwt_auth
cargo test -p velda-plugin --lib
cargo test -p velda-sync --lib
cargo test -p velda-edge --lib
cargo clippy --all-targets --all-features -- -D warnings
```

Observed results:

- `cargo fmt --check`: exited `0`
- `cargo test -p velda-etp`: `6 passed; 0 failed` (unit) + `15 passed; 0 failed` (`tests/etp_tests.rs`)
- `cargo test -p velda-plugin -- pre_upstream::jwt_auth`: `20 passed; 0 failed` (`420` filtered out)
- `cargo test -p velda-plugin --lib`: `440 passed; 0 failed`
- `cargo test -p velda-sync --lib`: `72 passed; 0 failed`
- `cargo test -p velda-edge --lib`: `55 passed; 0 failed`
- `cargo clippy --all-targets --all-features -- -D warnings`: exited `0` (`0` warnings)

---

## 2. Verified `jwt_auth` Test Catalog

### 2.1 L1D/L3 Per-Core SIMD Cache ([cache.rs](cache.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_control_header_and_bucket_cacheline_geometry` | Verifies `size_of::<JwtCache>() == 64`, `align_of::<JwtCache>() == 64`, `align_of::<CoreCacheLane>() == 64`, `offset_of!(BucketGroup, tags) == 0`, and 64-byte alignment across all per-core bucket groups. |
| `test_disabled_cache_allocates_zero_lanes` | Verifies `cache_ttl_secs == 0` allocates `0` per-core lanes and returns `CacheLookup::Miss`. |
| `test_per_core_lane_isolation_no_cross_lane_leakage` | Verifies each worker lane maintains its own isolated L3 slice without leaking entries or requiring cross-core locks. |
| `test_hardware_sized_respects_l3_cap_on_64_cores` | Verifies 64-core scaling obeys `MAX_HOT_L3_TOTAL_BYTES` (`<= 4 MiB` Hot L3 total) alongside bounded Cold DRAM capacity. |
| `test_hot_l3_eviction_demotes_to_cold_dram_and_promotes_on_hit` | Verifies Hot L3 overflow demotes valid entries to Cold DRAM, Cold DRAM hits swap back into Hot L3, and `401` rejects never pollute Cold DRAM. |
| `test_positive_and_negative_unified_simd_lookup_and_kid_tag_invalidation` | Verifies unified single-pass SIMD lookup across Positive, `Unauthorized`, and `Forbidden` decisions and per-slot `key_tag` invalidation on key change. |
| `test_64_way_bucket_saturation_and_clock_eviction` | Verifies 64-way bucket saturation evicts cleanly via O(1) clock-hand replacement. |
| `test_swar_parity_against_simd_and_adjacent_borrow_edges` | Verifies bit-exact parity between SIMD (`AVX2`/`SSE2`/`NEON`) and zero-borrow 64-bit SWAR across edge bytes and saturation. |
| `test_plausible_jwt_shape_guard` | Verifies fast 2-dot shape pre-filter rejects malformed strings before SHA-256 hashing. |

### 2.2 L1D Directory + L3 Integer Metadata Store ([store.rs](store.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_salted_kid_lookup_standby_retention_and_material_change_invalidation` | Verifies salted `kid_ints` resolution, `key_tag` retention for unchanged standby keys across rotation, and immediate invalidation when key material changes. |
| `test_static_single_key_wildcard_kid_turns_off_with_standby` | Verifies wildcard `kid` acceptance when unpaired single static key is active, and strict unknown-`kid` rejection when multi-slot keys are active. |

### 2.3 Cryptographic Verification & Claims Engine ([crypto.rs](crypto.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_hs256_full_verification` | Verifies HS256 verification, `iss`/`aud`/`exp`, nested claim projection, array serialization, and `assert_claims`. |
| `test_eddsa_ed25519_spki_pem_verification` | Verifies end-to-end `EdDSA` (`Ed25519`) SPKI PEM parsing and signature verification. |
| `test_float_and_malformed_nbf_never_bypass` | Verifies RFC 7519 numeric float `nbf` and invalid `nbf` values never bypass validation. |
| `test_signature_length_and_kid_rotation_errors` | Verifies `SignatureLengthMismatch` short-circuit, multi-slot `kid` routing, and `UnknownKid` rejection. |

### 2.4 Unified `internal` / `external` Schema Validation ([validation.rs](validation.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_internal_mode_hs256_and_rs256` | Verifies `MODE_INTERNAL` accepts pinned `HS256`/`RS256` configs and forces `pbww = 0`. |
| `test_external_mode_keyless_hmac_and_multi_algo` | Verifies `MODE_EXTERNAL` allows null `algorithm`/`secret_key`/`public_key_pem` for dynamic ETP keys and sets positive `pbww`. |
| `test_cross_mode_validation_rejects_mismatches` | Verifies `internal` rejects `etp_id`/`path` and missing keys, while `external` requires `etp_id` and rejects conflicting `secret_key` + `public_key_pem`. |

### 2.5 End-to-End `JwtAuthPlugin` Hot Path & Vault ETP Adapter ([plugin.rs](plugin.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_internal_mode_missing_and_valid_tokens` | Verifies `pbww() == 0`, `etp_id().is_none()`, `401` on missing tokens, and `200`/claim header injection on valid internal HS256 tokens. |
| `test_external_mode_vault_etp_fetches_key_with_alg_and_prioritizes_etp` | Verifies end-to-end Vault ETP borrowing (`KV v2` returning `HS512` + `kid`), ETP Slot 0 prioritization over static `HS256` fallback, and hot-path verification. |
