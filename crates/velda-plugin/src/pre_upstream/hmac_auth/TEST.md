# HMAC Authentication Plugin (`hmac_auth`) — Test Verification Report

Verification report for the rewritten `crates/velda-plugin/src/pre_upstream/hmac_auth` plugin across credential dispatch, canonical streaming HMAC, anti-replay nonces, spoofing prevention, and config validation.

---

## 1. Verification Commands & Observed Results

```bash
cargo test -p velda-plugin -- pre_upstream::hmac_auth
```

Observed output:

```text
running 18 tests
test result: ok. 18 passed; 0 failed; 0 ignored; 0 measured; 420 filtered out
```

Full crate & workspace checks run after final edits:

- `cargo fmt --check`: exited `0`
- `cargo test -p velda-plugin`: `438 passed; 0 failed`
- `cargo clippy --all-targets --all-features -- -D warnings`: exited `0`

---

## 2. Verified Test Catalog

### 2.1 Zero-Hash Credential Table ([credential.rs](credential.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_single_and_shared_prefix_keys_lookup` | Verifies `Single` plan and distinguishing bit dispatch across shared-prefix keys (`partner_alpha_*`, `partner_beta_*`, `partner_gamma_*`), plus negative rejection of mismatched keys and short keys. |
| `test_many_keys_varying_lengths_radix_fallback` | Verifies `120` keys across fallback buckets (`key_0000_end`..`key_0119_end`) resolve cleanly with zero false positives. |

### 2.2 Streaming Canonical & Crypto Primitives ([canonical.rs](canonical.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_streaming_sign_canonical_h1_and_h2_match` | Verifies `sign_canonical_request` streaming through `hmac::Context` matches canonical reference string identically for HTTP/1.1 (`Host` header) and HTTP/2 (`:authority` fallback) including sorted query (`b=2&flag&a=1` -> `a=1&b=2&flag=`). |
| `test_decode_hex_case_insensitive_and_base64` | Verifies stack decoding of uppercase/lowercase `hex` and `base64` signatures into raw 32-byte digest buffers. |
| `test_constant_time_eq_delegates_to_ring` | Verifies byte slice equality/inequality and length-mismatch rejection. |

### 2.3 Sharded Nonce Replay Cache ([nonce.rs](nonce.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_compute_optimal_shards_bounds` | Verifies `(cores * 16).next_power_of_two().clamp(16, 512)` shard count bounds across `1..=512` cores. |
| `test_nonce_fresh_and_replay` | Verifies fresh nonce recording, duplicate rejection within TTL window, distinct nonce acceptance, and re-acceptance after expiry. |

### 2.4 End-to-End Plugin Hot Path ([plugin.rs](plugin.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_hmac_auth_success_and_replay_protection` | Verifies valid HMAC pass, `x-consumer-id` injection (`test-consumer`), and immediate replay rejection (`Action::Reject(RejectReason::Unauthorized)`). |
| `test_hmac_auth_invalid_signature_does_not_poison_nonce` | Verifies forged signatures fail before `NonceCache` commit so legitimate requests using the same nonce still succeed. |
| `test_hmac_auth_tampered_payload_rejected` | Verifies mismatch between signed body and mutated `Body::Bytes` triggers `Unauthorized`. |
| `test_hmac_auth_body_tampering_bypass_with_content_sha256_header_prevented` | Verifies forged `x-content-sha256` matching original body is rejected when actual `Body::Bytes` is altered. |
| `test_hmac_auth_valid_with_content_sha256_header_and_uppercase_sig` | Verifies valid uppercase hex `x-content-sha256` and uppercase hex `x-signature` pass authentication. |
| `test_hmac_auth_http2_authority_support` | Verifies HTTP/2 request without `Host` header falls back cleanly to URI `:authority`. |
| `test_hmac_auth_scrubs_spoofed_consumer_id_when_none` | Verifies client-supplied `x-consumer-id` is scrubbed when matched credential has `consumer_id: None`. |

### 2.5 Configuration Schema Validation ([validation.rs](validation.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_valid_hmac_config_normalizes_case` | Verifies case-insensitive config values (`HMAC-SHA512`, `BASE64`) are normalized to lowercase (`hmac-sha512`, `base64`). |
| `test_missing_timestamp_in_signed_headers` | Rejects configs omitting `timestamp_header` from `signed_headers`. |
| `test_secret_key_too_short` | Rejects `secret_key` shorter than 16 characters. |
| `test_unknown_fields_rejected` | Rejects unknown JSON keys (`#[serde(deny_unknown_fields)]`). |
