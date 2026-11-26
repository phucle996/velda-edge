# OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`) — Test Verification Report

Verification report for the refactored ETP-backed `crates/velda-plugin/src/pre_upstream/oauth2_introspection` subsystem and `velda-sync` ETP reference validation.

---

## 1. Verification Commands & Observed Results

```bash
cargo fmt --check
cargo test -p velda-plugin -- oauth2_introspection
cargo test -p velda-plugin --lib
cargo test -p velda-sync --lib
cargo test -p velda-edge --lib
cargo test -p velda-etp
cargo clippy -p velda-plugin -p velda-sync -p velda-edge -p velda-etp --all-targets --all-features -- -D warnings
```

Observed results:

- `cargo test -p velda-plugin -- oauth2_introspection`: `21 passed; 0 failed; 0 ignored; 0 measured; 421 filtered out`
- `cargo test -p velda-plugin --lib`: `442 passed; 0 failed; 0 ignored`
- `cargo test -p velda-sync --lib`: `72 passed; 0 failed; 0 ignored`
- `cargo test -p velda-edge --lib`: `55 passed; 0 failed; 0 ignored`
- `cargo test -p velda-etp`: `6 passed; 0 failed` (`lib`) + `15 passed; 0 failed` (`tests/etp_tests.rs`)
- `cargo clippy -p velda-plugin -p velda-sync -p velda-edge -p velda-etp --all-targets --all-features -- -D warnings`: exited `0` (`0` warnings)
- `cargo fmt --check`: exited `0`

---

## 2. Verified Test Catalog

### 2.1 End-to-End ETP Providers, TLS & Single-Flight ([plugin.rs](plugin.rs), [providers/keycloak/response.rs](providers/keycloak/response.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_keycloak_chunked_cold_miss_warm_hit_and_header_semantics` | Attaches `KeycloakEtp` (`StreamPool`), yields on cold miss, decodes HTTP/1.1 `Transfer-Encoding: chunked`, strips credential header, preserves non-projected client headers, and serves request #2 synchronously via `try_on_upstream_request` with `0` extra IdP calls. |
| `test_https_tls_introspection_via_etp` | Validates HTTPS introspection over `KeycloakEtp` configured with `EtpTlsConfig` (`ca_cert_pem` + `server_name`) against TLS server. |
| `test_single_flight_deduplicates_concurrent_cold_misses` | Fires `16` concurrent tasks carrying identical unseen token against `HydraEtp` (`auth_method: "none"`); `inflight::InFlightCoordinator` coalesces misses cleanly (`<= 2` IdP calls). |
| `test_zero_ttl_disables_positive_cache_for_instant_logout_revocation` | Verifies `cache_ttl_seconds: 0` with `OktaEtp` (`client_secret_post`) reflects immediate revocation (`active: false`) while committing `401` to Hot L3 negative cache. |
| `test_insufficient_scope_and_idp_unavailable_fallback` | Verifies `403 Forbidden` (`insufficient_scope`) on `OidcEtp` and fail-closed (`502 BadGateway`) vs fail-open (`Action::Continue`) on unreachable `AutheliaEtp`. |
| `test_auth0_userinfo_and_vault_token_lookup_providers` | Verifies `Auth0Etp` `GET /userinfo` (`200` claim extraction + `401` inactive) and `VaultEtp` `POST /v1/auth/token/lookup` (`200` `entity_id`/`policies`/`ttl` mapping + `400` bad token -> `401`). |
| `test_numeric_date_float_and_nested_claims` | Verifies floating-point `exp` floor, `nbf` ceil, and dot-path nested claim extraction (`profile.tenant`). |

### 2.2 Per-Core Two-Tier Cache & Scope Matcher ([cache.rs](cache.rs), [scope.rs](scope.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_stale_refresh_window_clamps_to_10_percent_and_3_to_10_secs` | Verifies `compute_stale_refresh_secs` returns `0` for `0..=3`, `3` for `4..=30`, `5` for `50`, and `10` for `>= 100`. |
| `test_hardware_sizing_caps_hot_l3_and_cold_dram_on_many_cores` | Verifies `hardware_sized` bounds Hot L3 `<= MAX_HOT_L3_TOTAL_BYTES` (`4 MiB`) and Cold DRAM `<= MAX_COLD_DRAM_TOTAL_BYTES` (`256 MiB`) on 64-core sizing. |
| `test_hot_to_cold_demotion_promotion_and_401_non_demotion` | Verifies lazy Cold DRAM allocation on valid Hot L3 eviction, Cold $\to$ Hot promotion on hit, and that `401 Unauthorized` sprays never demote into Cold DRAM. |
| `test_per_core_isolation_across_lanes` | Verifies share-nothing lane isolation across `current_worker_lane()` indices (`lane 0` vs `lane 1`). |
| `test_unified_simd_lookup_stale_revalidation_and_negative_flip` | Verifies Hit $\to$ Stale $\to$ `mark_revalidating` deduplication $\to$ `store_negative` flip. |
| `test_swar_and_simd_parity` | Verifies bit-exact parity between SIMD (`AVX2`/`SSE2`/`NEON`) and 64-bit SWAR `TagLine` scanners. |
| `test_empty_required_always_passes`, `test_missing_granted_fails_when_required`, `test_matching_scopes`, `test_partial_token_does_not_match` | Verifies RFC 6749 space-delimited scope matching without substring false positives. |

### 2.3 Clean Schema Validation & ETP Sync Validation ([validation.rs](validation.rs), `velda-sync`)

| Test Case | Scope Verified |
| :--- | :--- |
| `test_valid_oauth2_introspection_config_and_zero_ttl` | Verifies `etp_id`, `path`, optional `client_id`/`client_secret`, `"none"` auth method, and `0` TTLs. |
| `test_unknown_and_legacy_pool_fields_rejected` | Rejects legacy pre-ETP fields (`introspection_url`, `keepalive_timeout_secs`, `pool_max_idle_connections`) via `deny_unknown_fields`. |
| `test_invalid_etp_id_path_and_auth_method_rejected` | Rejects empty `etp_id`, full URL passed as `path`, and unsupported `auth_method`. |
| `test_empty_sources_and_duplicate_target_headers_rejected` | Enforces >= 1 token extraction source and rejects duplicate target headers. |
| `velda_sync::post_sync::etp::tests::test_validate_plugin_etp_references` | Verifies `oauth2_introspection` accepts `Vault`, `Keycloak`, and supported OAuth2 ETP kinds while rejecting mismatched kinds (`redis`). |
