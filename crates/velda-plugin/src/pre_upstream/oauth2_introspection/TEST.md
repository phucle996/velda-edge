# OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`) — Test Specification

This document details the test architecture, empirical verification strategies, end-to-end integration scenarios, and testing procedures for the **`oauth2_introspection`** pre-upstream plugin (RFC 7662).

---

## 1. Test Architecture & Mindset

Testing the `oauth2_introspection` plugin adheres to three strict principles:

1. **Zero Mocking of Core Components**:
   Tests run against the genuine `IntrospectionCache` (L1 thread-local + L2 sharded) and real wire HTTP/1.1 parsing (`httparse` + `serde_json`). No internal methods are mocked.
2. **Deterministic Wire Simulation (`MockIdp`)**:
   A lightweight, RAII-governed TCP listener (`MockIdp`) simulates an RFC 7662 Authorization Server supporting HTTP/1.1 `Connection: keep-alive` on dynamic ports (`127.0.0.1:0`), ensuring clean parallel execution without port collisions.
3. **Comprehensive Edge & Security Verification**:
   Tests cover token extraction variants, negative caching, scope enforcement, IdP network failure policies, and anti-header spoofing.

---

## 2. Test Coverage Matrix

### 2.1 End-to-End Integration Tests (`tests/oauth2_introspection_test.rs`)

| # | Test Name | Scenario Description | Expected Outcome |
| :-: | :--- | :--- | :--- |
| **1** | `test_oauth2_introspection_e2e_valid_header_and_cache_hit` | Valid token in custom header (`x-access-token`). 1st request cold miss; 2nd request warm hit. | 1st request queries IdP (count=1), injects claims, hides header. 2nd request hits RAM cache (count remains 1). |
| **2** | `test_oauth2_introspection_e2e_authorization_bearer_extraction` | Standard RFC 6750 `Authorization: Bearer <token>` header extraction and stripping. | Token parsed, validated, genuine claims injected, `Authorization` header removed. |
| **3** | `test_oauth2_introspection_e2e_cookie_extraction` | Token extracted from HTTP `Cookie` header (`access_token=<token>`). | Token resolved, validated against IdP, upstream forwarded. |
| **4** | `test_oauth2_introspection_e2e_query_param_extraction` | Token extracted from URI query string (`?token=<token>&channel=ws`). | Token resolved and validated without modifying query parameters. |
| **5** | `test_oauth2_introspection_e2e_missing_token_rejected` | Request arriving without any configured header, cookie, or query parameter. | Immediately rejected with `401 Unauthorized` (`missing_token`) without querying IdP. |
| **6** | `test_oauth2_introspection_e2e_revoked_token_negative_caching` | Inactive/revoked token queried against IdP. 2nd request arrives during negative TTL. | 1st request queries IdP, gets `active: false`, caches negative hit. 2nd request rejected in RAM without querying IdP. |
| **7** | `test_oauth2_introspection_e2e_insufficient_scopes_forbidden` | Token has `read` scope, but route requires `read` AND `orders`. | Rejected with `403 Forbidden` (`insufficient_scope`). |
| **8** | `test_oauth2_introspection_e2e_expired_token_rejected` | Token active in payload but contains expired epoch timestamp (`exp: 1000`). | Rejected with `401 Unauthorized` (`token_expired`). |
| **9** | `test_oauth2_introspection_e2e_idp_down_fallback_behavior` | IdP unreachable on invalid port. Tests `reject` (Fail-Closed) vs `allow` (Fail-Open). | `reject` returns `502 Bad Gateway` (`idp_unavailable`); `allow` allows request through (`Action::Continue`). |
| **10** | `test_oauth2_introspection_e2e_anti_header_spoofing` | Attacker injects forged `x-user-id`, `x-user-role`, `x-client-id` headers in downstream request. | All client-forged target headers are stripped and overwritten with genuine IdP claims. |

### 2.2 Unit Tests (`src/pre_upstream/oauth2_introspection/`)

| File | Test Function | Purpose |
| :--- | :--- | :--- |
| `cache.rs` | `test_token_fingerprint_avalanche` | Verifies 128-bit hash avalanche across prefix, suffix, and middle-byte token variations. |
| `cache.rs` | `test_core_local_l1_cache` | Validates thread-local L1 insertion, direct-mapped lookup, and expiration. |
| `cache.rs` | `test_compute_optimal_shards` | Verifies shard scaling between 4 and 512 based on CPU cores and entry capacity. |
| `cache.rs` | `test_positive_hit_and_expiration` | Verifies L2 fresh hit, stale window detection (< 15s), and single-flight in-flight deduplication. |
| `cache.rs` | `test_negative_cache_hit_and_expiration` | Validates negative cache storage, hit resolution, and TTL expiration. |
| `scope.rs` | `test_empty_required_always_passes` | Empty required scope set passes unconditionally. |
| `scope.rs` | `test_missing_granted_fails_when_required` | Required scopes fail if granted scope string is empty or absent. |
| `scope.rs` | `test_matching_scopes` | Validates multi-scope subset matching with space delimiters. |
| `scope.rs` | `test_partial_token_does_not_match` | Prevents substring collisions (e.g. `read` must not match `reader` or `bread`). |

### 2.3 Live Infrastructure & Multi-Protocol Verification on Azure

| Protocol / Transport | Security Mode | Upstream IdP | Test Procedure | Outcome |
| :--- | :--- | :--- | :--- | :--- |
| **HTTP/1.1 (TCP :80)** | Cleartext | Live Ory Hydra (`:8085`) | Bearer token validation on `/v1/orders` | **PASS** (43,293.8 RPS) |
| **HTTP/1.1 (TCP :443)** | Standard TLS 1.3 | Live Ory Hydra (`:8085`) | End-to-end encrypted token verification | **PASS** (36,517.7 RPS) |
| **HTTP/2 (TCP :443)** | Standard TLS 1.3 | Live Ory Hydra (`:8085`) | Multiplexed stream token verification | **PASS** (42,578.3 RPS) |
| **HTTP/3 QUIC (UDP :443)** | Standard TLS 1.3 | Live Ory Hydra (`:8085`) | Datagram-transported token authentication | **PASS** (27,161.7 RPS) |
| **mTLS 2-Way (TCP :443)** | mTLS (Client Cert) | Live Ory Hydra (`:8085`) | Double-layer verification: mTLS + OAuth2 token | **PASS** (20,878.1 RPS) |
| **8-Way Chaotic Storm** | Mixed All Modes | Live Ory Hydra (`:8085`) | 30s concurrent storm (1.04M total requests) | **PASS** (0 leaks, 89.6 MB RSS) |

---

## 3. Running & Verifying Tests

### 3.1 Run All Integration Tests
```bash
cargo test --package velda-plugin --test oauth2_introspection_test
```

### 3.2 Run All Unit Tests
```bash
cargo test --package velda-plugin --lib pre_upstream::oauth2_introspection
```

### 3.3 Run Clippy Quality Audit
```bash
cargo clippy --package velda-plugin --all-targets --all-features -- -D warnings
```

### 3.4 Verify Formatting
```bash
cargo fmt --check
```
