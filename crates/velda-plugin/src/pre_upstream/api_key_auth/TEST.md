# API Key Authentication Plugin (`api_key_auth`) — Test Plan & Security Verification

This document specifies the verification methodology, unit test matrix, adversarial attack simulations, and live cluster empirical results for the hardened **`api_key_auth`** plugin.

---

## 1. Unit Test Matrix

The plugin contains 15 automated unit tests in `velda-plugin` and `velda-sync` covering core functionality and threat defense vectors:

| Test Name | File | Description | Status |
| :--- | :--- | :--- | :---: |
| `test_valid_api_key_header_injection` | `plugin.rs` | Verifies successful key authentication, credential stripping, and consumer metadata injection. | **PASSED** |
| `test_missing_api_key_returns_401_with_no_store` | `plugin.rs` | Asserts 401 response contains `WWW-Authenticate: ApiKey realm=...` and `Cache-Control: no-store`. | **PASSED** |
| `test_invalid_api_key_rejected_with_no_store` | `plugin.rs` | Asserts 401 response contains `WWW-Authenticate: ApiKey error=invalid_key` and `Cache-Control: no-store`. | **PASSED** |
| `test_authorization_header_with_prefix_and_query_source` | `plugin.rs` | Tests dual source extraction: `Authorization: ApiKey <token>` and URL query parameter `?key=<token>`. | **PASSED** |
| `test_privilege_escalation_sanitizes_spoofed_headers` | `plugin.rs` | Verifies that client-supplied identity headers are stripped before injecting authenticated claims. | **PASSED** |
| `test_hide_credentials_strips_query_param` | `plugin.rs` | Asserts that secret query parameters are stripped from `req.uri` before forwarding upstream. | **PASSED** |
| `test_length_guard_fast_reject` | `plugin.rs` | Tests that tokens shorter than `min_key_len` or longer than `max_key_len` are rejected immediately. | **PASSED** |
| `test_constant_time_eq` | `plugin.rs` | Verifies bitwise constant-time equality over equal, unequal, and varying length byte slices. | **PASSED** |
| `test_from_config_complete_deserialization` | `plugin.rs` | Verifies deserialization from `PluginConfig` JSON manifest. | **PASSED** |
| `test_extract_header_with_prefix` | `source.rs` | Verifies single header prefix matching and token extraction. | **PASSED** |
| `test_extract_header_multiple_values` | `source.rs` | Verifies iteration through multiple headers with the same name until prefix matches. | **PASSED** |
| `test_extract_query_param` | `source.rs` | Verifies query string token extraction. | **PASSED** |
| `test_positive_cache_hit_and_expiration` | `cache.rs` | Tests `ApiKeyCache` entry expiration lifecycle. | **PASSED** |
| `test_negative_cache_hit_and_expiration` | `cache.rs` | Tests `ApiKeyCache` negative entry TTL and fast-fail behavior. | **PASSED** |
| `test_negative_eviction_when_valid_added` | `cache.rs` | Tests that negative entries are cleared when valid keys are registered. | **PASSED** |

---

## 2. Adversarial Security Attack Scenarios

### 2.1 Privilege Escalation via Header Spoofing Probe
- **Attack**: A client using a low-privilege API key (`sk-live-guest-beta-67890`) injects `X-Consumer-Tier: superuser` in request headers.
- **Expected Defense**: The plugin purges `X-Consumer-Tier` from the request and forwards only the authorized claims (`x-consumer-id: guest-beta`).
- **Assertion**: Upstream server receives zero elevated role headers from unprivileged callers.

### 2.2 Credential Leakage via Query Parameter Probe
- **Attack**: A client invokes `GET /api/v1/orders?key=sk-live-partner-alpha-12345&filter=recent`.
- **Expected Defense**: When `hide_credentials: true`, the plugin reconstructs `req.uri` as `/api/v1/orders?filter=recent`.
- **Assertion**: Upstream access logs, error tracking, and referer headers contain zero secret keys.

### 2.3 Length Bombing DoS Probe
- **Attack**: An attacker sends an 8KB random string token via `X-API-Key` to exhaust gateway CPU hashing.
- **Expected Defense**: Fast-Reject Length Guard detects `token.len() > max_key_len` and returns `401 Unauthorized` in < 1ns without executing hashing or table lookup.

### 2.4 Cache Poisoning DoS (CPDoS) Probe
- **Attack**: An unauthenticated probe triggers a 401 response on a shared endpoint.
- **Expected Defense**: Gateway returns `Cache-Control: no-store` on both missing key and invalid key 401 responses.
- **Assertion**: Downstream reverse proxies and CDNs do not cache the 401 response for subsequent valid users.

---

## 3. Live Azure Cluster Empirical Verification

Live tests executed against the `velda-edge` production gateway running on Azure VM1 from load client Azure VM2:

### 3.1 Clean Authentication via `X-API-Key`
```bash
curl -i -H "X-API-Key: sk-live-partner-alpha-12345" http://<GATEWAY_HOST>/api/apikey/protected
```
```http
HTTP/1.1 200 OK
content-type: text/plain
content-length: 4
date: Fri, 23 Oct 2026 16:07:15 GMT
x-request-id: 000000006adb8633000200000000003e

pong
```

### 3.2 Clean Authentication via `Authorization: ApiKey`
```bash
curl -i -H "Authorization: ApiKey sk-live-partner-alpha-12345" http://<GATEWAY_HOST>/api/apikey/protected
```
```http
HTTP/1.1 200 OK
content-type: text/plain
content-length: 4
date: Fri, 23 Oct 2026 16:07:49 GMT
x-request-id: 000000006adb8655000200000000003d

pong
```

### 3.3 Clean Authentication via Query Parameter
```bash
curl -i "http://<GATEWAY_HOST>/api/apikey/protected?key=sk-live-partner-alpha-12345&mode=raw"
```
```http
HTTP/1.1 200 OK
content-type: text/plain
content-length: 4
date: Fri, 23 Oct 2026 16:07:54 GMT
x-request-id: 000000006adb865a000400000000003f

pong
```

### 3.4 Missing Key Rejection (Assert `no-store`)
```bash
curl -i http://<GATEWAY_HOST>/api/apikey/protected
```
```http
HTTP/1.1 401 Unauthorized
www-authenticate: ApiKey realm="velda-edge"
content-type: application/json
cache-control: no-store
x-request-id: 000000006adb8627000200000000003f
content-length: 27

{"error":"Missing API key"}
```

### 3.5 Invalid Key Rejection (Assert `no-store`)
```bash
curl -i -H "X-API-Key: invalid-key-12345" http://<GATEWAY_HOST>/api/apikey/protected
```
```http
HTTP/1.1 401 Unauthorized
www-authenticate: ApiKey error="invalid_key"
content-type: application/json
cache-control: no-store
x-request-id: 000000006adb862d000300000000003f
content-length: 27

{"error":"Invalid API key"}
```
