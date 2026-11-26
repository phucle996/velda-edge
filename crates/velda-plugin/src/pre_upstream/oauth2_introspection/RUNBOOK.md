# OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`) — SRE Operational Runbook

Operational guide, complete schema specification, revocation-window tuning recipes, and troubleshooting playbooks for `oauth2_introspection`.

---

## 1. Configuration Parameter Reference

`Oauth2IntrospectionConfig` and `ClaimToHeaderConfig` enforce `#[serde(deny_unknown_fields)]`. Note that `max_cache_entries` is no longer part of the schema—cache memory scales automatically up to **10% of host/cgroup RAM** probed by `velda-core`.

### Parameter Specifications

| Parameter | Type | Required | Constraints / Allowed Values | Operational & Security Impact |
| :--- | :---: | :---: | :--- | :--- |
| **`introspection_url`** | `String` | **Yes** | Valid `http://` or `https://` URI (`IPv4`, `[IPv6]`, or DNS host) | Scheme selects Plain TCP vs TLS (`velda_tls`); port defaults to `80`/`443` only when omitted. Hostnames resolve via `velda-discovery` (`/etc/hosts` + DNS + LKG). |
| **`client_id`** | `String` | **Yes** | Non-empty string | Gateway RFC 7662 client identifier sent to the IdP. |
| **`client_secret`** | `String` | **Yes** | Non-empty string | Gateway secret credential; use `https://` on untrusted networks. |
| **`auth_method`** | `String` | **Yes** | `"client_secret_basic"` or `"client_secret_post"` | Sends client credentials in `Authorization: Basic` header or URL-encoded POST body. |
| **`token_headers`** | `Vec<String>` | **Yes**\* | Valid RFC 9110 header names (normalized lowercase) | Checked in order; strips optional case-insensitive `"Bearer "` prefix. |
| **`token_cookies`** | `Vec<String>` | **Yes**\* | Non-empty cookie names | Checked across all `cookie` headers (supports RFC 9113 split HTTP/2 cookies). |
| **`token_query_params`** | `Vec<String>` | **Yes**\* | Non-empty query parameter names | Fallback lookup from URI query string (`?token=...`). |
| **`token_type_hint`** | `String` | **Yes** | `"access_token"`, `"refresh_token"`, or `"none"` | Sent in form body per RFC 7662 §2.1 (`"none"` omits parameter). |
| **`cache_ttl_seconds`** | `u64` | **Yes** | `0 ..= 86400` seconds | Positive L3 cache TTL (`0` disables positive cache for instant logout revocation). When `> 3s`, background stale refresh runs in the final `clamp(ttl/10, 3, 10)` seconds. |
| **`negative_cache_ttl_seconds`** | `u64` | **Yes** | `0 ..= 86400` seconds | Caches `active: false`, expired `exp`, or premature `nbf` tokens (`401`) on L3 to neutralize brute-force DoS (`0` disables). |
| **`keepalive_timeout_secs`** | `u64` | **Yes** | `1 ..= 86400` seconds | Max idle connection age checked via `velda_core::clock::coarse_sec()`. |
| **`pool_max_idle_connections`** | `usize` | **Yes** | `1 ..= 1000` | Max idle Keep-Alive TCP/TLS connections retained toward IdP. |
| **`request_timeout_ms`** | `u64` | **Yes** | `1 ..= 60000` ms | Bounded async timeout per DNS/TCP/TLS/HTTP operation against IdP. |
| **`required_scopes`** | `Vec<String>` | **Yes** | Space-free RFC 6749 scope tokens (`[]` if none) | All scopes must exist in IdP `scope` string, else returns `403 Forbidden` (cached in L3). |
| **`claims_to_headers`** | `Vec<Object>` | **Yes** | `[{ "claim": "<dot.path>", "header": "<rfc9110>" }]` (`[]` if none) | Projects **present** claims into `HeaderWorkspace` (`insert_prehashed`). Absent claims leave existing request headers untouched. Target headers must be unique. |
| **`hide_credentials`** | `bool` | **Yes** | `true` or `false` | Deletes the matched token source header before forwarding upstream when `true`. |
| **`fallback_behavior`** | `String` | **Yes** | `"reject"` or `"allow"` | Failure policy when IdP is unreachable/times out (`502 Bad Gateway` vs fail-open `Continue`). |
| **`tls`** | `Object` | No | `OutboundTlsConfig` (`ca_pem`, `client_cert_pem`, `client_key_pem`, `insecure_skip_verify`) | Optional custom CA / mTLS credentials for `https://` `introspection_url`. |

\* At least one of `token_headers`, `token_cookies`, or `token_query_params` must be non-empty.

---

## 2. Managing Logout Revocation Windows & Header Hygiene

### 2.1 Understanding Cache TTL vs Revocation Latency
Per RFC 7662 Section 4, caching active introspection results trades IdP load against the **revocation propagation delay** (if a user logs out and the IdP revokes their token before TTL expiry, cached entries remain valid until TTL expiration or background revalidation flips them to negative):
- **Ultra-Sensitive Routes (Payments / Password Reset / Admin Mutation)**: Set `"cache_ttl_seconds": 0` and `"negative_cache_ttl_seconds": 10`. Every request checks live status at the IdP (zero logout window) while still blocking invalid token floods in L3.
- **Standard API Routes**: Set `"cache_ttl_seconds": 15..=30`. Warm requests execute synchronously at ~15 ns, and shadow revalidation (`3s` lead window) keeps active user sessions continuously refreshed without frontline latency spikes while capping revocation lag to seconds.

### 2.2 `claims_to_headers` Composition
`claims_to_headers` maps claims **present** in the verified IdP JSON payload into upstream headers (`insert_prehashed`). Optional claims omitted by the IdP leave client headers intact. If a downstream backend lacks token verification of its own and expects privileged headers like `x-user-roles` to never survive from untrusted clients when omitted by the IdP, place a `header_mutation` plugin (`remove_headers: ["x-user-roles"]`) ahead of `oauth2_introspection` in the route's FIFO `plugins` list.

---

## 3. Scenario Configuration Recipes

### 3.1 Production HTTPS + mTLS Introspection (Recommended)

```json
{
  "id": "oauth2_introspection",
  "config": {
    "introspection_url": "https://hydra.auth.svc.cluster.local/admin/oauth2/introspect",
    "client_id": "velda-edge-prod",
    "client_secret": "strong-client-secret",
    "auth_method": "client_secret_basic",
    "token_headers": ["authorization"],
    "token_cookies": [],
    "token_query_params": [],
    "token_type_hint": "access_token",
    "cache_ttl_seconds": 30,
    "negative_cache_ttl_seconds": 10,
    "keepalive_timeout_secs": 60,
    "pool_max_idle_connections": 16,
    "request_timeout_ms": 500,
    "required_scopes": ["api.read"],
    "claims_to_headers": [
      { "claim": "sub", "header": "x-user-id" },
      { "claim": "ext.tenant_id", "header": "x-tenant-id" }
    ],
    "hide_credentials": true,
    "fallback_behavior": "reject",
    "tls": {
      "ca_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
      "insecure_skip_verify": false
    }
  }
}
```

### 3.2 Instant-Revocation Financial Endpoint (`cache_ttl_seconds: 0`)

```json
{
  "id": "oauth2_introspection",
  "config": {
    "introspection_url": "https://fapi-idp.internal:8443/oauth2/introspect",
    "client_id": "velda-fapi",
    "client_secret": "fapi-secret",
    "auth_method": "client_secret_post",
    "token_headers": ["authorization"],
    "token_cookies": [],
    "token_query_params": [],
    "token_type_hint": "access_token",
    "cache_ttl_seconds": 0,
    "negative_cache_ttl_seconds": 10,
    "keepalive_timeout_secs": 60,
    "pool_max_idle_connections": 32,
    "request_timeout_ms": 800,
    "required_scopes": ["payments:write"],
    "claims_to_headers": [{ "claim": "sub", "header": "x-psu-id" }],
    "hide_credentials": true,
    "fallback_behavior": "reject"
  }
}
```

---

## 4. Incident Triage Playbook

| Symptom | Root Cause | Action |
| :--- | :--- | :--- |
| Sync rejected: `unknown field 'max_cache_entries'` | Legacy config key from prior version. | Delete `"max_cache_entries"`; L3 cache self-sizes within 10% host RAM. |
| Spike in `502 Bad Gateway` | IdP unreachable, TLS cert/CA mismatch under `https://`, DNS lookup failed, or `request_timeout_ms` exceeded. | Verify IdP health and TLS chain; check `/etc/hosts` or `/etc/resolv.conf`; test with `curl --cacert ... -u client_id:secret -d token=... <introspection_url>`. |
| Spike in `403 Forbidden` (`insufficient_scope`) | User token lacks one or more tokens listed in `required_scopes`. | Inspect IdP `/introspect` JSON `"scope"` field; remember RFC 6749 requires exact space-delimited token equality. |
| Revoked token still accepted briefly | `cache_ttl_seconds` is set high (e.g. `300s`). | Reduce `cache_ttl_seconds` to `15..=30` (for background shadow refresh) or `0` on high-sensitivity routes. |
