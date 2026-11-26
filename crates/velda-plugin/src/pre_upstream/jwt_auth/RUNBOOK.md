# JWT Authentication Plugin (`jwt_auth`) — SRE Operational Runbook & Configuration Guide

Operational reference, strict schema specification, scenario presets, and incident response playbooks for `jwt_auth` in Velda Edge.

---

## 1. Configuration Parameter Reference

`JwtAuthConfig` enforces `#[serde(deny_unknown_fields)]`. The plugin identifier in `plugins.json` must be `"jwt_auth"`.

```json
{
  "id": "jwt_auth",
  "config": {
    "algorithm": "RS256",
    "secret_key": null,
    "public_key_pem": null,
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " }
    ],
    "validate_exp": true,
    "clock_skew_secs": 60,
    "issuer": "https://auth.example.com/",
    "audience": "https://api.example.com",
    "claims_to_headers": {
      "sub": "x-user-id",
      "roles": "x-user-roles"
    },
    "assert_claims": {},
    "cache_ttl_secs": 300
  }
}
```

### Parameter Specifications

| Parameter | Type | Required | Constraints / Allowed Values | Operational & Security Semantics |
| :--- | :---: | :---: | :--- | :--- |
| **`algorithm`** | `String` | **Yes** | `"HS256"`, `"HS384"`, `"HS512"`, `"RS256"`, `"RS384"`, `"RS512"`, `"ES256"`, `"ES384"`, `"EdDSA"` (case-insensitive, normalized) | **Algorithm Pinning**: Enforces a 3-way match between route config, JOSE `alg`, and key slot `alg`. Blocks `alg: "none"` and RSA/HMAC substitution attacks. |
| **`secret_key`** | `String` / `null` | No (`null`) | Non-empty string for `HS*`; must be `null`/omitted for asymmetric | Symmetric HMAC shared secret. Rejected at sync time if set alongside an asymmetric algorithm. |
| **`public_key_pem`** | `String` / `null` | No (`null`) | Valid PEM/DER public key (`RSA >= 2048-bit`, `P-256`, `P-384`, `Ed25519`), or `null` | Parsed and validated at `velda-sync` staging time. When `null` on an asymmetric algorithm, an active `jwks_rotator` targeting `"jwt_auth"` is mandatory. |
| **`token_sources`** | `Array` | **Yes** | Non-empty array of `header`, `cookie`, or `query` descriptors (`deny_unknown_fields`) | Evaluated in declaration order until a non-empty token is extracted. Header and Cookie sources inspect all matching header fields (supporting HTTP/2 split `cookie` headers). |
| **`validate_exp`** | `bool` | **Yes** | `true` or `false` | When `true`, requires a valid numeric `exp` claim and rejects when `now > exp + clock_skew_secs` (`401 Unauthorized`). |
| **`clock_skew_secs`** | `u64` | **Yes** | `0 ..= 3600` seconds | Clock drift tolerance applied with `saturating_add` to both `exp` and `nbf` evaluations. |
| **`issuer`** | `String` / `null` | No (`null`) | Non-empty string, or `null` to disable | Exact match against token `iss` claim. Mismatch returns `401 Unauthorized` (`Bearer error="invalid_token"`). Empty string `""` is rejected at sync time. |
| **`audience`** | `String` / `null` | No (`null`) | Non-empty string, or `null` to disable | Matches either string `aud` or any element of array `aud`. Mismatch returns `401 Unauthorized`. Empty string `""` is rejected at sync time. |
| **`claims_to_headers`** | `Map<String, String>` | **Yes** | Dot-separated non-empty claim path $\to$ valid RFC 9110 header name (`{}` if none) | Converts **present** claims into upstream headers (`insert_prehashed`: adds if missing, overwrites if present). Absent claims leave existing request headers untouched. |
| **`assert_claims`** | `Map<String, String>` | **Yes** | Dot-separated non-empty claim path $\to$ expected literal value (`{}` if none) | RBAC / tenant guard evaluated on strings, booleans, numbers, or array membership. Mismatch returns `403 Forbidden` and caches the `403` decision for `min(10, cache_ttl_secs)`. |
| **`cache_ttl_secs`** | `u64` | **Yes** | `0 ..= 86400` seconds | Positive L3 SIMD cache ceiling (`effective_exp = min(exp + skew, now + cache_ttl_secs)`). Negative/Forbidden TTL is `min(10, cache_ttl_secs)`. `0` disables caching (`0` bytes allocated). |

### `claims_to_headers` Operational Contract

`claims_to_headers` strictly projects claims **present** in the verified JWT payload:
- **Claim present in JWT**: Inserts the header if absent, or overwrites all existing occurrences of that header name in `HeaderWorkspace`.
- **Claim absent from JWT**: Performs **no deletion**, preserving legitimate client-supplied fallback headers (e.g., mapping an optional `"locale"` claim to `"accept-language"`).
- **Auth-Offloaded Legacy Backends**: If an upstream backend does not inspect the JWT itself and blindly trusts an optional identity header (such as `x-user-roles`) when the claim may be omitted from normal user tokens, place `header_mutation` with `remove_headers: ["x-user-roles"]` **before** `"jwt_auth"` in the route's FIFO `plugins` array.

---

## 2. Scenario-Based Configuration Recipes

### Scenario A: Cloud OIDC Edge Gateway with Live JWKS Rotation (`RS256` + `jwks_rotator`)

```json
[
  {
    "id": "jwt_auth",
    "config": {
      "algorithm": "RS256",
      "secret_key": null,
      "public_key_pem": null,
      "token_sources": [
        { "type": "header", "name": "authorization", "prefix": "Bearer " }
      ],
      "validate_exp": true,
      "clock_skew_secs": 60,
      "issuer": "https://auth.company.com/",
      "audience": "https://api.company.com",
      "claims_to_headers": {
        "sub": "x-user-id",
        "roles": "x-user-roles",
        "profile.tenant_id": "x-tenant-id"
      },
      "assert_claims": {},
      "cache_ttl_secs": 300
    }
  },
  {
    "id": "jwks_rotator",
    "config": {
      "target_jwt_auth_id": "jwt_auth",
      "jwks_uri": "https://auth.company.com/.well-known/jwks.json",
      "refresh_interval_secs": 300,
      "request_timeout_secs": 10,
      "preserve_static_key": false
    }
  }
]
```

### Scenario B: Internal East-West Service Mesh (`EdDSA` / `HS256`)

```json
{
  "id": "jwt_auth",
  "config": {
    "algorithm": "HS256",
    "secret_key": "replace-with-strong-32-byte-random-mesh-secret",
    "public_key_pem": null,
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " }
    ],
    "validate_exp": true,
    "clock_skew_secs": 30,
    "issuer": "velda-internal-mesh",
    "audience": "orders-service",
    "claims_to_headers": {
      "sub": "x-caller-spiffe"
    },
    "assert_claims": {},
    "cache_ttl_secs": 600
  }
}
```

### Scenario C: Zero-Cache Financial Grade API (`ES256`, Live Per-Request Verification)

```json
{
  "id": "jwt_auth",
  "config": {
    "algorithm": "ES256",
    "secret_key": null,
    "public_key_pem": "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE...\n-----END PUBLIC KEY-----",
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " }
    ],
    "validate_exp": true,
    "clock_skew_secs": 0,
    "issuer": "https://fapi.bank.example",
    "audience": "https://payments.bank.example/v1",
    "claims_to_headers": {
      "sub": "x-psu-id"
    },
    "assert_claims": {
      "acr": "urn:openbanking:psd2:sca"
    },
    "cache_ttl_secs": 0
  }
}
```

---

## 3. Wire Observability & Reject Mapping

| Condition | HTTP Wire Response | `WWW-Authenticate` Header | Internal `RejectReason` | gRPC Status |
| :--- | :---: | :--- | :---: | :---: |
| Token absent in all `token_sources` | `401 Unauthorized` | `Bearer realm="velda-edge"` | `RejectReason::Unauthorized` | `UNAUTHENTICATED (16)` |
| Bad dot shape / invalid Base64URL / JSON | `401 Unauthorized` | `Bearer error="invalid_token"` | `RejectReason::Unauthorized` | `UNAUTHENTICATED (16)` |
| `alg` mismatch / unknown `kid` / bad sig | `401 Unauthorized` | `Bearer error="invalid_token"` | `RejectReason::Unauthorized` | `UNAUTHENTICATED (16)` |
| Expired `exp` / premature `nbf` / bad `iss` or `aud` | `401 Unauthorized` | `Bearer error="invalid_token"` | `RejectReason::Unauthorized` | `UNAUTHENTICATED (16)` |
| `assert_claims` mismatch | `403 Forbidden` | *(omitted)* | `RejectReason::Forbidden` | `PERMISSION_DENIED (7)` |

---

## 4. Incident Triage Playbook

### 4.1 Decode Base64URL JWT Segments Safely in Shell
```bash
b64url_decode() {
  local s="$1"
  local pad=$(( (4 - ${#s} % 4) % 4 ))
  printf '%s' "$s" | tr '_-' '/+' | awk -v p="$pad" '{ printf "%s", $0; for(i=0;i<p;i++) printf "=" }' | base64 -d 2>/dev/null
}

TOKEN="<paste_jwt_here>"
b64url_decode "$(printf '%s' "$TOKEN" | cut -d'.' -f1)" | jq .
b64url_decode "$(printf '%s' "$TOKEN" | cut -d'.' -f2)" | jq .
```

### 4.2 Common Failure Modes & Resolutions

| Symptom | Probable Root Cause | Resolution |
| :--- | :--- | :--- |
| Sync rejected: `requires an active companion 'jwks_rotator'` | Asymmetric algorithm configured with `public_key_pem: null` without a `jwks_rotator` plugin targeting `"jwt_auth"`. | Either configure a valid `public_key_pem` or add `jwks_rotator` with `"target_jwt_auth_id": "jwt_auth"`. |
| Sync rejected: `unknown field` | Legacy fields (`cache_shards`, `cache_shard_capacity`, `negative_cache_ttl_secs`) left in `plugins.json`. | Remove unknown fields; `JwtCache` geometry is hardware-aligned automatically from `cache_ttl_secs`. |
| `401` with `Bearer error="invalid_token"` despite valid signature | `issuer` trailing slash mismatch (`https://idp/` vs `https://idp`), `audience` mismatch, or host NTP drift exceeding `clock_skew_secs`. | Inspect decoded `iss`, `aud`, `exp`, `nbf` via helper above and verify `chronyc tracking` on edge hosts. |
| Brief `401` right at cold process boot with `public_key_pem: null` | `jwks_rotator` initial HTTPS fetch is still in flight (`NoActiveKeys`). | Automatic recovery once initial fetch completes (`NoActiveKeys` intentionally skips negative caching so valid tokens are never blacklisted during boot). |
