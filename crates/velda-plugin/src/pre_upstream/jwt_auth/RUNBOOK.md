# JWT Authentication Plugin (`jwt_auth`) — SRE Operational Runbook & Configuration Guide

This document is the operational playbook and configuration guide (**SRE Runbook & Tuning Guide**) for engineers configuring, operating, and troubleshooting JWT authentication in Velda Edge.

---

## 1. Configuration Parameter Reference

Velda Edge enforces a **100% Explicit Configuration Invariant**: all parameters must be explicitly declared in `plugins.json`. No implicit defaults exist; unused options must be explicitly supplied as `null` or `{}`.

```json
{
  "id": "api_jwt_auth",
  "phase": "pre_upstream",
  "config": {
    "algorithm": "RS256",
    "secret_key": null,
    "public_key_pem": "-----BEGIN PUBLIC KEY-----\nMIIBIj...",
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " }
    ],
    "validate_exp": true,
    "clock_skew_secs": 60,
    "issuer": "https://auth.example.com",
    "audience": "api.velda.io",
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

| Parameter | Type | Required | Allowed Values / Constraints | Operational Meaning & Security Impact |
| :--- | :---: | :---: | :--- | :--- |
| **`algorithm`** | `String` | **Yes** | `"HS256"`, `"HS384"`, `"HS512"`, `"RS256"`, `"RS384"`, `"RS512"`, `"ES256"`, `"ES384"`, `"EdDSA"` | **Algorithm Pinning**: The route will strictly accept only tokens signed with this exact algorithm. Prevents algorithm substitution attacks (such as switching to `alg: "none"` or tricking RSA verifiers with HMAC). |
| **`secret_key`** | `String` / `null` | **Yes** | Non-empty string for HMAC (`HS*`), `null` for asymmetric | Shared secret key for symmetric HMAC verification. Must be `null` when using asymmetric algorithms (`RS*`, `ES*`). |
| **`public_key_pem`** | `String` / `null` | **Yes** | Valid PEM string for asymmetric, `null` for HMAC or when dynamic keys are supplied by `jwks_rotator` | Static PEM-encoded public key (X.509 SPKI or PKCS#1). Acts as Slot 0 key when paired with `jwks_rotator`. |
| **`token_sources`** | `Array` | **Yes** | Non-empty array of extraction source descriptors | Ordered list of token extraction rules evaluated sequentially until a token is found. Supported types: `header`, `cookie`, `query`. |
| **`validate_exp`** | `bool` | **Yes** | `true` or `false` | When `true`, enforces expiration check: `now_epoch > exp + clock_skew_secs` triggers `401 Unauthorized`. Evaluated against edge ingress time (zero clock syscalls). |
| **`clock_skew_secs`** | `u64` | **Yes** | `0 ..= 300` seconds | Tolerance window in seconds to absorb clock drift between Identity Provider and edge nodes. Recommended: `60s`. Set to `0` for strict zero-tolerance environments. |
| **`issuer`** | `String` / `null` | **Yes** | Expected `iss` claim string, or `null` to bypass | Rejects tokens with `403 Forbidden` if the `iss` claim does not exactly match this string. |
| **`audience`** | `String` / `null` | **Yes** | Expected `aud` claim string, or `null` to bypass | Rejects tokens with `403 Forbidden` if `aud` claim is missing or does not match (supports both string claims and JSON array claims). |
| **`claims_to_headers`** | `Map<String, String>` | **Yes** | Map of claim path to upstream header name (`{}` if none) | Extracts token claims and injects them as headers for upstream microservices. Supports dot notation for nested claims (e.g. `"user.id": "x-user-id"`). |
| **`assert_claims`** | `Map<String, String>` | **Yes** | Map of claim path to required literal value (`{}` if none) | Hard access control assertions. If any claim differs from the configured literal, the gateway rejects the request with `403 Forbidden`. |
| **`cache_ttl_secs`** | `u64` | **Yes** | `0 ..= 86400` seconds | Positive authentication cache TTL. When $> 0$, verified tokens are cached in the lock-striped Dual Cache, bypassing cryptographic math on repeated requests. `0` disables caching. |

### Token Source Format

```json
// 1. HTTP Header Source (Evaluates Authorization: Bearer <tok>)
{ "type": "header", "name": "authorization", "prefix": "Bearer " }

// 2. Cookie Source (Evaluates Cookie: access_token=<tok>)
{ "type": "cookie", "name": "access_token" }

// 3. Query Parameter Source (Evaluates ?token=<tok>)
{ "type": "query", "param": "token" }
```

---

## 2. Scenario-Based Configuration Recipes

Use these validated configuration presets for your architecture:

### Scenario A: Zero-Trust Public Edge Gateway (Auth0 / Okta / Entra ID)
*Goal: Asymmetric security, dynamic key rotation support, claim injection into upstream headers.*

```json
{
  "id": "public_jwt_auth",
  "phase": "pre_upstream",
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
      "email": "x-user-email",
      "roles": "x-user-roles",
      "tenant_id": "x-tenant-id"
    },
    "assert_claims": {},
    "cache_ttl_secs": 300
  }
}
```
* **Why this setup**: `public_key_pem` is set to `null` because public keys are dynamically populated by a companion `jwks_rotator`. `cache_ttl_secs: 300` ensures repeated requests from active users skip cryptographic verification, delivering high throughput.

---

### Scenario B: High-Throughput Internal Service Mesh (Zero-Alloc Symmetric)
*Goal: Maximum RPS, lowest latency between microservices.*

```json
{
  "id": "mesh_jwt_auth",
  "phase": "pre_upstream",
  "config": {
    "algorithm": "HS256",
    "secret_key": "env:MESH_SHARED_SECRET_KEY_STRONG_256",
    "public_key_pem": null,
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " }
    ],
    "validate_exp": true,
    "clock_skew_secs": 30,
    "issuer": "internal-mesh",
    "audience": "payment-service",
    "claims_to_headers": {
      "service": "x-caller-service",
      "role": "x-caller-role"
    },
    "assert_claims": {},
    "cache_ttl_secs": 600
  }
}
```
* **Why this setup**: HMAC-SHA256 (`HS256`) runs orders of magnitude faster than asymmetric algorithms. With `cache_ttl_secs: 600`, verification cost is amortized across thousands of microservice calls.

---

### Scenario C: Strict Financial-Grade / Banking API (FAPI Compliance)
*Goal: Highest cryptographic assurance, zero clock drift tolerance, mandatory claim assertions.*

```json
{
  "id": "banking_jwt_auth",
  "phase": "pre_upstream",
  "config": {
    "algorithm": "ES256",
    "secret_key": null,
    "public_key_pem": "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE...",
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " }
    ],
    "validate_exp": true,
    "clock_skew_secs": 0,
    "issuer": "https://auth.bank.com",
    "audience": "https://fapi.bank.com/v1",
    "claims_to_headers": {
      "sub": "x-client-id",
      "account_id": "x-account-id"
    },
    "assert_claims": {
      "acr": "urn:mace:incommon:iap:silver"
    },
    "cache_ttl_secs": 0
  }
}
```
* **Why this setup**: Uses NIST P-256 Elliptic Curve (`ES256`). `clock_skew_secs: 0` ensures expired tokens are rejected immediately with no grace window. `cache_ttl_secs: 0` disables caching so every single banking transaction undergoes live cryptographic verification.

---

### Scenario D: Multi-Channel Web & Mobile Gateway (Header + Cookie + Query Fallback)
*Goal: Flexible token extraction supporting single-page apps, cookies, and WebSocket upgrades.*

```json
{
  "id": "omnichannel_jwt_auth",
  "phase": "pre_upstream",
  "config": {
    "algorithm": "RS256",
    "secret_key": null,
    "public_key_pem": "-----BEGIN PUBLIC KEY-----\nMIIBIj...",
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " },
      { "type": "cookie", "name": "auth_token" },
      { "type": "query", "param": "access_token" }
    ],
    "validate_exp": true,
    "clock_skew_secs": 60,
    "issuer": null,
    "audience": null,
    "claims_to_headers": {
      "sub": "x-user-id"
    },
    "assert_claims": {},
    "cache_ttl_secs": 180
  }
}
```
* **Why this setup**: Evaluates the `Authorization` header first. If absent, checks the browser `auth_token` cookie. For WebSocket handshakes where headers cannot be modified by browser JS, falls back to `?access_token=<tok>`.

---

## 3. Emergency Incident Triage Matrix

| Severity | Symptoms & Alerts | Root Cause Analysis | Rapid Mitigation Action | SLA |
| :---: | :--- | :--- | :--- | :---: |
| **P0** | 100% of traffic rejected with `401 Unauthorized` | Public key corrupted, PEM mismatch, or companion `jwks_rotator` uninitialized | Verify `public_key_pem` or check `jwks_rotator` sync status; inspect `WWW-Authenticate` response | **< 5 min** |
| **P1** | Spike in `401 Unauthorized` with `Token has expired` | Edge server clock drifted significantly ($> \text{clock\_skew\_secs}$) or client token refresh failed | Sync NTP on edge nodes (`chronyd`); temporarily increase `clock_skew_secs` to `120` | **< 15 min** |
| **P2** | Elevated `403 Forbidden` with `Issuer mismatch` or `Audience mismatch` | Identity Provider changed default `iss` URL (e.g. trailing slash `/`) or new client using wrong `aud` | Check JWT payload using `jq`; synchronize `issuer` and `audience` in configuration | **< 30 min** |
| **P3** | High CPU usage on edge workers under invalid token attacks | Malicious caller flooding with forged tokens attempting cryptographic exhaustion | Verify Negative Cache is active; apply rate limiting via `rate_limit` plugin | **< 1 hour** |

---

## 4. Diagnostic CLI Toolkit

When an alert fires, use these CLI commands to diagnose authentication issues:

### 4.1 Decode Inbound JWT Token without External Tools
```bash
# Decode JOSE Header
echo "<JWT_TOKEN>" | cut -d'.' -f1 | base64 -d 2>/dev/null | jq .

# Decode Claims Payload
echo "<JWT_TOKEN>" | cut -d'.' -f2 | base64 -d 2>/dev/null | jq .
```

### 4.2 Inspect Edge Gateway Authentication Response
```bash
# Test with Authorization Header
curl -iv -H "Authorization: Bearer <TOKEN>" http://127.0.0.1:8080/api/v1/resource

# Check failure reason from response headers
# Look for: WWW-Authenticate: Bearer realm="velda-edge", error="..."
```

### 4.3 Inspect `jwt_auth` Operational Logs
```bash
# Filter journalctl for authentication rejections
journalctl -u velda-edge -n 100 --no-pager | grep -E "jwt_auth|Malformed JWT|Algorithm mismatch"
```

---

## 5. Step-by-Step Incident Resolution Playbooks

### Playbook 1: Spike in `401 Unauthorized` (Token Expired or Key Mismatch)
1. **Extract and inspect sample failing token**:
   ```bash
   TOKEN="<paste_sample_token>"
   EXP=$(echo "$TOKEN" | cut -d'.' -f2 | base64 -d 2>/dev/null | jq -r .exp)
   NOW=$(date +%s)
   echo "Token Exp: $EXP, Current Time: $NOW, Diff: $((NOW - EXP))s"
   ```
2. **Evaluate Root Cause**:
   - If `Diff > 0` and greater than `clock_skew_secs`: Client is failing to refresh tokens.
   - If `Diff < 0` but still rejected: Check `kid` header:
     ```bash
     echo "$TOKEN" | cut -d'.' -f1 | base64 -d 2>/dev/null | jq -r .kid
     ```
     Verify that this `kid` is present in the `jwks_rotator` table or matches the static key.

---

### Playbook 2: Spike in `403 Forbidden` (Claims Assertion / Issuer Mismatch)
1. **Inspect Token Claims against Configuration**:
   ```bash
   echo "$TOKEN" | cut -d'.' -f2 | base64 -d 2>/dev/null | jq '{iss: .iss, aud: .aud}'
   ```
2. **Common Pitfalls**:
   - **Trailing slash issue**: `issuer: "https://auth.company.com"` vs `"https://auth.company.com/"`.
   - **Audience as array**: If token has `aud: ["api://default", "https://api.company.com"]`, ensure `audience` in config matches one of the array items.

---

### Playbook 3: Cryptographic Exhaustion Attack Mitigation
1. **Verify if negative cache is absorbing bad tokens**:
   - The negative cache automatically caches hashes of failed tokens for 10 seconds, returning fast rejection without running cryptographic math.
2. **Apply Route-Level Rate Limiting**:
   - If malicious traffic persists, engage the `rate_limit` plugin ahead of `jwt_auth` to drop traffic at L4/L7 before running CPU-intensive operations.
