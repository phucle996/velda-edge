# JWT Authentication Plugin (`jwt_auth`) — SRE Operational Runbook & Configuration Guide

Operational reference, unified `internal` / `external` schema specification, ETP provider recipes, and incident triage playbooks for `jwt_auth`.

---

## 1. Configuration Parameter Reference

`JwtAuthConfig` enforces `#[serde(deny_unknown_fields)]` under plugin ID `"jwt_auth"`.

### Parameter Specifications

| Parameter | Type | Required | Default | Constraints / Operational Semantics |
| :--- | :---: | :---: | :---: | :--- |
| **`mode`** | `String` | No | `"internal"` | `"internal"` (`MODE_INTERNAL = 0`, static key only, `pbww = 0`) or `"external"` (`MODE_EXTERNAL = 1`, borrows `etp_id` on Shadow Worker). |
| **`etp_id`** | `String` / `null` | Conditional | `null` | **Required** non-empty ETP ID in `mode: "external"`; **forbidden** in `mode: "internal"`. Must reference one of the 11 supported auth/secret `EtpKind` entries in `etp.json`. |
| **`path`** | `String` / `null` | Conditional | `null` | Provider-specific HTTP URI path / SecretId / S3 object key. Forbidden in `internal` mode. Required for `aws_secrets_manager` and `s3`; optional for IdPs and `vault` (uses provider default path when `null`). |
| **`algorithm`** | `String` / `null` | Conditional | `null` | Supported: `"HS256"`, `"HS384"`, `"HS512"`, `"RS256"`, `"RS384"`, `"RS512"`, `"ES256"`, `"ES384"`, `"EdDSA"`. **Required** whenever `secret_key` or `public_key_pem` is set (`internal` always requires it). Optional (`null`) in pure `external` mode, where ETP keys carry per-slot `u4` `alg`. |
| **`secret_key`** | `String` / `null` | No | `null` | Shared HMAC secret for `HS*` static key. Rejected if set alongside `public_key_pem` or an asymmetric `algorithm`. |
| **`public_key_pem`** | `String` / `null` | No | `null` | ASN.1 PEM public key (`RSA >= 2048-bit`, `P-256`, `P-384`, `Ed25519`) for static key. Rejected if set alongside `secret_key` or symmetric `algorithm`. |
| **`refresh_interval_secs`** | `u64` | No | `300` | External key polling interval in seconds. Clamped to minimum `10` seconds (`velda_core::clock::coarse_sec()`). |
| **`request_timeout_secs`** | `u64` | No | `10` | Per-request ETP read/write timeout in seconds (`0` normalizes to `10`). |
| **`preserve_static_key`** | `bool` | No | `true` | In `external` mode with a static fallback configured, retains the static key as a standby slot behind ETP keys (ETP keys always take priority). |
| **`pbww`** | `u32` | No | `20` | Shadow Worker load-balancing weight (`1..=100` in `external` mode; forced to `0` in `internal` mode). |
| **`token_sources`** | `Array` | **Yes** | — | Non-empty ordered list of `{ "type": "header", "name", "prefix"? }`, `{ "type": "cookie", "name" }`, or `{ "type": "query", "param" }`. |
| **`validate_exp`** | `bool` | **Yes** | — | When `true`, requires numeric `exp` claim and rejects tokens where `now > exp.saturating_add(clock_skew_secs)`. |
| **`clock_skew_secs`** | `u64` | **Yes** | — | Allowed skew in `0..=3600` seconds for `exp` and `nbf`. |
| **`issuer`** | `String` / `null` | No | `null` | Exact expected `iss` claim string, or `null` to disable check. Empty whitespace string is rejected. |
| **`audience`** | `String` / `null` | No | `null` | Expected `aud` string (matches string `aud` or element in array `aud`), or `null` to disable. |
| **`claims_to_headers`** | `Map<String, String>` | **Yes** | — | Dot-path claim $\to$ target RFC 9110 header name (`{}` if none). |
| **`assert_claims`** | `Map<String, String>` | **Yes** | — | Dot-path claim $\to$ expected literal value (`403 Forbidden` on mismatch; `{}` if none). |
| **`cache_ttl_secs`** | `u64` | **Yes** | — | `0..=86400` seconds (`effective_exp = min(exp + skew, now + cache_ttl_secs)`). Negative/Forbidden TTL is `min(10, cache_ttl_secs)`. `0` allocates `0` shard bytes. |

---

## 2. Supported `etp_id` Providers (`velda-etp` Matrix)

| ETP `kind` | Connection Layer Owned by `velda-etp` | Protocol / Auth Semantics Owned by `jwt_auth::providers/<kind>` | Default `path` when omitted |
| :--- | :--- | :--- | :--- |
| `"vault"` | TLS/TCP pool + Token or AppRole login cache | Sends `GET {path}` with `X-Vault-Token` (+ `X-Vault-Namespace`); retries once on `403`; parses JWKS / Transit / KV v2 / KV v1 (`secret_key`, `public_key_pem`, `alg`) | `"/v1/identity/oidc/.well-known/keys"` |
| `"aws_secrets_manager"` | Regional TLS/TCP pool (`secretsmanager.{region}.amazonaws.com`) | Signs `POST /` with AWS4-HMAC-SHA256 (`GetSecretValue` for `SecretId = path`); parses `SecretString` as JWKS or JSON secret | **Required** (`path` = SecretId/ARN) |
| `"s3"` | S3 endpoint TLS/TCP pool + bucket/region config | Signs `GET /{bucket}/{key}` with AWS4-HMAC-SHA256; parses JWKS or JSON secret | **Required** (`path` = object key) |
| `"keycloak"` | IdP host/port/TLS `StreamPool` | `GET {path}` JWKS, handles HTTP/1.1 `chunked`, filters `"use": "enc"` | `"/realms/master/protocol/openid-connect/certs"` |
| `"hydra"` | IdP host/port/TLS `StreamPool` | `GET {path}` Ory Hydra public JWKS endpoint | `"/.well-known/jwks.json"` |
| `"authelia"` | IdP host/port/TLS `StreamPool` | `GET {path}` Authelia OIDC JWKS endpoint | `"/api/oidc/jwks.json"` |
| `"okta"` | Tenant domain/host TLS `StreamPool` | `GET {path}` Okta authorization server JWKS | `"/oauth2/default/v1/keys"` |
| `"auth0"` | Tenant domain/host TLS `StreamPool` | `GET {path}` Auth0 tenant JWKS | `"/.well-known/jwks.json"` |
| `"entra_id"` | `tenant_id` / host TLS `StreamPool` | `GET {path}` (`/{tenant_id}/discovery/v2.0/keys`) Microsoft Entra ID JWKS | `"/{tenant_id}/discovery/v2.0/keys"` |
| `"cognito"` | Regional AWS Cognito IDP TLS `StreamPool` | `GET {path}` AWS Cognito User Pool JWKS | **Recommended** (`"/{user_pool_id}/.well-known/jwks.json"`, fallback `"/.well-known/jwks.json"`) |
| `"oidc"` | Generic OIDC host/endpoint TLS `StreamPool` | `GET {path}` standard RFC 7517 JWKS | `"/.well-known/jwks.json"` |

---

## 3. Scenario-Based Configuration Recipes

### Scenario A: HashiCorp Vault HMAC Rotation (Pure `external` Mode, Zero Static Keys)

`etp.json`:
```json
{
  "id": "vault-auth",
  "kind": "vault",
  "pool": { "pool_size": 2, "connect_timeout_ms": 2000, "idle_timeout_ms": 60000 },
  "settings": {
    "endpoint": "https://vault.internal:8200",
    "role_id": "velda-edge-role",
    "secret_id": "velda-edge-secret",
    "namespace": "prod"
  }
}
```
`plugins.json`:
```json
{
  "id": "jwt_auth",
  "config": {
    "mode": "external",
    "etp_id": "vault-auth",
    "path": "/v1/secret/data/edge/jwt",
    "algorithm": null,
    "secret_key": null,
    "public_key_pem": null,
    "refresh_interval_secs": 120,
    "request_timeout_secs": 5,
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " }
    ],
    "validate_exp": true,
    "clock_skew_secs": 30,
    "claims_to_headers": { "sub": "x-user-id", "roles": "x-user-roles" },
    "assert_claims": {},
    "cache_ttl_secs": 300
  }
}
```

### Scenario B: Keycloak Multi-Key OIDC (`RS256` + Static Fallback)

```json
{
  "id": "jwt_auth",
  "config": {
    "mode": "external",
    "etp_id": "keycloak-main",
    "path": "/realms/prod/protocol/openid-connect/certs",
    "algorithm": "RS256",
    "public_key_pem": "-----BEGIN PUBLIC KEY-----\nMIIBIjAN...\n-----END PUBLIC KEY-----",
    "preserve_static_key": true,
    "refresh_interval_secs": 300,
    "pbww": 10,
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " }
    ],
    "validate_exp": true,
    "clock_skew_secs": 60,
    "issuer": "https://sso.company.com/realms/prod",
    "audience": "velda-gateway",
    "claims_to_headers": { "sub": "x-user-id", "preferred_username": "x-username" },
    "assert_claims": {},
    "cache_ttl_secs": 300
  }
}
```

### Scenario C: Internal East-West Mesh (`mode: "internal"`, Zero Network I/O)

```json
{
  "id": "jwt_auth",
  "config": {
    "mode": "internal",
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
    "claims_to_headers": { "sub": "x-caller-spiffe" },
    "assert_claims": {},
    "cache_ttl_secs": 600
  }
}
```

---

## 4. Wire Observability & Reject Mapping

| Condition | HTTP Wire Response | `WWW-Authenticate` Header | Internal `RejectReason` | gRPC Status |
| :--- | :---: | :--- | :---: | :---: |
| Token absent in all `token_sources` | `401 Unauthorized` | `Bearer realm="velda-edge"` | `RejectReason::Unauthorized` | `UNAUTHENTICATED (16)` |
| Bad dot shape / invalid Base64URL / JSON | `401 Unauthorized` | `Bearer error="invalid_token"` | `RejectReason::Unauthorized` | `UNAUTHENTICATED (16)` |
| `alg` mismatch / unknown `kid` / bad sig | `401 Unauthorized` | `Bearer error="invalid_token"` | `RejectReason::Unauthorized` | `UNAUTHENTICATED (16)` |
| Empty `JwksStore` during cold boot (`NoActiveKeys`) | `401 Unauthorized` | `Bearer error="invalid_token"` | `RejectReason::Unauthorized` | `UNAUTHENTICATED (16)` *(not negative-cached)* |
| Expired `exp` / premature `nbf` / bad `iss` or `aud` | `401 Unauthorized` | `Bearer error="invalid_token"` | `RejectReason::Unauthorized` | `UNAUTHENTICATED (16)` |
| `assert_claims` mismatch | `403 Forbidden` | *(omitted)* | `RejectReason::Forbidden` | `PERMISSION_DENIED (7)` |

---

## 5. Incident Triage Playbook

| Symptom | Probable Root Cause | Resolution |
| :--- | :--- | :--- |
| Sync rejected: `mode 'internal' forbids 'etp_id'` / `mode 'external' requires non-empty 'etp_id'` | Config forgot `"mode": "external"` when configuring `etp_id`, or left `etp_id` set in `internal` mode. | Set `"mode": "external"` with `"etp_id"` (or remove `etp_id`/`path` for static `internal` mode). |
| Sync rejected: `static key ... requires 'algorithm'` | Static `secret_key` or `public_key_pem` supplied with `"algorithm": null`. | Specify `"algorithm"` for the static key (external ETP keys do not need plugin-level `"algorithm"`). |
| Sync rejected: `requires 'path' (SecretId)` or `'path' (object key)` | `aws_secrets_manager` or `s3` ETP used without `"path"`. | Set `"path"` to the AWS secret ID/ARN or S3 object key. |
| Brief `401` right at boot without static fallback (`NoActiveKeys`) | External ETP initial fetch is in flight. | Recovers automatically after first `sync_keys_once()`; `NoActiveKeys` skips negative caching so valid client tokens are not blacklisted. Optional: provide static standby + `"preserve_static_key": true`. |
