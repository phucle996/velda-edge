# OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`) — SRE Operational Runbook

Operational guide, complete schema specification, ETP provider recipes, revocation-window tuning, and troubleshooting playbooks for `oauth2_introspection`.

---

## 1. Configuration Parameter Reference

`Oauth2IntrospectionConfig` and `ClaimToHeaderConfig` enforce `#[serde(deny_unknown_fields)]`. Transport pooling (`pool_size`, burst spillover, idle timeout) and TLS/mTLS configuration live solely in the referenced `velda-etp` entry (`etp_id`).

### Parameter Specifications

| Parameter | Type | Required | Constraints / Allowed Values | Operational & Security Impact |
| :--- | :---: | :---: | :--- | :--- |
| **`etp_id`** | `String` | **Yes** | Non-empty ETP ID registered in `etp.json` | Connects plugin to one of the 9 supported `velda-etp` pools (`keycloak`, `hydra`, `authelia`, `okta`, `oidc`, `auth0`, `entra_id`, `cognito`, `vault`). |
| **`path`** | `Option<String>` | No | Relative HTTP path (no whitespace/control chars or `http(s)://` scheme) | Overrides provider default path when omitted/null. |
| **`client_id`** | `Option<String>` | No | Non-empty string if present | Optional plugin-level OAuth2 client ID; overrides `client_id` configured in the attached ETP entry. |
| **`client_secret`** | `Option<String>` | No | Non-empty string if present | Optional plugin-level OAuth2 client secret; overrides `client_secret` configured in the attached ETP entry. |
| **`auth_method`** | `String` | No | `"client_secret_basic"` (default), `"client_secret_post"`, or `"none"` | Controls client auth on RFC 7662 POST endpoints (`"none"` skips client creds for Hydra admin ports, `/userinfo`, or Vault token lookup). |
| **`token_headers`** | `Vec<String>` | **Yes**\* | Valid RFC 9110 header names (normalized lowercase) | Checked in order; strips optional case-insensitive `"Bearer "` prefix. |
| **`token_cookies`** | `Vec<String>` | **Yes**\* | Non-empty cookie names | Checked across all `cookie` headers (supports RFC 9113 split HTTP/2 cookies). |
| **`token_query_params`** | `Vec<String>` | **Yes**\* | Non-empty query parameter names | Fallback lookup from URI query string (`?token=...`). |
| **`token_type_hint`** | `String` | **Yes** | `"access_token"`, `"refresh_token"`, or `"none"` | Sent in RFC 7662 form body (`"none"` omits parameter). |
| **`cache_ttl_seconds`** | `u64` | **Yes** | `0 ..= 86400` seconds | Positive L3/Cold cache TTL (`0` disables positive cache for instant revocation). When `> 3s`, background stale refresh runs in final `clamp(ttl/10, 3, 10)` seconds. |
| **`negative_cache_ttl_seconds`** | `u64` | **Yes** | `0 ..= 86400` seconds | Caches inactive/expired tokens (`401`) in Hot L3 only (`0` disables; never demotes into Cold DRAM). |
| **`request_timeout_ms`** | `u64` | **Yes** | `1 ..= 60000` ms | Bounded async write/flush/read timeout per provider HTTP roundtrip. |
| **`required_scopes`** | `Vec<String>` | **Yes** | Space-free RFC 6749 scope tokens (`[]` if none) | All scopes must exist in response `scope`, else returns `403 Forbidden`. |
| **`claims_to_headers`** | `Vec<Object>` | **Yes** | `[{ "claim": "<dot.path>", "header": "<rfc9110>" }]` (`[]` if none) | Projects **present** claims via `insert_prehashed`. Absent claims leave request headers intact. |
| **`hide_credentials`** | `bool` | **Yes** | `true` or `false` | Strips matched token header before forwarding upstream when `true`. |
| **`fallback_behavior`** | `String` | **Yes** | `"reject"` or `"allow"` | Failure policy when IdP/Vault is unreachable or returns `5xx` (`502 Bad Gateway` vs fail-open `Continue`). |

\* At least one of `token_headers`, `token_cookies`, or `token_query_params` must be non-empty.

---

## 2. Supported ETP Provider Matrix

| ETP `kind` | Default `path` | Default Request Mode | Custom Notes |
| :--- | :--- | :--- | :--- |
| **`keycloak`** | `/realms/master/protocol/openid-connect/token/introspect` | RFC 7662 `POST` (`client_secret_basic` / `post` / `none`) | Inherits ETP `bearer_token` & `custom_headers`. |
| **`hydra`** | `/admin/oauth2/introspect` | RFC 7662 `POST` | Commonly used with `auth_method: "none"` on internal admin port. |
| **`authelia`** | `/api/oidc/introspection` | RFC 7662 `POST` | Requires registered confidential OIDC client creds. |
| **`okta`** | `/oauth2/default/v1/introspect` | RFC 7662 `POST` | Override `path` when using a custom Authorization Server ID. |
| **`oidc`** | `/oauth2/introspect` | RFC 7662 `POST` | Generic OIDC/OAuth2 provider fallback. |
| **`auth0`** | `/userinfo` | `GET` `Authorization: Bearer <token>` | Set custom `path` + `client_id`/`client_secret` + `client_secret_basic`/`post` to force RFC 7662 `POST`. |
| **`entra_id`** | `/oidc/userinfo` | `GET` `Authorization: Bearer <token>` | Microsoft Entra ID OIDC UserInfo (`200` -> `active: true`, `401/403` -> `active: false`). |
| **`cognito`** | `/oauth2/userInfo` | `GET` `Authorization: Bearer <token>` | AWS Cognito hosted userInfo validation. |
| **`vault`** | `/v1/auth/token/lookup` | `POST` `{token}` w/ `X-Vault-Token` (or `GET /lookup-self`) | Maps `data.entity_id`/`display_name` -> `sub`, `data.policies` -> `scope`, `data.ttl` -> `exp`. Also supports `/v1/identity/oidc/introspect`. |

---

## 3. Managing Revocation Windows & Header Hygiene

- **Sensitive Routes (Payments / Password Reset)**: Set `"cache_ttl_seconds": 0` and `"negative_cache_ttl_seconds": 10`. Every valid token checks live state with the provider while invalid sprays stay blocked in Hot L3.
- **Standard API Routes**: Set `"cache_ttl_seconds": 15..=30`. Warm hits resolve synchronously at ~15 ns; stale hits trigger non-blocking background refresh on `velda-shadow-{id}`.
- **Header Hygiene**: `claims_to_headers` only overwrites claims present in the response. If backends trust headers like `x-user-roles` that might be omitted when empty, add a `header_mutation` plugin ahead of `oauth2_introspection` to strip untrusted incoming client headers first.

---

## 4. Scenario Configuration Recipes

### 4.1 Ory Hydra Admin Introspection (`auth_method: "none"`)

```json
{
  "id": "oauth2_introspection",
  "config": {
    "etp_id": "hydra-admin",
    "auth_method": "none",
    "token_headers": ["authorization"],
    "token_cookies": [],
    "token_query_params": [],
    "token_type_hint": "access_token",
    "cache_ttl_seconds": 30,
    "negative_cache_ttl_seconds": 10,
    "request_timeout_ms": 500,
    "required_scopes": ["api.read"],
    "claims_to_headers": [{ "claim": "sub", "header": "x-user-id" }],
    "hide_credentials": true,
    "fallback_behavior": "reject"
  }
}
```

### 4.2 HashiCorp Vault Token Lookup ETP

```json
{
  "id": "oauth2_introspection",
  "config": {
    "etp_id": "vault-corp",
    "auth_method": "none",
    "token_headers": ["x-vault-token", "authorization"],
    "token_cookies": [],
    "token_query_params": [],
    "token_type_hint": "none",
    "cache_ttl_seconds": 60,
    "negative_cache_ttl_seconds": 10,
    "request_timeout_ms": 500,
    "required_scopes": ["app-read"],
    "claims_to_headers": [{ "claim": "sub", "header": "x-entity-id" }],
    "hide_credentials": true,
    "fallback_behavior": "reject"
  }
}
```

---

## 5. Incident Triage Playbook

| Symptom | Root Cause | Action |
| :--- | :--- | :--- |
| Sync rejected: `unknown field 'introspection_url'` / `'tls'` / `'keepalive_timeout_secs'` | Legacy pre-ETP schema fields present. | Move host/endpoint, TLS/CA, and pool settings to `etps[]` in `etp.json` and reference via `etp_id`. |
| Startup error: `requires client_id and client_secret ... when auth_method != 'none'` | `client_secret_basic` / `client_secret_post` selected without credentials in plugin or ETP config. | Supply `client_id` + `client_secret` in plugin/ETP config, or set `"auth_method": "none"` for Hydra admin / `/userinfo` / Vault. |
| Spike in `502 Bad Gateway` | Referenced ETP endpoint unreachable, TLS failure, timeout (`request_timeout_ms`), or `5xx` from IdP. | Inspect `etp.json` endpoint/CA settings and IdP health logs; temporarily switch `"fallback_behavior": "allow"` only if fail-open is intended. |
| Spike in `403 Forbidden` (`insufficient_scope`) | Token lacks one or more tokens listed in `required_scopes`. | Verify IdP `scope` string or Vault `policies` array matches exact space-delimited scope tokens. |
