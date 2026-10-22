# JWT Authentication Plugin (`jwt_auth`)

Enterprise-grade L7 ingress authentication engine providing route-scoped JSON Web Token (RFC 7519) cryptographic signature verification, claim assertions, downstream header injection, and lock-striped Dual Cache (`Positive Cache` + `Negative Cache`) for sub-20ns evaluation.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `jwt_auth` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Write)` (Injects claims into upstream headers) |
| **Payload Passthrough** | **100% Zero-Copy streaming**. Validated request bodies stream unbuffered |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Hot-Path Overhead** | ~20ns on positive cache hits; constant-time cryptographic verification on cache misses |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All fields must be intentionally supplied, including `null` or empty maps `{}` for unused options.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `algorithm` | String | **Yes** | `"HS256"`, `"HS384"`, `"HS512"`, `"RS256"`, `"RS384"`, `"RS512"`, `"ES256"`, `"ES384"`, `"EdDSA"` | `"RS256"` (asymmetric) / `"HS256"` (symmetric) | Cryptographic signing algorithm. Pinning defeats algorithm confusion attacks. |
| `secret_key` | String / null | **Yes** | Mandatory non-empty string for HMAC (`HS*`), `null` for asymmetric | `null` (for RS256) | Shared secret key for symmetric HMAC signing. |
| `public_key_pem` | String / null | **Yes** | Mandatory PEM string for asymmetric (`RS*`, `ES*`, `EdDSA`), `null` for HMAC | `"-----BEGIN PUBLIC KEY-----\n..."` | PEM-encoded public key for asymmetric signature verification. |
| `token_sources` | Array of objects | **Yes** | Non-empty array of extraction source descriptors (`header`, `cookie`, or `query`) | `[{"type":"header","name":"authorization","prefix":"Bearer "}]` | Token extraction locations evaluated in priority order. |
| `validate_exp` | Boolean | **Yes** | `true` or `false` | `true` | Enforces expiration (`exp`) claim verification against current time. |
| `clock_skew_secs` | Integer (u64) | **Yes** | `0 ..= 300` seconds | `60` | Clock drift tolerance window in seconds. |
| `issuer` | String / null | **Yes** | Expected `iss` claim value, or `null` to disable issuer checking | `"https://identity.example.com"` | Expected token issuer (`iss`). |
| `audience` | String / null | **Yes** | Expected `aud` claim value, or `null` to disable audience checking | `"api.velda.io"` | Expected token audience (`aud`). |
| `claims_to_headers` | Map<String, String> | **Yes** | Key-value mapping of JSON claim path to upstream header name (`{}` if none) | `{"sub": "x-user-id", "roles": "x-user-roles"}` | Maps extracted token claims to upstream request headers. |
| `assert_claims` | Map<String, String> | **Yes** | Key-value claims assertions (`{}` if none) | `{}` | Strict claim value assertions (returns `403 Forbidden` on mismatch). |
| `cache_ttl_secs` | Integer (u64) | **Yes** | `1 ..= 86400` seconds | `300` (5 minutes) | Positive authentication cache TTL in seconds. |

### Token Source Format
- **Header**: `{"type": "header", "name": "authorization", "prefix": "Bearer "}`
- **Cookie**: `{"type": "cookie", "name": "access_token"}`
- **Query**: `{"type": "query", "param": "token"}`

---

## 3. Headers Contract

### Request Headers / Cookies Inspected
- Configured header sources (e.g. `Authorization: Bearer <jwt>`).
- Configured cookie sources (e.g. `Cookie: access_token=<jwt>`).
- Configured query parameters (e.g. `?token=<jwt>`).

### Response Generated on Violation (`401 Unauthorized` or `403 Forbidden`)
- Status: `401 Unauthorized` (missing/invalid token) or `403 Forbidden` (assert claims failed).
- Header: `WWW-Authenticate: Bearer realm="velda-edge"`.
- Header: `Content-Type: application/json`.
- Body: `{"error":"Missing or invalid JWT token"}`.

### Request Headers Injected Upstream (on Success)
- All header mappings configured in `claims_to_headers` (e.g. `x-user-id`, `x-user-roles`, `x-tenant-id`).

---

## 4. Configuration Examples

### Asymmetric RS256 Manifest (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "jwt_auth",
      "config": {
        "algorithm": "RS256",
        "secret_key": null,
        "public_key_pem": "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0YpE3u9jN6x7zQjRzW4V\n+Q2g3Z7tK5vL8mNp4rQ5s6t7u8v9w0x1y2z3A4B5C6D7E8F9G0H1I2J3K4L5M6N7\nO8P9Q0R1S2T3U4V5W6X7Y8Z9a0b1c2d3e4f5g6h7i8j9k0l1m2n3o4p5q6r7s8t9\nu0v1w2x3y4z5A6B7C8D9E0F1G2H3I4J5K6L7M8N9O0P1Q2R3S4T5U6V7W8X9Y0Z\n1a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p7q8r9s0t1u2v3w4x5y6z7A8B9C0D1E2F\n3G4H5I6J7K8L9M0N1O2P3Q==\n-----END PUBLIC KEY-----\n",
        "token_sources": [
          {
            "type": "header",
            "name": "authorization",
            "prefix": "Bearer "
          },
          {
            "type": "cookie",
            "name": "access_token"
          }
        ],
        "validate_exp": true,
        "clock_skew_secs": 60,
        "issuer": "https://identity.internal.net",
        "audience": "api.velda.io",
        "claims_to_headers": {
          "sub": "x-user-id",
          "email": "x-user-email"
        },
        "assert_claims": {},
        "cache_ttl_secs": 300
      }
    }
  ]
}
```

### Route Attachment (`routes.json`)

```json
{
  "id": "api-orders-route",
  "match": {
    "path": "/api/v1/orders"
  },
  "upstream_id": "orders-backend",
  "plugins": [
    "jwt_auth"
  ]
}
```
