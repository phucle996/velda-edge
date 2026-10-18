# `jwt_auth` — Pre-Upstream JWT Authentication Plugin

The `jwt_auth` plugin provides route-scoped JSON Web Token (RFC 7519) authentication, cryptographic signature verification, nested claim extraction, and dual lock-striped caching for Velda Edge.

---

## 1. Overview & Capabilities

- **Route-Scoped Ingress Isolation**: Attached directly to protected routes in `routes.json`. Public routes (e.g. `/login`, `/healthz`) omit the plugin entirely, eliminating path exclusion boilerplate.
- **Fail-Fast Security Boundary**: Evaluated in the `pre_upstream` phase immediately before upstream connection leasing and request payload streaming. Rejects unauthorized or expired requests with `401 Unauthorized`.
- **Dual Lock-Striped Caching**:
  - **Positive Cache**: Stores verified tokens and pre-built HTTP header values across 64 mutex shards using [`rustc_hash::FxHasher`]. Cache hits execute in ~20ns without repeating cryptographic verification or JSON parsing.
  - **Negative Cache**: Temporarily records invalid and malformed tokens (default 10s TTL) to neutralize Crypto-Exhaustion Denial-of-Service attacks.
- **Flexible Token Sources**: Extracts tokens from HTTP headers (`Authorization: Bearer <token>`, custom headers), HTTP cookies (`Cookie: access_token=<token>`), or URL query parameters (`?token=<token>`).
- **Deep Claim Resolution**: Supports flat claims (`sub`), nested dot notation (`user.id`, `profile.auth.tenant`), and array flattening (`roles: ["admin", "editor"]` $\to$ `admin, editor`).
- **Claim Assertions (RBAC)**: Validates required claims (e.g. `role: "admin"`); returns `403 Forbidden` on mismatch.
- **Algorithm Pinning**: Strict checking of JOSE header `alg` against the configured algorithm to defeat algorithm confusion attacks (`alg: "none"` or HMAC/RSA substitution).

---

## 2. Configuration Schema & Types

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `algorithm` | `string` | `"HS256"` | Cryptographic algorithm: `HS256`, `HS384`, `HS512`, `RS256`, `RS384`, `RS512`, `ES256`, `ES384`. |
| `secret_key` | `string` | `null` | Shared secret key for symmetric HMAC algorithms (`HS256`, `HS384`, `HS512`). |
| `public_key_pem` | `string` | `null` | PEM-encoded public key for asymmetric algorithms (`RS*`, `ES*`). |
| `token_sources` | `array` | `[{"type":"header","name":"authorization","prefix":"Bearer "}]` | List of token extraction sources checked in priority order. |
| `validate_exp` | `bool` | `true` | Validates standard `exp` claim against current epoch seconds. |
| `clock_skew_secs` | `integer` | `60` | Clock drift tolerance window in seconds. |
| `issuer` | `string` | `"none"` | Expected `iss` claim. Set to `"none"` or omit to disable issuer check. |
| `audience` | `string` | `"none"` | Expected `aud` claim (supports string or array in token payload). |
| `claims_to_headers`| `object` | `{}` | Mapping of JSON claim paths to downstream HTTP request headers. |
| `assert_claims` | `object` | `{}` | Key-value assertions that must match payload claims (returns `403` on mismatch). |
| `cache_ttl_secs` | `integer` | `300` | Maximum positive cache TTL in seconds. |
| `negative_cache_ttl_secs`| `integer`| `10` | Negative cache duration in seconds for invalid tokens. |
| `cache_shards` | `integer` | `64` | Number of lock-striped shards (rounded to power of 2). |
| `cache_shard_capacity` | `integer` | `2048` | Maximum cache entries per shard before LRU eviction. |

### Token Source Object Format

- **Header**: `{"type": "header", "name": "authorization", "prefix": "Bearer "}`
- **Cookie**: `{"type": "cookie", "name": "access_token"}`
- **Query**: `{"type": "query", "param": "token"}`

---

## 3. Execution Pipeline & Architecture

```
Downstream Request
       │
       ▼
[ Pre-Route Phase: ip_filter, waf ]
       │
       ▼
[ Route Match & Upstream Selection ]
       │
       ▼
[ Pre-Upstream Phase ]
       │
       ├─► 1. Extract Token (Header / Cookie / Query)
       │      └─► None: 401 Unauthorized (WWW-Authenticate: Bearer realm="velda-edge")
       │
       ├─► 2. Check Negative Cache (Hash Lookup)
       │      └─► Hit: 401 Unauthorized (Skip Crypto)
       │
       ├─► 3. Check Positive Cache (Hash Lookup)
       │      └─► Hit: Inject Cached Headers (~20ns) ──► Action::Continue
       │
       └─► 4. Cache Miss:
              ├─► Verify JOSE Header alg Pinning
              ├─► Verify Cryptographic Signature (ring constant-time)
              ├─► Parse JSON Payload & Validate exp, nbf, iss, aud
              ├─► Verify assert_claims (Fail -> 403 Forbidden)
              ├─► Extract claims_to_headers
              ├─► Insert Positive Cache
              └─► Inject Headers ──► Action::Continue
```

---

## 4. Configuration Examples

### Symmetric HMAC Authentication (HS256)

```json
{
  "id": "jwt_auth",
  "phase": "pre_upstream",
  "priority": 200,
  "config": {
    "algorithm": "HS256",
    "secret_key": "your-256-bit-secret-key-here",
    "validate_exp": true,
    "clock_skew_secs": 60,
    "issuer": "https://identity.example.com",
    "audience": "velda-api",
    "claims_to_headers": {
      "sub": "x-user-id",
      "roles": "x-user-roles",
      "profile.tenant_id": "x-tenant-id"
    },
    "cache_ttl_secs": 300,
    "negative_cache_ttl_secs": 15
  }
}
```

### Asymmetric RSA Authentication (RS256) with RBAC Assertion

```json
{
  "id": "jwt_auth",
  "phase": "pre_upstream",
  "priority": 200,
  "config": {
    "algorithm": "RS256",
    "public_key_pem": "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----",
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " },
      { "type": "cookie", "name": "session_token" }
    ],
    "validate_exp": true,
    "assert_claims": {
      "role": "admin"
    },
    "claims_to_headers": {
      "sub": "x-authenticated-user",
      "email": "x-user-email"
    }
  }
}
```

---

## 5. Performance & Operational Considerations

- **Zero-Allocation Hot Path**: Pre-computes `HeaderName` and `HeaderValue` during verification and caches them. Subsequent hits clone pre-constructed values directly into `req.headers` without string parsing or allocation.
- **Lock Contention**: 64 independent mutex shards prevent cache write locks from bottlenecking concurrent worker threads.
- **DoS Immunity**: Negative caching ensures that invalid tokens repeated in quick succession bypass expensive asymmetric signature algorithms.
- **Zero Disk / RPC Dependency**: Verification uses local in-memory keys without blocking external RPC calls in the request path.
