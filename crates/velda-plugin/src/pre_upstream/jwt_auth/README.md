# JWT Authentication Plugin (`jwt_auth`)

```text
+-----------------------------------------------------------------------------------+
|                        JWT Authentication — High-Level Workflow                   |
|                                                                                   |
|  [Downstream Request Ingress]                                                     |
|         │                                                                         |
|         ▼                                                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Token Extraction                                                            │  |
|  │  - Extracts from Header ("Authorization: Bearer <tok>"), Cookie, or Query   │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │ Raw Token Slices (&str)                 |
|                                         ▼                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Dual Cache Check                                                            │  |
|  │  - Negative Cache: Fast-reject known malformed / invalid tokens             │  |
|  │  - Positive Cache: Bypass crypto math on repeated valid tokens              │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │ Cache Miss                              |
|                                         ▼                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Signature Verification & Key Lookup                                         │  |
|  │  - Match key in JwksKeyTable (static key or dynamic via jwks_rotator)       │  |
|  │  - Constant-time verification (HMAC / RSA / ECDSA) purely on stack          │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │ Verified Token Claims                   |
|                                         ▼                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Claims Validation & Header Injection                                        │  |
|  │  - Enforce exp, nbf, iss, aud, and custom claim assertions                  │  |
|  │  - Inject user claims into upstream headers (e.g. x-user-id, x-tenant-id)   │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │ Action::Continue                        |
|                                         ▼                                         |
|  [Upstream Service Request Forwarding]                                            |
+-----------------------------------------------------------------------------------+
```

The **`jwt_auth`** plugin is Velda Edge's cryptographic authentication engine at L7. It enforces identity verification by validating RFC 7519 JSON Web Tokens, evaluating expiration and custom claim policies, and projecting authenticated identity context directly into upstream request headers.

---

## 1. What This Plugin Does

1. **Cryptographic Token Verification**:
   - Validates signatures signed by HMAC (HS256/384/512), RSA (RS256/384/512), or ECDSA (ES256/384).
   - Operates with static keys or seamlessly integrates with `jwks_rotator` for live zero-downtime key rotation.
2. **Zero-Allocation Request Hot Path**:
   - Executes token slicing, JOSE header parsing, and standard payload extraction purely in stack-allocated memory, resulting in zero dynamic heap allocations per request.
3. **Lock-Striped Dual Caching**:
   - **Positive Cache**: Stores pre-parsed claims for high-frequency tokens to bypass redundant cryptographic math.
   - **Negative Cache**: Rejects repeated invalid or malicious tokens instantly, defeating cryptographic exhaustion Denial-of-Service attacks.
4. **Declarative Claims & Identity Projection**:
   - Extracts nested or top-level claims (e.g., `user.id`, `roles`, `tenant`) and injects them as standard HTTP headers into the upstream request before forwarding.
   - Enforces strict assertions (`assert_claims`), rejecting unauthorized tokens with `403 Forbidden`.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/pre_upstream/jwt_auth/`
- **Execution Stage**: `PreUpstreamPlugin` (compile-time fixed)
- **Pipeline Placement**: Executes immediately after route matching and before forwarding requests to the upstream connection pool.
- **Companion Relationship**: Can be paired with `jwks_rotator`. When paired, `jwt_auth` dynamically verifies tokens against the in-memory key table published by `jwks_rotator`.

---

## 3. Requirements & Prerequisites

1. **Token Ingress Source**: Incoming requests must carry a JWT token in one of the configured extraction locations (`header`, `cookie`, or `query`).
2. **Key Configuration**:
   - **Symmetric**: Requires `secret_key` when using HMAC (`HS256/384/512`).
   - **Asymmetric**: Requires `public_key_pem` or a paired `jwks_rotator` when using RSA or ECDSA.
3. **Algorithm Pinning**: The configured `algorithm` must strictly match the token's JOSE header `alg` parameter to prevent algorithm substitution vulnerabilities.

---

## 4. HTTP Headers Contract

### Inspected from Downstream Request
- Header sources: e.g., `Authorization: Bearer <jwt_token>`
- Cookie sources: e.g., `Cookie: access_token=<jwt_token>`
- Query parameter sources: e.g., `?token=<jwt_token>`

### Generated on Authentication Failure
- **`401 Unauthorized`**: Generated when a token is missing, expired, has an invalid signature, or is malformed.
  - Injects header: `WWW-Authenticate: Bearer realm="velda-edge"`
- **`403 Forbidden`**: Generated when the token is cryptographically valid, but fails an `assert_claims`, `issuer`, or `audience` policy check.

### Injected into Upstream Request (on Success)
- Injects all mappings defined in `claims_to_headers` (e.g., `x-user-id: 12345`, `x-user-roles: admin,editor`).

---

## 5. Quick Start Configuration

Add the plugin to your `plugins.json` (and reference its ID in route `plugins`):

```json
{
  "id": "api_jwt_auth",
  "config": {
    "algorithm": "RS256",
    "secret_key": null,
    "public_key_pem": "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0...",
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

---

## 6. Documentation Directory

For deeper technical and operational information, refer to the accompanying specifications:

- **[RUNBOOK.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/pre_upstream/jwt_auth/RUNBOOK.md)**: Field-by-field parameter explanations, tuned setup recipes for production scenarios, incident triage playbooks, and diagnostic CLI tools.
- **[ARCHITECTURE.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/pre_upstream/jwt_auth/ARCHITECTURE.md)**: Deep micro-architectural specification (cacheline alignment, MESI coherence, shard disjointness proofs, physical hardware caveats, and scalar vs AVX branch prediction trade-offs).
- **[BENCHMARK.md](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/setup_kvm_collectors_isolation/crates/velda-plugin/src/pre_upstream/jwt_auth/BENCHMARK.md)**: Benchmark specifications evaluating token parsing latency, cache hit ratios, and throughput under heavy cryptographic loads.
