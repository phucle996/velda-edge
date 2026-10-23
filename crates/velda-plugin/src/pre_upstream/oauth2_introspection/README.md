# OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`)

## Overview
The `oauth2_introspection` plugin executes during the `pre_upstream` execution phase to validate opaque reference access tokens against an upstream OAuth 2.0 Authorization Server (IdP such as Keycloak, Okta, Auth0, or Ory Hydra) according to **RFC 7662**.

For detailed specifications, see:
- [ARCHITECTURE.md](./ARCHITECTURE.md) — Internal engine design, 2-tier caching, and cooperative worker coordination.
- [TEST.md](./TEST.md) — Comprehensive test specification and coverage matrix.
- [BENCHMARK.md](./BENCHMARK.md) — Empirical high-throughput benchmark results on Azure VM.

---

## Architecture & Performance Invariants

1. **Zero-IO Hot Path**: Token verification utilizes a **2-tier caching engine** (Tier-1 Core-Local Thread-Local L1 + Tier-2 Hardware-Sharded L2 with `#[repr(align(64))]` cache-line alignment), delivering sub-2ns L1 lookup latency.
2. **128-bit Avalanche Fingerprint**: Employs `token_fingerprint_128` across 100% of token bytes with SplitMix64 mixing, eliminating prefix/suffix collision blind spots.
3. **Single-Flight In-Flight Deduplication**: Stale tokens (< 15s remaining) dispatch exactly one background revalidation lease across all cores, eliminating thundering herd storms against the IdP.
4. **Cooperative Shadow Revalidation**: Dispatches background revalidations exclusively to dedicated low-priority **Shadow Workers** (`velda-shadow-0`, `SCHED_IDLE`) via non-blocking queues (`try_dispatch_shadow`). If the queue is saturated, tasks drop cleanly without blocking the primary Traffic Worker.
5. **Anti-Header Spoofing**: Unconditionally strips client-supplied request headers matching configured claim targets prior to injecting authenticated claims, preventing claim injection attacks.
6. **Persistent HTTP Keep-Alive Pool**: Network introspection to the IdP reuses persistent TCP connections with kernel `SO_KEEPALIVE`, `TCP_NODELAY`, and transparent single-retry on remote closed connections.
7. **Anti-DoS Negative Caching**: Inactive or revoked tokens (`active: false`) are cached in RAM for `negative_cache_ttl_seconds` to reject subsequent attacks in < 15ns.
8. **Multi-Source Token Extraction**: Matches exact header names, cookies, and query parameters in configured order with zero-copy header borrowing.

---

## 100% Explicit Configuration

```json
{
  "id": "oauth2_introspection",
  "phase": "pre_upstream",
  "config": {
    "introspection_url": "http://127.0.0.1:8080/oauth/introspect",
    "client_id": "velda-edge-gateway",
    "client_secret": "sec_gateway_secret_key_12345",
    "auth_method": "client_secret_basic",
    "token_headers": [
      "x-access-token",
      "authorization"
    ],
    "token_cookies": [
      "access_token"
    ],
    "token_query_params": [
      "token"
    ],
    "token_type_hint": "access_token",
    "cache_ttl_seconds": 300,
    "negative_cache_ttl_seconds": 10,
    "max_cache_entries": 10000,
    "keepalive_timeout_secs": 60,
    "pool_max_idle_connections": 10,
    "request_timeout_ms": 500,
    "required_scopes": [
      "read",
      "orders"
    ],
    "claims_to_headers": [
      { "claim": "sub", "header": "x-user-id" },
      { "claim": "client_id", "header": "x-client-id" },
      { "claim": "scope", "header": "x-authenticated-scope" }
    ],
    "hide_credentials": true,
    "fallback_behavior": "reject"
  }
}
```

---

## Downstream Request Examples

### 1. Via Custom Header
```bash
curl -X GET http://127.0.0.1:8080/v1/orders \
  -H "x-access-token: tok_9a8b7c6d5e4f3a2b1c0d"
```

### 2. Via Standard Authorization Header
```bash
curl -X GET http://127.0.0.1:8080/v1/orders \
  -H "Authorization: Bearer tok_9a8b7c6d5e4f3a2b1c0d"
```

### 3. Via Cookie
```bash
curl -X GET http://127.0.0.1:8080/v1/orders \
  --cookie "access_token=tok_9a8b7c6d5e4f3a2b1c0d"
```

### 4. Via Query Parameter
```bash
curl -X GET "http://127.0.0.1:8080/v1/ws?token=tok_9a8b7c6d5e4f3a2b1c0d&channel=notifications"
```

---

## Known Limitations

### `introspection_url` — IPv4 / Hostname Only (No IPv6 Literal)

The internal HTTP Keep-Alive client ([`client.rs`](./client.rs)) parses `introspection_url` using host/port splitting on `:` without bracket parsing.

```json
// ❌ Not supported:
"introspection_url": "http://[::1]:8080/oauth/introspect"

// ✅ Supported:
"introspection_url": "http://127.0.0.1:8080/oauth/introspect"
"introspection_url": "http://idp.internal:8080/oauth/introspect"
```
In production deployments, Authorization Servers are addressed via IPv4 or DNS hostnames.
