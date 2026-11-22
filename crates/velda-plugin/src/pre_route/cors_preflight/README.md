# CORS Preflight Interceptor Plugin (`cors_preflight`)

```text
+-----------------------------------------------------------------------------------+
|                     CORS Preflight Pre-Route Architecture                         |
|                                                                                   |
|  [Inbound L7 Request] (HTTP/1.1, HTTP/2, HTTP/3)                                  |
|         │                                                                         |
|         ▼                                                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Frontline Traffic Worker (velda-worker-{i}) — Pre-Route Phase               │  |
|  │  - Method != OPTIONS or Origin missing? ──► Action::Continue (Pass-through) │  |
|  │  - Match Origin against pre-compiled OriginPatterns (Exact / Wildcard)      │  |
|  │  - Validate Access-Control-Request-Method & Access-Control-Request-Headers  │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │                                         |
|                            [Origin & Request Valid?]                              |
|                             /                     \                               |
|                           YES                      NO                             |
|                           /                          \                            |
|      Action::Respond(204 No Content)         Action::Reject(Forbidden)            |
|      (Pre-baked Access-Control-* headers)    (403 Forbidden, Upstream Untouched)  |
+-----------------------------------------------------------------------------------+
```

The **`cors_preflight`** plugin intercepts browser CORS preflight `OPTIONS` requests at the **Velda Edge** ingress perimeter (`phase: "pre_route"`), terminating valid preflights with `204 No Content` before route lookup or upstream connection leasing.

---

## 1. What This Plugin Does

1. **Zero-Upstream Edge Termination**:
   - Replies directly to valid `OPTIONS` preflight requests with `204 No Content` and pre-computed `Access-Control-Allow-*` headers, offloading 100% of preflight traffic from backend microservices.
2. **RFC 6454 Security Enforcement**:
   - Strictly forbids `"*"` or `"null"` origins when `allow_credentials: true`, preventing credential leakage to arbitrary or sandboxed origins.
3. **Pass-Through for Standard Traffic**:
   - Non-`OPTIONS` requests and requests without an `Origin` header bypass evaluation immediately (`Action::Continue`).

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/pre_route/cors_preflight/`
- **Execution Phase**: `phase: "pre_route"`
- **Runtime Placement**: Executed inline on Frontline Traffic Workers (`velda-worker-{i}`).

---

## 3. Requirements & Prerequisites

1. **Explicit Configuration Contract**: `allowed_origins`, `allowed_methods`, `allowed_headers`, `allow_credentials`, and `max_age_secs` (`<= 86400`) must all be explicitly declared.
2. **Response CORS Headers**: Pair with the `post_response` `cors` plugin if actual (`GET`/`POST`) responses also require `Access-Control-Allow-Origin` headers.

---

## 4. Quick Start Configuration

```json
{
  "id": "cors_preflight",
  "phase": "pre_route",
  "config": {
    "allowed_origins": ["https://app.example.com", "https://*.corp.internal"],
    "allowed_methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    "allowed_headers": ["Content-Type", "Authorization"],
    "allow_credentials": true,
    "max_age_secs": 3600
  }
}
```

---

## 5. Documentation Directory

- **[RUNBOOK.md](./RUNBOOK.md)**: Parameter specifications, SPA/public API presets, and troubleshooting playbooks.
- **[ARCHITECTURE.md](./ARCHITECTURE.md)**: Deep internals (Origin matching, RFC 6454 invariants, zero-allocation header emission).
- **[BENCHMARK.md](./BENCHMARK.md)**: Standardized 5-tier diagnostic benchmark matrix and verified `x86-64-v3` release assembly audit.
- **[TEST.md](./TEST.md)**: Unit and RFC 6454 compliance test matrix.
