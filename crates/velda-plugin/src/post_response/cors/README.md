# CORS Post-Response Plugin (`cors`)

```text
  Upstream L7Response
        │
        ▼
  [post_response: cors] (Frontline Worker, pbww = 0)
        ├─ Read-only: match `req_headers["origin"]` against `CorsOriginPattern[]`
        ├─ If `needs_vary_origin`: ensure `Vary: Origin` (RFC 9110 §12.5.5 cache safety)
        └─ Origin missing / disallowed ─► return, no `Access-Control-*` header written
           Origin allowed              ─► one `.insert()` per enabled header (never write-then-remove)
```

---

## 1. What This Plugin Does

`cors` enforces Cross-Origin Resource Sharing (`RFC 6454`, `WHATWG Fetch`, `RFC 9110 §12.5.5`) on outgoing HTTP responses with a validate-then-write flow:
- Validates the request `Origin` first, without touching the response.
- For an allowed `Origin`, inserts `Access-Control-Allow-Origin` (`*` or reflected origin), plus `Access-Control-Allow-Credentials` and `Access-Control-Expose-Headers` only when enabled in config.
- For a missing or disallowed `Origin`, writes no `Access-Control-*` header. It does not read or remove CORS headers already present on the upstream response.
- Appends `Vary: Origin` across all outcomes (allowed, disallowed, and missing `Origin`) whenever the CORS policy varies by `Origin` (`!allow_all_origins || allow_credentials`).

## 2. Where It Sits in Velda Edge

- **Phase**: `post_response` (inline on Frontline Traffic Workers `velda-worker-{i}`).
- **Background Weight (`pbww`)**: `0` (hot-path only, zero background tasks).
- **Capability**: `PluginCapability::header_only(AccessLevel::Write)` — compatible with both buffered and streaming HTTP pipes (`HTTP/1.1`, `HTTP/2`, `HTTP/3`).
- **Two-Phase Separation**: Preflight `OPTIONS` requests are handled independently in `pre_route` by `cors_preflight`; `post_response/cors` is completely self-contained with zero cross-phase coupling.

## 3. Requirements & Prerequisites

- Attach to HTTP listeners/routes (`SupportedProtocols::ALL_HTTP`; non-HTTP protocols fail at compile time).
- All 3 configuration fields (`allow_origins`, `allow_credentials`, `expose_headers`) must be declared explicitly (zero silent fallbacks).

## 4. Quick Start Configuration

```json
{
  "id": "cors",
  "config": {
    "allow_origins": ["https://app.example.com", "https://*.partner.io"],
    "allow_credentials": true,
    "expose_headers": ["x-request-id", "x-total-count"]
  }
}
```

## 5. Documentation Directory

- [RUNBOOK.md](RUNBOOK.md) — Parameter reference, configuration recipes, and incident triage.
- [ARCHITECTURE.md](ARCHITECTURE.md) — Origin byte-matching engine, 64-bit SWAR `Vary` scanner, and security invariants.
- [BENCHMARK.md](BENCHMARK.md) — 5-tier performance specification and verified x86-64-v3 release ASM audit.
- [TEST.md](TEST.md) — Unit and integration test verification report.
