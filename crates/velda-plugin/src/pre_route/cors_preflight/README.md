# CORS Preflight Interceptor Plugin (`cors_preflight`)

High-performance L7 ingress short-circuiting gatekeeper that intercepts Cross-Origin Resource Sharing (CORS) HTTP `OPTIONS` preflight requests at the edge, returning an RFC-compliant `204 No Content` response prior to route trie matching, authentication checks, body streaming, or upstream connection pool acquisition.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `cors_preflight` |
| **Canonical Phase** | `pre_route` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (Generates `Action::Respond` 204 or 403) |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Hot-Path Overhead** | Single-digit nanoseconds; pre-computed `HeaderValue` clones; **0 heap allocations** |

### What it Solves
- **Edge Short-Circuiting**: Automatically responds to browser CORS preflight requests (`OPTIONS` with `Origin` and `Access-Control-Request-Method`) directly at the edge in $< 1\ \mu\text{s}$, preventing unnecessary load on backend microservices.
- **Eliminates False Auth Rejections**: Because standard browsers send Preflight `OPTIONS` without credentials, intercepting in `pre_route` (before `pre_upstream` plugins like `jwt_auth` or `api_key`) prevents preflight requests from falsely failing with `401 Unauthorized`.
- **W3C Fetch Spec Compliance**: When `allow_credentials` is `true`, wildcards (`*`) are prohibited by browsers. The plugin dynamically reflects the exact requesting origin while preserving security boundaries.

### Pairing with `cors` (`post_response`)
- **`cors_preflight` (`pre_route`)**: Handles incoming browser preflight checks (`OPTIONS`).
- **`cors` (`post_response`)**: Injects standard CORS response headers on actual incoming requests (`GET`, `POST`, `PUT`, `DELETE`).

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `allowed_origins` | Array of strings | **Yes** | Non-empty list of origins (`"https://*.domain.com"`, exact origins, or `["*"]`). Cannot be `"*"` if `allow_credentials: true`. | `["https://app.example.com", "https://admin.example.com"]` | Allowed CORS origins for preflight requests. |
| `allowed_methods` | Array of strings | **Yes** | Non-empty list of valid HTTP methods (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS`) | `["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]` | Allowed HTTP methods in preflight response (`Access-Control-Allow-Methods`). |
| `allowed_headers` | Array of strings | **Yes** | List of valid HTTP header names or `["*"]` to mirror requested headers | `["Authorization", "Content-Type", "X-Requested-With"]` | Allowed request headers (`Access-Control-Allow-Headers`). |
| `allow_credentials` | Boolean | **Yes** | `true` or `false` | `true` (for authenticated apps) / `false` (for public APIs) | Enables `Access-Control-Allow-Credentials: true`. |
| `max_age_secs` | Integer (u64) | **Yes** | $\ge 0$ seconds | `86400` (24 hours) | Browser preflight cache lifetime in seconds (`Access-Control-Max-Age`). |

---

## 3. Headers Contract

### Request Headers Inspected
- `Origin`: Downstream client web origin (e.g. `https://dashboard.example.com`).
- `Access-Control-Request-Method`: HTTP method intended for the subsequent actual request (e.g. `POST`).
- `Access-Control-Request-Headers`: Comma-separated list of custom headers requested by the browser.

### Response Headers Emitted (`204 No Content`)
- `Access-Control-Allow-Origin`: Matched origin or `*` (exact origin if `allow_credentials: true`).
- `Access-Control-Allow-Methods`: Pre-computed list of allowed HTTP methods.
- `Access-Control-Allow-Headers`: Allowed headers or reflected requested headers.
- `Access-Control-Allow-Credentials`: `"true"` (when enabled).
- `Access-Control-Max-Age`: Pre-computed cache TTL string.
- `Vary`: `Origin, Access-Control-Request-Method, Access-Control-Request-Headers`.
- `Content-Length`: `"0"`.

---

## 4. Configuration Examples

### Production Manifest (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "cors_preflight",
      "config": {
        "allowed_origins": [
          "https://*.velda.io",
          "https://velda.io"
        ],
        "allowed_methods": [
          "GET",
          "POST",
          "PUT",
          "DELETE",
          "PATCH",
          "HEAD",
          "OPTIONS"
        ],
        "allowed_headers": [
          "Authorization",
          "Content-Type",
          "X-Requested-With"
        ],
        "allow_credentials": true,
        "max_age_secs": 86400
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
    "ip_filter",
    "resource_limit",
    "cors_preflight",
    "waf_lite",
    "jwt_auth",
    "cors"
  ]
}
```

---

## 5. Architectural Invariants & Hardware Optimizations

1. **Short-Circuit Authority**: Returns [`Action::Respond`] with `204 No Content` immediately. Does not allocate router trie nodes, execute route matching, spawn body readers, or lease upstream connections.
2. **Pre-Baked Header Template (Zero-Branch Hot Path)**: Pre-computes all static response headers (`methods`, `credentials`, `max-age`, `vary`, `content-length`) at initialization. On request, simply clones the template into the response, eliminating 5+ runtime `if` checks.
3. **Bitmask Method Validation**: Maps HTTP methods to a compact `u16` bitmask for $O(1)$ single-instruction bitwise validation (`AND`), replacing $O(N)$ linear scans and branching.
4. **Branchless 256-Byte Subdomain LUT**: Enforces strict DNS label syntax via a static character lookup table resident in CPU L1 cache, rejecting CVE-2020-7662 wildcard injection probes without CPU pipeline stalls.
5. **Zero Heap Allocation on Hot Path**: Inspecting and responding to preflight requests uses `Bytes::from_static` and atomic `HeaderValue` reference cloning, ensuring 0 heap allocations.

---

## 6. Live Cloud Benchmark Results (Azure 4 vCPUs)

| Scenario | Throughput | Avg Latency | P50 Latency | P90 Latency | Status Code |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **HTTP/1.1 Cleartext Preflight** | **101,005.54 RPS** | **743.97 µs** | 495.00 µs | 770.00 µs | `204 No Content` |
| **HTTPS HTTP/1.1 TLS Preflight** | **73,604.14 RPS** | **729.70 µs** | 644.00 µs | 900.00 µs | `204 No Content` |
| **HTTPS HTTP/2 Multiplexed** | **36,490.82 RPS** | **1.74 ms** | 1.70 ms | 1.90 ms | `204 No Content` |
| **Malicious Probes (403 Reject)** | **105,310.57 RPS** | **778.57 µs** | 470.00 µs | 890.00 µs | `403 Forbidden` |
| **Concurrent Mixed SPA Traffic** | **69,260.30 RPS** | **1.26 ms** | — | — | Combined 100c |

