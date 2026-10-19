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
- **Origin Pattern Matching**: Supports both exact origins (`https://app.velda.io`) and subdomain wildcards (`https://*.velda.io`).

### Pairing with `cors` (`post_response`)
- **`cors_preflight` (`pre_route`)**: Handles incoming browser preflight checks (`OPTIONS`).
- **`cors` (`post_response`)**: Injects standard CORS response headers on actual incoming requests (`GET`, `POST`, `PUT`, `DELETE`).

---

## 2. Configuration Field Specification

| Field | Type | Required / Default | Constraints / Allowed Values | Description |
| :--- | :--- | :--- | :--- | :--- |
| `allowed_origins` | array[string] | Optional (`["*"]`) | Array of origins or `["*"]` | Allowed CORS origins. Supports exact matches and wildcards (`https://*.domain.com`). |
| `allowed_methods` | array[string] | Optional (Standard 7 methods) | `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS` | Allowed HTTP methods in preflight response. |
| `allowed_headers` | array[string] | Optional (`["*"]`) | Array of header names or `["*"]` | Allowed request headers. `"*"` reflects requested headers. |
| `allow_credentials` | boolean | Optional (`false`) | `true`, `false` | Enables `Access-Control-Allow-Credentials: true`. |
| `max_age_secs` | integer | Optional (`86400`) | $\ge 0$ | Cache lifetime in seconds for the browser preflight result (`Access-Control-Max-Age`). |

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

### Production SaaS SPA Setup (Subdomain Wildcard + Credentials)
```json
{
  "id": "cors_preflight",
  "config": {
    "allowed_origins": [
      "https://*.velda.io",
      "https://velda.io",
      "https://localhost:3000"
    ],
    "allowed_methods": [
      "GET",
      "POST",
      "PUT",
      "DELETE",
      "PATCH",
      "OPTIONS"
    ],
    "allowed_headers": [
      "Authorization",
      "Content-Type",
      "X-Requested-With",
      "X-API-Key"
    ],
    "allow_credentials": true,
    "max_age_secs": 86400
  }
}
```

### Public API Setup (Permissive Wildcard)
```json
{
  "id": "cors_preflight",
  "config": {
    "allowed_origins": ["*"],
    "allowed_methods": ["GET", "POST", "OPTIONS"],
    "allowed_headers": ["*"],
    "allow_credentials": false,
    "max_age_secs": 86400
  }
}
```

---

## 5. Architectural Invariants

1. **Short-Circuit Authority**: Returns [`Action::Respond`] with `204 No Content` immediately. Does not allocate router trie nodes, execute route matching, spawn body readers, or lease upstream connections.
2. **Pre-Computed Header Values**: All method strings, max-age integers, and static header definitions are compiled into static [`HeaderValue`] handles during `from_config` initialization.
3. **Zero Heap Allocation on Hot Path**: Inspecting and responding to preflight requests does not perform dynamic allocations or string formatting.
