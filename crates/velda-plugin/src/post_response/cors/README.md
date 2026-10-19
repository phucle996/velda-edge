# CORS Post-Response Plugin (`cors`)

Production-grade Cross-Origin Resource Sharing (CORS) response header injection plugin evaluated during the `post_response` phase. Operates directly on outgoing upstream responses before downstream encoding, ensuring frontend web applications can securely access API resources across distinct origin boundaries.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `cors` |
| **Canonical Phase** | `post_response` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Hot-Path Overhead** | Nanosecond-level; zero allocation when using static headers, zero I/O |

---

## 2. CORS Architecture & Security Invariants

In modern distributed microservices and decoupled single-page applications (SPA), browser-enforced Same-Origin Policy (SOP) blocks frontend web clients from reading responses originating from distinct hostnames, ports, or protocols unless explicit CORS headers are returned.

### The Credentials vs Wildcard Security Invariant
According to the W3C CORS and Fetch specifications:
- When `allow_credentials` is `true` (`Access-Control-Allow-Credentials: true`), **browsers strictly reject responses with `Access-Control-Allow-Origin: *`**.
- The gateway **must** reflect the exact matching client origin (e.g., `Access-Control-Allow-Origin: https://app.example.com`).
- To prevent caching proxies and CDNs from returning a response intended for one origin to another, the gateway **must inject `Vary: Origin`**.

### Pre-Route vs Post-Response Coordination
In the Velda Edge Hourglass Model, CORS handling is cleanly split across two phases:
1. **`pre_route` (CORS Preflight Handshake)**:
   - Intercepts incoming `OPTIONS` preflight requests *before* routing.
   - Validates `Access-Control-Request-Method` and `Access-Control-Request-Headers`.
   - Terminates early with `204 No Content`, bypassing upstream execution entirely.
2. **`post_response` (CORS Response Injection)**:
   - Evaluated on actual requests (`GET`, `POST`, `PUT`, `PATCH`, `DELETE`).
   - Injects `Access-Control-Allow-Origin`, `Access-Control-Allow-Credentials`, `Access-Control-Expose-Headers`, and `Access-Control-Max-Age` into the upstream response before sending it downstream to the client.

---

## 3. Configuration Schema

As with all Velda Edge plugins, **never specify `phase` or `priority`** in user configuration. The execution phase is statically inferred as `post_response`, and execution order follows the route's array declaration order (FIFO).

```json
{
  "id": "cors",
  "config": {
    "allow_origins": [
      "https://app.example.com",
      "https://*.partner.io",
      "http://localhost:3000"
    ],
    "allow_credentials": true,
    "expose_headers": [
      "x-request-id",
      "x-total-count",
      "x-pagination-page"
    ],
    "max_age_secs": 86400
  }
}
```

### Configuration Parameters

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `allow_origins` | string[] | `["*"]` | Allowed origin patterns (exact domain, wildcards `*`, or subdomain wildcards `https://*.example.com`). |
| `allow_origin` | string | `"*"` | Backwards-compatible singular origin fallback if `allow_origins` is omitted. |
| `allow_credentials` | boolean | `false` | Injects `Access-Control-Allow-Credentials: true` and `Vary: Origin`. |
| `expose_headers` | string[] | `[]` | Injects `Access-Control-Expose-Headers` to allow frontend JavaScript to read custom response headers. |
| `max_age_secs` | integer | `null` | Injects `Access-Control-Max-Age` to specify preflight cache duration in seconds. |

---

## 4. Route Attachment Example

```json
{
  "id": "customer-api-route",
  "path": "/api/v1/customers",
  "upstream_id": "customers-service",
  "plugins": [
    { "id": "resource_limit" },
    { "id": "jwt_auth" },
    { "id": "security_headers" },
    {
      "id": "cors",
      "config": {
        "allow_origins": ["https://portal.example.com"],
        "allow_credentials": true,
        "expose_headers": ["x-request-id"]
      }
    },
    { "id": "access_log" }
  ]
}
```
