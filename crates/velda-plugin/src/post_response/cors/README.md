# CORS Post-Response Plugin (`cors`)

Production-grade Cross-Origin Resource Sharing (CORS) response header injection plugin evaluated during the `post_response` phase. Operates directly on outgoing upstream responses before downstream encoding, ensuring frontend web applications can securely access API resources across distinct origin boundaries.

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: Every configuration parameter must be declared explicitly in `plugins.json`. No silent fallbacks, implicit defaults, or hidden behaviors are permitted.
>
> **RFC 6454 Security Invariant**: When `allow_credentials` is `true`, `allow_origins` must NOT contain wildcard `*`.
>
> **Two-Phase CORS Separation**: OPTIONS preflights are handled in `pre_route` by `cors_preflight`. Actual response header injection is handled in `post_response` by `cors`.

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `cors` |
| **Lifecycle Phase** | `post_response` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Hot Path Latency** | ~5ns to 15ns (Pre-computed HeaderValues, zero I/O) |

---

## 2. Configuration Parameters

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `allow_origins` | `array[string]` | **Yes** | Non-empty array of exact origins, subdomains (`https://*.domain.com`), or `["*"]` | `["https://app.example.com"]` | Allowed origins permitted to read the response. |
| `allow_credentials` | `boolean` | **Yes** | `true` or `false` (cannot be true if `allow_origins` has `*`) | `true` | Indicates whether client requests can include credentials (cookies, auth headers). |
| `expose_headers` | `array[string]` | **Yes** | Array of valid RFC 9110 header tokens, or `[]` | `["x-request-id"]` | Headers exposed to browser JavaScript via `Access-Control-Expose-Headers`. |
| `max_age_secs` | `integer \| null` | **Yes** | `1..=86400` seconds (24h) or `null` | `86400` | Injects `Access-Control-Max-Age` caching directive for preflights. |

---

## 3. Configuration Manifest (`plugins.json`)

```json
{
  "plugins": [
    {
      "id": "app_cors_response",
      "plugin": "cors",
      "config": {
        "allow_origins": [
          "https://app.example.com",
          "https://admin.example.com"
        ],
        "allow_credentials": true,
        "expose_headers": [
          "x-request-id",
          "x-total-count"
        ],
        "max_age_secs": 86400
      }
    }
  ]
}
```

---

## 4. Route Attachment (`routes.json`)

Attach the plugin to an HTTP route via its unique identifier in the `plugins` array:

```json
{
  "routes": [
    {
      "id": "route_api",
      "match": {
        "path": {
          "prefix": "/api/"
        }
      },
      "plugins": [
        "app_cors_response"
      ],
      "upstream": "cluster_main"
    }
  ]
}
```

---

## 5. Security & Invariant Rules

1. **Credentials vs Wildcard Constraint (RFC 6454)**:
   - When `allow_credentials: true`, user-agents strictly reject responses with `Access-Control-Allow-Origin: *`.
   - The plugin reflects the matching client origin (e.g. `Access-Control-Allow-Origin: https://app.example.com`).
   - Automatically injects `Vary: Origin` to ensure intermediate CDNs and reverse proxies never cache cross-tenant responses.
2. **Expose Headers**:
   - Browsers only expose basic CORS-safelisted response headers by default (`Cache-Control`, `Content-Language`, `Content-Type`, `Expires`, `Last-Modified`, `Pragma`). Custom headers like `x-request-id` require explicit declaration in `expose_headers`.
