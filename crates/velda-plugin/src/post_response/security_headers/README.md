# Security Response Headers Plugin (`security_headers`)

Client-side browser hardening and OWASP L7 hygiene plugin for outgoing HTTP responses. Automatically injects pre-compiled security headers instructing user-agents to activate internal sandbox defenses against MITM, Clickjacking, MIME-sniffing, XSS, and Referrer leakage.

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: Every configuration parameter must be declared explicitly in `plugins.json`. No silent fallbacks, implicit defaults, or hidden behaviors are permitted.
>
> **Explicit 'none' / null Sentinel**: When a security header is intentionally omitted or disabled, declare `"none"` or `null` explicitly in the configuration.
>
> **Zero-Allocation Hot Path**: All `HeaderName` and `HeaderValue` pairs are pre-computed and validated at boot time (`from_config`). Serving a response executes a contiguous iteration with zero heap allocations.

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `security_headers` |
| **Lifecycle Phase** | `post_response` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Hot Path Latency** | ~10ns (Pre-computed HeaderValues, atomic ref-count clone) |

---

## 2. Configuration Parameters

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `hsts` | `object` | **Yes** | Object containing `enable`, `max_age_secs`, `include_subdomains`, `preload` | See subfields below | HTTP Strict Transport Security configuration. |
| `hsts.enable` | `boolean` | **Yes** | `true` or `false` | `true` | Enables or disables the `Strict-Transport-Security` header. |
| `hsts.max_age_secs` | `integer` | **Yes** | `1..=63072000` (must be > 0 if enabled) | `31536000` (1 year) | How long (seconds) browser enforces HTTPS. |
| `hsts.include_subdomains` | `boolean` | **Yes** | `true` or `false` | `true` | Extends HSTS policy to all subdomains. |
| `hsts.preload` | `boolean` | **Yes** | `true` or `false` | `false` | Requests inclusion in browser HSTS preload list. |
| `frame_options` | `string` | **Yes** | `"DENY"`, `"SAMEORIGIN"`, `"ALLOW-FROM"`, or `"none"` | `"DENY"` | X-Frame-Options clickjacking defense. |
| `content_type_nosniff` | `boolean` | **Yes** | `true` or `false` | `true` | When true, injects `X-Content-Type-Options: nosniff`. |
| `referrer_policy` | `string` | **Yes** | Valid W3C policy or `"none"` | `"strict-origin-when-cross-origin"` | Referrer-Policy leakage control directive. |
| `content_security_policy` | `string \| null` | **Yes** | Valid CSP string, `"none"`, or `null` | `"none"` | Content-Security-Policy header directive. |
| `permissions_policy` | `string \| null` | **Yes** | Valid permissions policy string, `"none"`, or `null` | `"none"` | Browser hardware/feature restrictions. |

---

## 3. Configuration Manifest (`plugins.json`)

```json
{
  "plugins": [
    {
      "id": "edge_security_headers",
      "plugin": "security_headers",
      "config": {
        "hsts": {
          "enable": true,
          "max_age_secs": 31536000,
          "include_subdomains": true,
          "preload": false
        },
        "frame_options": "DENY",
        "content_type_nosniff": true,
        "referrer_policy": "strict-origin-when-cross-origin",
        "content_security_policy": "none",
        "permissions_policy": "none"
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
      "id": "route_web",
      "match": {
        "path": {
          "prefix": "/"
        }
      },
      "plugins": [
        "edge_security_headers"
      ],
      "upstream": "cluster_frontend"
    }
  ]
}
```

---

## 5. Defense Invariants

1. **HSTS (RFC 6797)**: Prevents SSL-stripping downgrade attacks by enforcing HTTPS for a declared duration.
2. **X-Content-Type-Options (RFC 7231)**: Sets `nosniff` to disable browser MIME sniffing, preventing malicious scripts disguised as images from executing.
3. **X-Frame-Options (RFC 7034)**: Disables or restricts `<iframe>` embedding (`DENY` or `SAMEORIGIN`), neutralizing UI Redressing and Clickjacking exploits.
4. **Referrer-Policy (W3C)**: Restricts referrer leakage to external origins, preventing tokens and query parameters from exposing in logs.
5. **Content-Security-Policy (CSP W3C Level 3)**: Restricts authorized domains for scripts, styles, images, and child frames.
6. **Permissions-Policy (W3C)**: Disables unused browser hardware capabilities (camera, microphone, geolocation).
