# Security Response Headers Plugin (`security_headers`)

Client-side browser hardening and OWASP L7 hygiene plugin for outgoing HTTP responses. Automatically injects pre-compiled security headers instructing user-agents to activate internal sandbox defenses against MITM, Clickjacking, MIME-sniffing, XSS, and Referrer leakage.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `security_headers` |
| **Canonical Phase** | `post_response` |
| **Execution Order** | Stably sorted descending by route `priority` (Evaluated immediately before downstream frame encoding) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Hot-Path Overhead** | O(1) per header; pre-computed `HeaderValue` atomic ref-count clone; **0 heap allocations** |

### What it Protects
- **HSTS (RFC 6797)**: Prevents SSL-stripping downgrade attacks by enforcing HTTPS for a declared duration.
- **X-Content-Type-Options (RFC 7231)**: Sets `nosniff` to disable browser MIME sniffing, preventing malicious scripts disguised as images from executing.
- **X-Frame-Options (RFC 7034)**: Disables or restricts `<iframe>` embedding (`DENY` or `SAMEORIGIN`), neutralizing UI Redressing and Clickjacking exploits.
- **Referrer-Policy (W3C)**: Restricts referrer leakage to external origins, preventing tokens and query parameters from exposing in logs.
- **Content-Security-Policy (CSP W3C Level 3)**: Restricts authorized domains for scripts, styles, images, and child frames.
- **Permissions-Policy (W3C)**: Disables unused browser hardware capabilities (camera, microphone, geolocation).

### Out of Scope
- **Backend Protection**: Does not protect servers from SQLi, RCE, or volumetric floods.
- **CORS Handling**: Cross-Origin Resource Sharing and preflight `OPTIONS` are owned by the `cors` plugin.
- **Arbitrary Header Transformations**: Dynamic variable substitutions are owned by `header_mutation`.

---

## 2. Configuration Field Specification

Any unused security header must be explicitly set to `"none"` to omit it from the response.

| Field | Type | Required / Default | Constraints / Allowed Values | Description |
| :--- | :--- | :--- | :--- | :--- |
| `hsts` | object \| string | Optional | Object or `"none"` | HTTP Strict Transport Security configuration |
| `hsts.enable` | boolean | Optional (`true`) | `true`, `false` | Enables or disables the HSTS header |
| `hsts.max_age_secs` | integer | Optional (`31536000`) | `>= 0` (seconds, default 1 year) | How long the browser must remember to use HTTPS |
| `hsts.include_subdomains`| boolean | Optional (`true`) | `true`, `false` | Extends HSTS policy to all subdomains |
| `hsts.preload` | boolean | Optional (`false`) | `true`, `false` | Requests inclusion in browser HSTS preload list |
| `frame_options` | string | Optional (`"DENY"`) | `"DENY"`, `"SAMEORIGIN"`, or `"none"` | X-Frame-Options clickjacking defense |
| `content_type_nosniff` | boolean | Optional (`true`) | `true`, `false` | When true, injects `x-content-type-options: nosniff` |
| `referrer_policy` | string | Optional (`"strict-origin-when-cross-origin"`) | Valid W3C policy or `"none"` | Referrer leakage control policy |
| `csp` | string | Optional (`"none"`) | CSP directive string or `"none"` | Content-Security-Policy header value |
| `permissions_policy` | string | Optional (`"none"`) | Permissions policy string or `"none"` | Browser hardware/API restrictions |

---

## 3. Headers Contract

### Response Headers Injected
| Header Name | Standard Config Example |
| :--- | :--- |
| `strict-transport-security` | `max-age=31536000; includeSubDomains` |
| `x-content-type-options` | `nosniff` |
| `x-frame-options` | `DENY` |
| `referrer-policy` | `strict-origin-when-cross-origin` |
| `content-security-policy` | `default-src 'self'` (if configured) |
| `permissions-policy` | `camera=(), microphone=(), geolocation=()` (if configured) |

---

## 4. Configuration Examples

### Minimal Production Configuration
```json
{
  "id": "security_headers",
  "priority": 100,
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
    "csp": "none",
    "permissions_policy": "none"
  }
}
```

### Full Configuration with CSP & Permissions Policy
```json
{
  "id": "security_headers",
  "priority": 100,
  "config": {
    "hsts": {
      "enable": true,
      "max_age_secs": 63072000,
      "include_subdomains": true,
      "preload": true
    },
    "frame_options": "SAMEORIGIN",
    "content_type_nosniff": true,
    "referrer_policy": "no-referrer-when-downgrade",
    "csp": "default-src 'self'; script-src 'self' 'nonce-rAnd0m'; object-src 'none'",
    "permissions_policy": "camera=(), microphone=(), geolocation=()"
  }
}
```

---

## 5. Architectural & Hot-Path Invariants

1. **Zero-Allocation Hot Path**: All `HeaderName` and `HeaderValue` pairs are pre-computed, pre-formatted, and validated once during initialization (`from_config`). Serving a response executes a tight contiguous iteration cloning `HeaderValue` (O(1) atomic ref-count increment on internal `Bytes`, strictly **0 heap allocations** on the request hot path).
2. **Granular Toggles**: Every single security header can be enabled, customized, or disabled individually by specifying `"none"`, ensuring applications requiring iframe embedding or custom staging configs are never broken.
