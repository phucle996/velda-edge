# Cross-Site Request Forgery (CSRF) Protection Plugin (`csrf`)

High-performance, cryptographic Layer 7 CSRF defense plugin evaluated during the `pre_upstream` execution phase. Protects upstream APIs, financial transactions, and state-changing endpoints against cross-site request forgery, W3C Simple Request form traps, and subdomain cookie tossing attacks.

> [!IMPORTANT]
> **Pre-Upstream Defense Invariant**: Evaluated after route matching and endpoint selection, prior to upstream request dispatch. Blocks mutating requests (`POST`, `PUT`, `DELETE`, `PATCH`) before touching upstream services.
>
> **100% Explicit Declaration**: All configuration parameters must be declared explicitly in `plugins.json`. No hidden or implicit fallbacks are permitted.
>
> **Cryptographic Signed Token Defense**: When `secret_key` is configured, verifies 114-byte HMAC-SHA256 signed stateless tokens (`<timestamp_hex>.<nonce_hex>.<signature_hex>`) with TTL enforcement, neutralizing RFC 6265 §8.5 Subdomain Cookie Tossing.

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `csrf` (alias: `csrf_protection`) |
| **Lifecycle Phase** | `pre_upstream` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Capability** | `PluginCapability::header_only(AccessLevel::Read)` |
| **Hot Path Latency** | Safe methods: ~0.5ns (Bitmask $O(1)$) \| Mutating checks: ~150ns (HMAC + 128-bit SIMD) |
| **Memory Allocation** | **0 bytes on hot path** (Stack buffers `[u8; 64]` and zero-alloc byte slices) |

---

## 2. Configuration Parameters

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `cookie_name` | `string` | **Yes** | Non-empty ASCII string | `__Host-velda_csrf` | Name of the cookie carrying the CSRF token. |
| `header_name` | `string` | **Yes** | Valid RFC 9110 HTTP header token | `X-CSRF-Token` | Name of the custom request header carrying the matching token. |
| `secret_key` | `string \| null` | **Yes** | `null` or string $\ge 16$ chars | `sec_k89f2a019482bca881726c0981273412` | Cryptographic secret for signing stateless tokens via HMAC-SHA256. |
| `token_ttl_secs` | `integer` | **Yes** | $\ge 1$ second | `7200` (2 hours) | Lifetime of signed tokens before expiration. |
| `safe_methods` | `array[string]` | **Yes** | Subset of `["GET", "HEAD", "OPTIONS", "TRACE"]` | `["GET", "HEAD", "OPTIONS"]` | Idempotent HTTP methods exempt from CSRF validation. |
| `trusted_origins` | `array[string]` | **Yes** | Array of exact or wildcard origins (`https://*.corp.internal`) | `["https://app.example.com"]` | External origins allowed to perform cross-origin state changes. |
| `enforce_fetch_metadata` | `boolean` | **Yes** | `true` or `false` | `true` | Enforces W3C Fetch Metadata (`Sec-Fetch-Site`). |
| `enforce_origin` | `boolean` | **Yes** | `true` or `false` | `true` | Enforces RFC 6454 Origin/Referer verification against `Host`. |

---

## 3. Configuration Manifest (`plugins.json`)

```json
{
  "plugins": [
    {
      "id": "banking_csrf_guard",
      "plugin": "csrf",
      "config": {
        "cookie_name": "__Host-velda_csrf",
        "header_name": "X-CSRF-Token",
        "secret_key": "sec_k89f2a019482bca881726c0981273412",
        "token_ttl_secs": 7200,
        "safe_methods": ["GET", "HEAD", "OPTIONS"],
        "trusted_origins": [
          "https://app.example.com",
          "https://*.corp.example.com"
        ],
        "enforce_fetch_metadata": true,
        "enforce_origin": true
      }
    }
  ]
}
```
