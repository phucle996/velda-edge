# Cross-Site Request Forgery Protection Plugin (`csrf`)

The `csrf` plugin enforces stateless edge **Cross-Origin Protection** in the **`pre_upstream`** phase (`AccessLevel::Read`, HTTP/1.1, HTTP/2, and HTTP/3) using W3C Fetch Metadata (`Sec-Fetch-Site`), `Origin`/`Referer` verification, and an optional constant-time Double-Submit Cookie mode for legacy backends.

---

## Core Design Principles

1. **Modern Tokenless Protection by Default**:
   Uses browser-enforced `Sec-Fetch-Site` + `Origin` provenance checks with zero custom token wire formats, zero shared secret rotation, zero heap allocations, and zero `SystemTime::now()` syscalls.
2. **Legacy Browser, Proxy & API Compatibility**:
   - Requests missing `Sec-Fetch-Site` evaluate `Origin` or `Referer` against `Host` / HTTP/2 `:authority` and `trusted_origins` inside a `#[cold] #[inline(never)]` out-of-line fallback branch so modern traffic L1i remains compact.
   - Pure stateless API/CLI callers with no `Cookie`, no `Sec-Fetch-Site`, and no `Origin`/`Referer` pass cleanly without false 403s, while cookie-bearing requests without any provenance fail closed (`403 Forbidden`).
3. **Optional Standard Double-Submit (`double_submit`)**:
   Legacy SPAs/backends using `XSRF-TOKEN` / `X-XSRF-TOKEN` (or `csrf` / `X-CSRF-Token`) can enable `double_submit` to compare cookie and header tokens in constant time across **all** HTTP/1.1 and HTTP/2 `Cookie` header lines.

---

## Configuration Schema

Configured in `plugins.json` with canonical plugin ID `"csrf"` (all unknown fields strictly rejected via `#[serde(deny_unknown_fields)]`).

| Field | Type | Required | Constraints / Allowed Values | Description |
| :--- | :--- | :---: | :--- | :--- |
| `safe_methods` | `array<string>` | Yes | Non-empty; subset of `GET`, `HEAD`, `OPTIONS`, `TRACE` | Idempotent HTTP methods that bypass CSRF validation in a 1-instruction bitmask check. |
| `trusted_origins` | `array<string>` | Yes | May be `[]`; explicit `http(s)://host[:port]` or `http(s)://*.domain.tld` | Cross-origin callers explicitly permitted for state-changing requests. Bare `*` and `https://*` are rejected. |
| `allow_same_site` | `bool` | Yes | `true` \| `false` | When `false` (recommended), `Sec-Fetch-Site: same-site` sibling subdomains are blocked unless listed in `trusted_origins`. |
| `enforce_origin` | `bool` | Yes | `true` \| `false` | Enables `Origin` / `Referer` provenance verification when `Sec-Fetch-Site` is absent. |
| `double_submit` | `object \| null` | Yes | `null` or `{ "cookie_name": string, "header_name": string }` | Optional constant-time Double-Submit check (`cookie_name` non-empty RFC 6265 token, `header_name` valid RFC 9110 header). |

---

## Configuration Examples

### Modern Tokenless Protection (`plugins.json`)
```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "csrf",
      "enabled": true,
      "config": {
        "safe_methods": ["GET", "HEAD", "OPTIONS"],
        "trusted_origins": ["https://app.example.com"],
        "allow_same_site": false,
        "enforce_origin": true,
        "double_submit": null
      }
    }
  ]
}
```

### Legacy App with Double-Submit Cookie (`plugins.json`)
```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "csrf",
      "enabled": true,
      "config": {
        "safe_methods": ["GET", "HEAD", "OPTIONS"],
        "trusted_origins": [],
        "allow_same_site": false,
        "enforce_origin": true,
        "double_submit": {
          "cookie_name": "XSRF-TOKEN",
          "header_name": "X-XSRF-TOKEN"
        }
      }
    }
  ]
}
```

---

## Constraints & Non-Goals

- **HTTP-Family Only**: Protects `SupportedProtocols::ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3). Non-HTTP listeners cannot attach `csrf`.
- **No Token Minting**: Velda Edge does not inject CSRF cookies on responses; modern routes rely on `Sec-Fetch-Site` / `Origin`, while `double_submit` verifies cookies/tokens issued by your backend.
- **No Proxy Header Mutation**: Declares `AccessLevel::Read`; downstream headers pass unmodified to upstreams.
