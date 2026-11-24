# CSRF Protection Operations Runbook

## 1. Deployment Modes

1. **Modern Web / SPA / BFF (`double_submit: null`)**:
   - Protects browser sessions using `Sec-Fetch-Site` + `Origin`/`Referer` with zero cookie parsing and zero backend changes.
   - Non-browser API/CLI clients sending `Authorization: Bearer ...` without cookies or Fetch Metadata pass without false `403 Forbidden`.
2. **Legacy Backend Mode (`double_submit: { cookie_name, header_name }`)**:
   - Enables Gate 4 alongside Cross-Origin checks for backends issuing `XSRF-TOKEN` / `X-XSRF-TOKEN` or similar cookie/header pairs.
   - Scans all HTTP/1.1 and HTTP/2 `Cookie` header lines so multi-line cookie headers do not cause spurious rejections.

---

## 2. Live Verification Probes

```bash
# 1. Safe GET method passes unconditionally
curl -i http://<gateway>:8080/ping

# 2. Modern browser same-origin POST passes without CSRF cookie/token
curl -i -X POST -H "Sec-Fetch-Site: same-origin" http://<gateway>:8080/api/action

# 3. Cross-site attack from untrusted origin is rejected with 403 Forbidden
curl -i -X POST -H "Sec-Fetch-Site: cross-site" -H "Origin: https://evil.com" http://<gateway>:8080/api/action

# 4. Cookie-authenticated request with missing Sec-Fetch-Site, Origin, and Referer is rejected (403)
curl -i -X POST -H "Cookie: session=abc" http://<gateway>:8080/api/action
```

---

## 3. Troubleshooting & Incident Response

| Symptom / Error | Root Cause | Resolution |
| :--- | :--- | :--- |
| **`non-idempotent method 'POST' cannot be declared in 'safe_methods'`** | Config error rejected by `velda-sync`. | Restrict `safe_methods` to `GET`, `HEAD`, `OPTIONS`, `TRACE`. |
| **`wildcard trusted origin ... must scope to a domain`** or **`unknown field 'secret_key'`** | Unsafe wildcard (`*`, `https://*`, `https://*.com`) or removed legacy HMAC field (`secret_key`, `token_ttl_secs`). | Specify explicit `https://host` or `https://*.domain.tld` origins and remove legacy HMAC keys. |
| **Legitimate sibling subdomain receives `403`** | `Sec-Fetch-Site: same-site` is blocked by default (`allow_same_site: false`). | Add that exact subdomain to `trusted_origins` (recommended) or set `allow_same_site: true` if all subdomains are trusted. |
| **Legacy Double-Submit returns `403` on valid requests** | Backend splits cookies over HTTP/2 or client sends blank/missing custom header. | Confirm `double_submit.cookie_name` and `double_submit.header_name` match exact backend names and header is non-empty. |
| **Immediate Bypass / Rollback** | Emergency policy relaxation needed on a route. | Remove `"csrf"` from `route.plugins` in `routes.json` or set `"enabled": false` in `plugins.json`; LKG swaps atomically. |
