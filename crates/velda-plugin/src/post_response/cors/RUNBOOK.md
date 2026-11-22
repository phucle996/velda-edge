# CORS Post-Response Plugin (`cors`) — Operations & Configuration Runbook

---

## 1. Parameter Reference Table

All parameters inside `config` are **required** (`CorsResponseConfig`):

| Field | Type | Required | Constraints / Allowed Values | Description |
| :--- | :--- | :---: | :--- | :--- |
| `allow_origins` | `string[]` | **Yes** | Non-empty array of `"*"`, `"null"`, `http(s)://host[:port]`, or `http(s)://*.domain` (no path, userinfo `@`, query `?`, or fragment `#`). | Authorized origins permitted to receive CORS response grants. |
| `allow_credentials` | `bool` | **Yes** | `true` or `false`. Forbidden when `allow_origins` contains `"*"` or `"null"` (RFC 6454). | Emits `Access-Control-Allow-Credentials: true` when enabled; scrubs it when `false`. |
| `expose_headers` | `string[]` | **Yes** | Array of RFC 9110 `1*tchar` header tokens (or `["*"]` when `allow_credentials: false`). | Joined with `", "` into `Access-Control-Expose-Headers`; scrubbed when empty. |

> [!NOTE]
> `max_age_secs` is intentionally absent from `post_response/cors`. Per W3C Fetch, `Access-Control-Max-Age` applies exclusively to `OPTIONS` preflight responses (`cors_preflight`).

---

## 2. Configuration Recipes

### Strict Multi-Tenant Web App (With Credentials)
```json
{
  "id": "cors",
  "config": {
    "allow_origins": ["https://app.velda.io", "https://*.partner.io"],
    "allow_credentials": true,
    "expose_headers": ["x-request-id", "x-total-count"]
  }
}
```

### Public Unauthenticated Read-Only API (`*`)
```json
{
  "id": "cors",
  "config": {
    "allow_origins": ["*"],
    "allow_credentials": false,
    "expose_headers": ["x-request-id"]
  }
}
```

### Minimal Strict Single-Origin (No Extra Headers Exposed)
```json
{
  "id": "cors",
  "config": {
    "allow_origins": ["https://console.velda.io"],
    "allow_credentials": false,
    "expose_headers": []
  }
}
```

---

## 3. Edge Single Source of Truth Semantics

| Scenario | `Allow-Origin` | `Allow-Credentials` | `Expose-Headers` | `Allow-Methods` / `Allow-Headers` / `Max-Age` | `Vary: Origin` |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Wildcard (`*`), `Origin` present | `*` | Removed | Set or Removed | Removed | Omitted |
| Whitelist/Credentials, `Origin` allowed | Reflected `<Origin>` | `"true"` or Removed | Set or Removed | Removed | Guaranteed present |
| Whitelist/Credentials, `Origin` missing or disallowed | Removed | Removed | Removed | Removed | Guaranteed present |

---

## 4. Incident Triage Playbook

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| Config rejected at sync: `allow_credentials cannot be true when allow_origins contains '*'` | RFC 6454 forbids `*` or `null` with credentials, including via `expose_headers: ["*"]`. | Replace `*` / `null` with explicit scheme+host origins or set `allow_credentials: false`. |
| Browser error: `No 'Access-Control-Allow-Origin' header` | Client `Origin` (scheme, host, or port) does not match `allow_origins`, so Edge stripped upstream CORS headers too. | Verify exact `Origin` header sent by browser (note `:port` and `http` vs `https`) and add to `allow_origins`. |
| Browser cannot read custom response header (`x-total-count`) | Header not declared in `expose_headers`. | Add header token to `expose_headers` array. |
| CDN served non-CORS cached response to browser | Older proxy upstream lacked `Vary: Origin`. | Verify `needs_vary_origin` is active (automatic when `!allow_all_origins \|\| allow_credentials`) and purge stale CDN cache once. |
