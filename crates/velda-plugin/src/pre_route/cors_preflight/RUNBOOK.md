# CORS Preflight Interceptor (`cors_preflight`) — SRE Operational Runbook & Configuration Guide

This document is the operational playbook and configuration guide (**SRE Runbook & Tuning Guide**) for engineers configuring, operating, and troubleshooting the `cors_preflight` pre-route plugin in Velda Edge.

---

## 1. Configuration Parameter Reference

`cors_preflight` intercepts HTTP `OPTIONS` preflight requests containing an `Origin` header directly at the edge (`pre_route`), short-circuiting with `204 No Content` (`Action::Respond`) or rejecting unauthorized origins with `403 Forbidden` (`Action::Reject(RejectReason::Forbidden)`).

```json
{
  "id": "cors_preflight",
  "config": {
    "allowed_origins": ["https://app.example.com", "https://*.corp.internal"],
    "allowed_methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    "allowed_headers": ["Content-Type", "Authorization", "X-Request-Id"],
    "allow_credentials": true,
    "max_age_secs": 3600
  }
}
```

### Parameter Specifications

| Parameter | Type | Required | Allowed Values / Constraints | Operational Meaning & Security Impact |
| :--- | :---: | :---: | :--- | :--- |
| **`allowed_origins`** | `Vec<String>` | **Yes** | Non-empty list: `"*"`, `"null"`, exact origin (`https://a.com`), or subdomain wildcard (`https://*.example.com`) | Evaluated against the incoming `Origin` header. Wildcard `"*"` and `"null"` are strictly rejected at config validation if `allow_credentials` is `true` (RFC 6454). |
| **`allowed_methods`** | `Vec<String>` | **Yes** | Valid RFC 9110 HTTP method tokens | Emitted in `Access-Control-Allow-Methods` and checked against `Access-Control-Request-Method`. |
| **`allowed_headers`** | `Vec<String>` | **Yes** | Valid RFC 9110 header names or `["*"]` | Emitted in `Access-Control-Allow-Headers` and checked against `Access-Control-Request-Headers`. |
| **`allow_credentials`** | `bool` | **Yes** | `true` or `false` | When `true`, emits `Access-Control-Allow-Credentials: true` and echoes the exact matched origin instead of `*`. |
| **`max_age_secs`** | `u64` | **Yes** | `0 ..= 86400` seconds | Emitted in `Access-Control-Max-Age` so browsers cache preflight responses up to 24 hours. |

---

## 2. Scenario-Based Configuration Recipes

### Scenario A: Authenticated SaaS Web Application (SPA + Cookies/Bearer)
*Goal: Allow credentials from primary domain and tenant subdomains with 12-hour browser cache.*

```json
{
  "id": "saas_cors_preflight",
  "config": {
    "allowed_origins": ["https://app.velda.io", "https://*.velda.io"],
    "allowed_methods": ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    "allowed_headers": ["Authorization", "Content-Type", "X-Requested-With"],
    "allow_credentials": true,
    "max_age_secs": 43200
  }
}
```

### Scenario B: Public Open Read-Only API (Unauthenticated)
*Goal: Allow any origin to query public endpoints without credentials.*

```json
{
  "id": "public_cors_preflight",
  "config": {
    "allowed_origins": ["*"],
    "allowed_methods": ["GET", "HEAD", "OPTIONS"],
    "allowed_headers": ["Content-Type"],
    "allow_credentials": false,
    "max_age_secs": 86400
  }
}
```

---

## 3. Observability & Troubleshooting Playbook

### Telemetry & Access Log Signals
- **Allowed Preflight**: Short-circuits with `204 No Content`, `Vary: Origin`, `Access-Control-Allow-*` headers (upstream never contacted).
- **Denied Preflight**: `Action::Reject(RejectReason::Forbidden)` (`403 Forbidden`), logged as `completion=reject:forbidden`.
- **Non-OPTIONS / Non-CORS Requests**: Immediate `Action::Continue` pass-through (zero header mutation).

### Incident Triage

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| Config rejected by `velda-sync` (`allow_credentials cannot be true`) | `allowed_origins` contains `"*"` or `"null"` alongside `allow_credentials: true`. | Replace `"*"` with explicit domains (`https://app.example.com`) or set `allow_credentials: false`. |
| Browser preflight returns `403 Forbidden` | Request `Origin`, `Access-Control-Request-Method`, or `Access-Control-Request-Headers` is not in the configured allowlists. | Check browser DevTools Network tab for the exact `Origin` and requested headers; add them to the config. |
| Subdomain wildcard `https://*.com` rejected at validation | Public suffix wildcarding is forbidden for security; wildcards require at least 2 domain labels (`https://*.example.com`). | Specify a concrete base domain (`https://*.example.com`). |
