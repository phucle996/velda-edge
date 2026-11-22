# HTTPS Redirect (`https_redirect`) — Operations Runbook

## 1. Parameter Reference

| Field | Type | Default | Constraints | Description |
| :--- | :--- | :---: | :--- | :--- |
| `status_code` | `u16` | `308` | `301` or `308` | Redirect HTTP status code (`308` preserves HTTP method & body; `301` is legacy permanent redirect). |
| `https_port` | `u16` | `443` | `1..=65535` | Target HTTPS port. When `443`, omitted from `Location` URI per RFC 9110. |

---

## 2. Scenario-Based Configuration Recipes

### Recipe A: Production Edge (`443` Standard HTTPS, Method-Preserving)
```json
{
  "id": "https_redirect",
  "config": {
    "status_code": 308,
    "https_port": 443
  }
}
```

### Recipe B: Non-Standard HTTPS Port (e.g., Lab / Staging `:8443`)
```json
{
  "id": "https_redirect",
  "config": {
    "status_code": 308,
    "https_port": 8443
  }
}
```
Redirects `http://example.com:8080/login?a=1` $\to$ `https://example.com:8443/login?a=1`.

---

## 3. Observability Signals

| Outcome | HTTP Wire Status | Headers Emitted |
| :--- | :--- | :--- |
| **Already HTTPS** | Pass-through (`Action::Continue`) | None |
| **Upgraded** | `301 Moved Permanently` or `308 Permanent Redirect` | `location: https://...`, `content-length: 0` |
| **Missing/Invalid Host** | `400 Bad Request` (`RejectReason::BadRequest`) | Standard rejection headers (`x-request-id`) |

---

## 4. Incident Triage Playbook

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| **Redirect loop (`https://` $\to$ `https://`)** | Upstream TLS is terminated externally without `https` listener, or custom port mismatch. | Verify `https_port` matches the external TLS listener port and `x-forwarded-proto` is `https`. |
| **Client POST becomes GET after redirect** | Configured `status_code: 301` (browsers rewrite POST $\to$ GET on 301). | Switch to `"status_code": 308`. |
| **400 Bad Request on redirect** | Client omitted `Host` header or sent malformed IPv6/control bytes (`<= 0x20`, `0x7f`). | Expected security rejection; inspect client request. |
