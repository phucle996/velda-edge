# Request Termination Plugin (`request_termination`)

Lightweight HTTP request short-circuiting plugin evaluated during the `pre_upstream` phase. Returns `Action::Respond(L7Response)` with a pre-compiled synthetic response so the outer `velda-edge` pipeline cancels upstream dispatch, stamps `x-request-id`, executes `post_response` plugins, and writes the HTTP response.

```text
  Sync / Compile Time
    JSON Config ─► validate_request_termination_config()
                     └─► RequestTerminationPlugin (base_response: L7Response)

  Frontline Hot Path (PreUpstreamPlugin)
    on_upstream_request(_req, _targets)
      └─► Action::Respond(self.base_response.clone())
            └─► velda-edge handles target cancellation, x-request-id, post_response_hooks, and wire I/O
```

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `request_termination` |
| **Lifecycle Phase** | `PreUpstreamPlugin` (`pbww() == 0`) |
| **Supported Protocols** | HTTP only (`SupportedProtocols::ALL_HTTP`: `http/1.1`, `http/2`, `http/3`) |
| **Capability** | `PluginCapability::default()` (`header: AccessLevel::None`, `payload: AccessLevel::None`) |

---

## 2. Configuration Schema (`#[serde(deny_unknown_fields)]`)

| Field | Type | Required | Constraints |
| :--- | :--- | :--- | :--- |
| `status_code` | `integer` | **Yes** | `200..=599` (valid HTTP `StatusCode`). `204`, `205`, and `304` require `body == ""` and `content_type == ""`. |
| `body` | `string` | **Yes** | Literal payload bytes. `""` produces `Body::Empty`. |
| `content_type` | `string` | **Yes** | Non-empty valid MIME header value when `body` is non-empty; must be `""` when `body == ""`. |
| `headers` | `object` | **Yes** | Static response headers (`{ "key": "value" }`). Case-insensitive duplicates and `content-length`, `content-type`, `transfer-encoding`, `connection`, `keep-alive`, `proxy-connection`, `upgrade`, `te`, `trailer`, `x-request-id` are rejected. |

- Unknown fields are rejected at sync validation time.
- `204` and `304` responses omit `Content-Length` per RFC 9112 §6.2 / RFC 9110 §15.4.5.

---

## 3. Examples

### Maintenance Mode (`503 Service Unavailable`)
```json
{
  "id": "request_termination",
  "config": {
    "status_code": 503,
    "body": "{\"error\": \"Service undergoing scheduled maintenance\"}",
    "content_type": "application/json",
    "headers": {
      "retry-after": "300"
    }
  }
}
```

### Empty Response / Redirect (`204 No Content` / `301 Moved Permanently`)
```json
{
  "id": "request_termination",
  "config": {
    "status_code": 301,
    "body": "",
    "content_type": "",
    "headers": {
      "location": "https://example.com/v2/login"
    }
  }
}
```
