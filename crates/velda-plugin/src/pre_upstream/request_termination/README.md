# Request Termination Plugin (`request_termination`)

High-performance HTTP request short-circuiting plugin evaluated during the `pre_upstream` phase. Immediately halts request pipeline execution at the Edge and returns a synthetic HTTP response without contacting upstream backends or leasing upstream connections.

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: Every configuration parameter must be declared explicitly in `plugins.json`. No silent fallbacks, implicit defaults, or hidden behaviors are permitted.
>
> **Zero Upstream Dispatch**: When this plugin executes, upstream endpoint selection, circuit breakers, and connection pool leasing are completely bypassed.
>
> **Never Specify `phase` in User Configuration**: The execution phase is statically inferred by Velda as `pre_upstream`.
>
> **System-Level Request ID Invariant**: The synthetic response automatically carries the incoming request's `x-request-id`, ensuring full observability continuity across `post_response` plugins (such as `access_log` and `security_headers`).

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `request_termination` |
| **Lifecycle Phase** | `pre_upstream` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Capability** | `PluginCapability::header_only(AccessLevel::Read)` |
| **Hot Path Latency** | ~5ns to 15ns (Pre-compiled response, static byte slice, zero heap allocation) |

---

## 2. Configuration Parameters

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `status_code` | `integer` | **Yes** | `200..=599` | `503` | HTTP status code returned to the client. |
| `body` | `string` | **Yes** | Any string payload, or `""` for protocol RFC canonical text | `"{\"error\": \"Service undergoing scheduled maintenance\"}"` | Response body payload returned to the client. |
| `content_type` | `string` | **Yes** | Valid MIME string, e.g. `"application/json"`, `"text/plain"` | `"application/json"` | Response `Content-Type` header value. |
| `headers` | `object` | **Yes** | Key-value pairs of string headers (`{ "key": "value" }`) | `{"retry-after": "300"}` | Custom static HTTP headers to include in the synthetic response. |

---

## 3. Configuration Manifest (`plugins.json`)

```json
{
  "plugins": [
    {
      "id": "maintenance_termination",
      "plugin": "request_termination",
      "config": {
        "status_code": 503,
        "body": "{\"error\": \"Service undergoing scheduled maintenance\"}",
        "content_type": "application/json",
        "headers": {
          "retry-after": "300",
          "x-maintenance-window": "2026-10-22-0300"
        }
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
      "id": "route_maintenance",
      "match": {
        "path": {
          "prefix": "/api/"
        }
      },
      "plugins": [
        "maintenance_termination"
      ],
      "upstream": "cluster_main"
    }
  ]
}
```

---

## 5. Common Use Cases

### 5.1 Scheduled Maintenance Mode (503 Service Unavailable)
Immediately return 503 with a retry advisory without placing load on backends:
```json
{
  "status_code": 503,
  "body": "{\"status\": 503, \"message\": \"Down for scheduled database migration\"}",
  "content_type": "application/json",
  "headers": {
    "retry-after": "120"
  }
}
```

### 5.2 Edge Redirect (301 Moved Permanently / 302 Found)
Redirect clients directly from Edge to a new URL:
```json
{
  "status_code": 301,
  "body": "",
  "content_type": "text/plain",
  "headers": {
    "location": "https://example.com/v2/login"
  }
}
```

### 5.3 Static Mock or Edge Health Probe (200 OK)
Serve lightweight edge probes or health check mocks:
```json
{
  "status_code": 200,
  "body": "{\"status\":\"healthy\"}",
  "content_type": "application/json",
  "headers": {}
}
```
