# Request Termination Plugin (`request_termination`)

High-performance HTTP request short-circuiting plugin evaluated during the `pre_upstream` phase. Immediately halts request pipeline execution at the Edge and returns a synthetic HTTP response without contacting upstream backends or leasing upstream connections.

> [!IMPORTANT]
> **Zero Upstream Dispatch**: When this plugin executes, upstream endpoint selection, circuit breakers, and connection pool leasing are completely bypassed.
>
> **Never Specify `phase` in User Configuration**: The execution phase is statically inferred by Velda as `pre_upstream`.
>
> **Protocol-Owned Canonical Response**: Response payloads use standardized RFC plain-text reason strings (or empty bodies for redirects/no-content), eliminating arbitrary body schema bloat at the Edge.
>
> **System-Level Request ID Invariant**: The synthetic response automatically carries the request's `x-request-id`, ensuring full observability continuity across `post_response` plugins (such as `access_log` and `security_headers`).

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `request_termination` |
| **Lifecycle Phase** | `pre_upstream` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` |
| **Hot Path Latency** | ~5ns to 15ns (Pre-compiled response, static byte slice, zero heap allocation) |

---

## 2. Configuration Schema

```json
{
  "id": "request_termination",
  "config": {
    "status_code": 503,
    "headers": {
      "retry-after": "300",
      "x-maintenance-window": "2026-10-20-0300"
    }
  }
}
```

### Configuration Fields

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `status_code` | `integer` (200..=599) | `200` | HTTP status code returned to the client. Uses canonical RFC text for errors or empty body for redirects/204. |
| `headers` | `object` or `array` | `{}` | Custom static HTTP headers to include in the synthetic response. Both object format (`{"retry-after": "300"}`) and array format (`[{"name": "retry-after", "value": "300"}]`) are supported. |

---

## 3. Common Use Cases

### 3.1 Scheduled Maintenance Mode (503 Service Unavailable)
Immediately return 503 with a retry advisory without putting load on backends:
```json
{
  "id": "request_termination",
  "config": {
    "status_code": 503,
    "headers": {
      "retry-after": "120"
    }
  }
}
```

### 3.2 Edge Redirect (301 Moved Permanently / 302 Found)
Redirect clients directly from Edge to a new URL:
```json
{
  "id": "request_termination",
  "config": {
    "status_code": 301,
    "headers": {
      "location": "https://example.com/login"
    }
  }
}
```

### 3.3 Static Mock or Edge Health Probe (200 OK)
Serve lightweight edge probes or mocks:
```json
{
  "id": "request_termination",
  "config": {
    "status_code": 200
  }
}
```
