# Request Termination Plugin (`request_termination`)

High-performance HTTP request short-circuiting plugin evaluated during the `pre_upstream` phase. Immediately halts request pipeline execution at the Edge and returns a synthetic HTTP response without contacting upstream backends or leasing upstream connections.

> [!IMPORTANT]
> **Zero Upstream Dispatch**: When this plugin executes, upstream endpoint selection, circuit breakers, and connection pool leasing are completely bypassed.
>
> **Never Specify `phase` in User Configuration**: The execution phase is statically inferred by Velda as `pre_upstream`.
>
> **Post-Response Pipeline Continuity**: Because the synthetic response is wrapped into [`velda_core::Action::Respond`], it seamlessly traverses subsequent `post_response` plugins (such as `access_log`, `security_headers`, and `response_header_mutation`) before downstream wire encoding.

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `request_termination` |
| **Lifecycle Phase** | `pre_upstream` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` |
| **Hot Path Latency** | ~5ns to 15ns (Pre-compiled response, zero-copy payload reference-count increment) |

---

## 2. Configuration Schema

```json
{
  "id": "request_termination",
  "config": {
    "status_code": 503,
    "body": "Service Under Scheduled Maintenance\n",
    "headers": {
      "retry-after": "300",
      "x-maintenance-window": "2026-10-20-0300"
    },
    "echo_request_id": true
  }
}
```

### Configuration Fields

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `status_code` | `integer` (200..=599) | `200` | HTTP status code returned to the client. |
| `body` | `string` | `""` | Response body. If non-empty and no `content-type` is specified in `headers`, defaults to `text/plain; charset=utf-8`. |
| `headers` | `object` or `array` | `{}` | Custom static HTTP headers to include in the synthetic response. Both object format (`{"retry-after": "300"}`) and array format (`[{"name": "retry-after", "value": "300"}]`) are supported. |
| `echo_request_id` | `boolean` | `true` | When `true`, automatically extracts `x-request-id` from incoming request headers and injects it into the synthetic response. |

---

## 3. Common Use Cases

### 3.1 Scheduled Maintenance Mode (503 Service Unavailable)
Immediately return 503 with a retry advisory without putting load on backends:
```json
{
  "id": "request_termination",
  "config": {
    "status_code": 503,
    "body": "Service Temporarily Unavailable\n",
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

### 3.3 Static Mock or Edge Health Check (200 OK)
Serve lightweight edge probes or mocks:
```json
{
  "id": "request_termination",
  "config": {
    "status_code": 200,
    "body": "OK\n"
  }
}
```

---

## 4. Pipeline Execution & Interaction Model

```
Downstream Client
      │
      ▼
┌──────────────┐
│  pre_route   │ (IP Filter, WAF, CORS Preflight)
└──────┬───────┘
      │
      ▼
┌──────────────┐
│ pre_upstream │
│              │ ──> [request_termination] (Returns Action::Respond)
└──────────────┘             │
                             │ (Bypasses Upstream Pool & Network)
                             ▼
                      ┌──────────────┐
                      │post_response │ (security_headers, access_log)
                      └──────┬───────┘
                             │
                             ▼
                      Downstream Client
```

1. **Upstream Isolation**: Upstream DNS resolution, endpoint health checks, circuit breakers, and connection leases are never triggered.
2. **Observability Continuity**: Because the synthetic response emits an `L7Response`, downstream access logging (`access_log`) and metric exporters capture the termination with the exact status code and latency.
