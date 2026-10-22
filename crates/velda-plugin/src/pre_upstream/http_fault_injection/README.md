# HTTP Fault Injection Plugin (`http_fault_injection`)

Enterprise-grade Chaos Engineering & Microservice Resilience Testing plugin specifically engineered for HTTP traffic (HTTP/1.1, HTTP/2 Web/REST, and HTTP/3). Evaluated during the `pre_upstream` execution phase prior to upstream backend dispatch.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `http_fault_injection` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Strictly FIFO based on route `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (or `Action::Respond` on abort) |
| **Payload Passthrough** | **100% Zero-Copy streaming**. Non-aborted traffic maintains zero-copy byte streaming |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Protocol Isolation** | Serves HTTP traffic only. For gRPC RPC traffic, use [`grpc_fault_injection`](../grpc_fault_injection/) |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. Even if an injection block is disabled, all subfields must be explicitly supplied with valid numbers or sentinels.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `delay.enabled` | Boolean | **Yes** | `true` or `false` | `false` (Prod) / `true` (Chaos test) | Whether artificial network delay injection is active. |
| `delay.fixed_delay_ms` | Integer (u64) | **Yes** | `1 ..= 60000` ms (when `enabled: true`), or `0` if disabled | `200` | Fixed artificial delay in milliseconds injected before upstream request. |
| `delay.percentage` | Integer (u32) | **Yes** | `0 ..= 100` (%) | `10` (when testing) / `0` (Prod) | Percentage probability ($0\%$ to $100\%$) of applying delay to requests. |
| `abort.enabled` | Boolean | **Yes** | `true` or `false` | `false` (Prod) / `true` (Chaos test) | Whether simulated upstream failure abort is active. |
| `abort.http_status` | Integer (u16) | **Yes** | `200 ..= 599` | `503` (Service Unavailable) | Synthetic HTTP status code returned immediately to client. |
| `abort.percentage` | Integer (u32) | **Yes** | `0 ..= 100` (%) | `5` (when testing) / `0` (Prod) | Percentage probability ($0\%$ to $100\%$) of aborting requests with synthetic response. |

---

## 3. Execution Precedence

When both `delay` and `abort` are active for a matching request, the gateway delays execution first, then aborts with the synthetic HTTP status code:
$$\mathbf{Request} \longrightarrow \mathbf{Delay\ (sleep)} \longrightarrow \mathbf{Abort\ (503\ Response)} \longrightarrow \mathbf{Client}$$
This accurately models real-world backend behavior such as database connection timeouts and cascading worker thread exhaustion.

---

## 4. Configuration Examples

### Chaos Testing Manifest (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "http_fault_injection",
      "config": {
        "delay": {
          "enabled": true,
          "fixed_delay_ms": 200,
          "percentage": 10
        },
        "abort": {
          "enabled": true,
          "http_status": 503,
          "percentage": 5
        }
      }
    }
  ]
}
```

### Production Manifest with Fault Injection Disabled (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "http_fault_injection",
      "config": {
        "delay": {
          "enabled": false,
          "fixed_delay_ms": 1,
          "percentage": 0
        },
        "abort": {
          "enabled": false,
          "http_status": 503,
          "percentage": 0
        }
      }
    }
  ]
}
```

### Route Attachment (`routes.json`)

```json
{
  "id": "api-orders-route",
  "match": {
    "path": "/api/v1/orders"
  },
  "upstream_id": "orders-backend",
  "plugins": [
    "http_fault_injection",
    "jwt_auth"
  ]
}
```
