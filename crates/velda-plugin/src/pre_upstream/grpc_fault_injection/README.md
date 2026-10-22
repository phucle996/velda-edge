# gRPC Fault Injection Plugin (`grpc_fault_injection`)

Enterprise-grade Chaos Engineering & Microservice Resilience Testing plugin specifically engineered for gRPC RPC traffic over HTTP/2 and HTTP/3 (QUIC). Evaluated during the `pre_upstream` execution phase prior to upstream gRPC backend forwarding.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `grpc_fault_injection` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Strictly FIFO based on route `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (or `Action::Respond` on abort) |
| **Payload Passthrough** | **100% Zero-Copy streaming**. Non-aborted gRPC messages flow through unbuffered |
| **Supported Protocols** | `gRPC` over HTTP/2 and HTTP/3 |
| **Protocol Isolation** | Serves gRPC RPC traffic only. For standard HTTP REST/Web traffic, use [`http_fault_injection`](../http_fault_injection/) |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. Even if an injection block is disabled, all subfields must be explicitly supplied with valid numbers or sentinels.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `delay.enabled` | Boolean | **Yes** | `true` or `false` | `false` (Prod) / `true` (Chaos test) | Whether artificial network delay injection is active. |
| `delay.fixed_delay_ms` | Integer (u64) | **Yes** | `1 ..= 60000` ms (when `enabled: true`), or `0` if disabled | `200` | Fixed artificial delay in milliseconds injected before forwarding gRPC RPC. |
| `delay.percentage` | Integer (u32) | **Yes** | `0 ..= 100` (%) | `10` (when testing) / `0` (Prod) | Percentage probability ($0\%$ to $100\%$) of applying delay to gRPC calls. |
| `abort.enabled` | Boolean | **Yes** | `true` or `false` | `false` (Prod) / `true` (Chaos test) | Whether simulated gRPC error abort is active. |
| `abort.grpc_status` | Integer (u32) | **Yes** | `0 ..= 16` (Canonical gRPC status codes) | `14` (`UNAVAILABLE`) | Canonical gRPC status code returned in `grpc-status` trailers. |
| `abort.grpc_message` | String | **Yes** | Non-empty string | `"synthetic chaos injection error"` | Human-readable error diagnostic message in `grpc-message` trailer. |
| `abort.percentage` | Integer (u32) | **Yes** | `0 ..= 100` (%) | `5` (when testing) / `0` (Prod) | Percentage probability ($0\%$ to $100\%$) of aborting gRPC calls with synthetic trailers. |

---

## 3. gRPC Status Codes Reference

| Code | Name | Description |
| :---: | :--- | :--- |
| `14` | `UNAVAILABLE` | Service currently unavailable (transient network or server overload). |
| `16` | `UNAUTHENTICATED` | Request does not have valid authentication credentials. |
| `4` | `DEADLINE_EXCEEDED` | Deadline expired before operation could complete. |
| `13` | `INTERNAL` | Serious internal system error. |

---

## 4. Configuration Examples

### Chaos Testing Manifest (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "grpc_fault_injection",
      "config": {
        "delay": {
          "enabled": true,
          "fixed_delay_ms": 200,
          "percentage": 10
        },
        "abort": {
          "enabled": true,
          "grpc_status": 14,
          "grpc_message": "synthetic upstream unavailable",
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
      "id": "grpc_fault_injection",
      "config": {
        "delay": {
          "enabled": false,
          "fixed_delay_ms": 1,
          "percentage": 0
        },
        "abort": {
          "enabled": false,
          "grpc_status": 14,
          "grpc_message": "disabled",
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
  "id": "grpc-payments-route",
  "match": {
    "path": "/payments.PaymentService/ProcessPayment"
  },
  "upstream_id": "grpc-payments-backend",
  "plugins": [
    "grpc_fault_injection"
  ]
}
```
