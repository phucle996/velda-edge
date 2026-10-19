# gRPC Fault Injection Plugin (`grpc_fault_injection`)

The `grpc_fault_injection` plugin delivers enterprise-grade **Chaos Engineering & Microservice Resilience Testing** specifically engineered for gRPC RPC traffic.

Operating in the `pre_upstream` execution phase, it deliberately introduces artificial delays and canonical gRPC abort responses directly before request dispatch to backend RPC services.

---

## 1. Architectural Mindset & Core Invariants

1. **Protocol Isolation Invariant (Rule 2.7)**:
   - Exclusively serves gRPC traffic (`SupportedProtocols::GRPC`). Web/REST HTTP routes are handled by `http_fault_injection`.
2. **Transparent Multi-Transport Support (TCP & UDP)**:
   - Intercepts and injects faults into **both gRPC over TCP** (HTTP/2 binary framing) and **gRPC over UDP** (QUIC datagrams) with 100% feature parity.
3. **Trailers-Only Framing (RFC 9113 / gRPC Core)**:
   - When aborting, the plugin **never returns an HTTP 5xx status code** (which would break client-side framing). Instead, it returns a canonical gRPC **Trailers-Only** response:
     ```http
     HTTP/2 200 OK
     content-type: application/grpc
     grpc-status: 14
     grpc-message: fault injected by edge
     ```
   - Client SDKs (such as `tonic`, `grpc-go`, `grpc-java`) parse this directly as a handled `Status::new(Code::Unavailable, "...")`.
4. **Disjoint Interval IP Matching**:
   - Compiles IPv4 and IPv6 CIDRs into sorted numeric intervals, executing in $O(\log N)$ binary search without linear scanning.
5. **Reverse Permission Granting**:
   - Declares `PluginCapability { header: Read, payload: None }`. Unaffected gRPC calls preserve **100% Zero-Copy streaming passthrough**.

---

## 2. Canonical gRPC Status Codes

The plugin supports all 17 canonical gRPC status codes (`0..=16`):

| Code | Name | Description | Recommended Testing Scenario |
| :---: | :--- | :--- | :--- |
| **14** | `UNAVAILABLE` | Service is currently unavailable / backend crashed. *(Default)* | Circuit-breaker failover & downstream retries. |
| **13** | `INTERNAL` | Serious internal error / database transaction crash. | Downstream error logging & alerting pipelines. |
| **8** | `RESOURCE_EXHAUSTED` | Quota exceeded / backend rate limited. | Client backoff & rate-limit handling. |
| **4** | `DEADLINE_EXCEEDED` | Request processing deadline expired. | Client RPC timeout budget verification. |
| **16** | `UNAUTHENTICATED` | Missing or invalid authentication token. | Token refresh / re-authentication workflows. |

---

## 3. Dynamic Metadata Overrides & Security Safeguards

### 3.1 Metadata Overrides
- `x-grpc-fault-delay-ms`: Overrides delay duration in milliseconds (e.g. `x-grpc-fault-delay-ms: 300`).
- `x-grpc-fault-abort-code`: Overrides abort status code with a canonical code from `0` to `16` (e.g. `x-grpc-fault-abort-code: 14`).

### 3.2 Security Safeguards
1. **Default Disabled (`allow_header_override: false`)**: Must be explicitly enabled in configuration.
2. **Safety Clamping (`max_override_delay_ms`)**: Delays are capped (default: `60,000ms` / 60s) to prevent Slowloris denial-of-service.
3. **Status Code Validation**: Values outside `0..=16` are rejected.
4. **Scope Protection**: If `client_ips` or `header_match` are configured, non-matching requests bypass immediately in $\sim 1\text{ns}$.

---

## 4. Configuration Schema

```json
{
  "id": "grpc_fault_injection",
  "phase": "pre_upstream",
  "config": {
    "delay": {
      "percentage": 10.0,
      "range_ms": [50, 200]
    },
    "abort": {
      "percentage": 5.0,
      "grpc_status": 14,
      "message": "service unavailable due to resilience drill"
    },
    "condition": {
      "client_ips": ["10.0.0.0/8"],
      "service": "/order.v1.OrderService/",
      "method": "/order.v1.OrderService/CreateOrder",
      "header_match": {
        "name": "x-chaos-token",
        "value": "secret-qa-token"
      },
      "allow_header_override": true,
      "max_override_delay_ms": 30000
    }
  }
}
```

### Field Definitions

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `delay.percentage` | float | `0.0` | Probability ($0.0\% - 100.0\%$) of injecting delay into scoped RPCs. |
| `delay.range_ms` | `[u64, u64]` | None | Range `[min_ms, max_ms]` of delay in milliseconds. |
| `delay.fixed_ms` | u64 | None | Convenience alternative to `range_ms: [fixed, fixed]`. |
| `abort.percentage` | float | `0.0` | Probability ($0.0\% - 100.0\%$) of aborting scoped RPCs. |
| `abort.grpc_status` | u32 | `14` | Canonical gRPC status code (`0..=16`). |
| `abort.message` | string | `"fault injected by edge"` | Diagnostic message in `grpc-message` trailer. |
| `condition.client_ips` | array[string] | `[]` | Whitelisted IPv4/IPv6 CIDRs. Non-matching traffic bypasses immediately. |
| `condition.service` | string | None | Prefix match on gRPC service (e.g. `"/payment.v1.PaymentService/"`). |
| `condition.method` | string | None | Exact match on RPC method (e.g. `"/payment.v1.PaymentService/Charge"`). |
| `condition.header_match.name` | string | None | Metadata header required to trigger fault injection. |
| `condition.header_match.value` | string | None | Optional exact value match. |
| `condition.allow_header_override` | bool | `false` | Enables `x-grpc-fault-*` client overrides. |
| `condition.max_override_delay_ms` | u64 | `60000` | Safety clamp ceiling for client-requested delay durations. |
