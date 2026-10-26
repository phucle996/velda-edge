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
| `scope.client_ips` | Array of strings | No | Valid IPv4/IPv6 CIDRs or addresses (`[]` if none) | `["10.0.0.0/8"]` | Targeting filter: request client IP must match (authoritative `x-real-ip` prioritized). |
| `scope.header_match` | Object | No | `{"name": "...", "exact_match": "..."}` | `{"name": "x-chaos", "exact_match": "qa"}` | Targeting filter: request must contain specified header. |
| `scope.allow_header_override` | Boolean | No | `true` or `false` (defaults to `false`) | `false` (Prod) / `true` (Test) | Allows `x-fault-delay-ms` and `x-fault-abort-code` headers. Requires scoping to prevent Slowloris attacks. |
| `scope.max_override_delay_ms` | Integer (u64) | No | `1 ..= 60000` ms (defaults to `5000`) | `5000` | Safety clamp on dynamic delay override to prevent worker pool starvation. |
| `delay.enabled` | Boolean | **Yes** | `true` or `false` | `false` (Prod) / `true` (Chaos test) | Whether artificial network delay injection is active. |
| `delay.fixed_delay_ms` | Integer (u64) | **Yes** | `1 ..= 60000` ms (when `enabled: true`), or `0` if disabled | `200` | Fixed artificial delay in milliseconds injected before upstream request. |
| `delay.percentage` | Integer (u32) | **Yes** | `0 ..= 100` (%) | `10` (when testing) / `0` (Prod) | Percentage probability ($0\%$ to $100\%$) of applying delay to requests. |
| `abort.enabled` | Boolean | **Yes** | `true` or `false` | `false` (Prod) / `true` (Chaos test) | Whether simulated upstream failure abort is active. |
| `abort.http_status` | Integer (u16) | **Yes** | `400 ..= 599` | `503` (Service Unavailable) | Synthetic HTTP client/server error status code returned immediately to client. |
| `abort.percentage` | Integer (u32) | **Yes** | `0 ..= 100` (%) | `5` (when testing) / `0` (Prod) | Percentage probability ($0\%$ to $100\%$) of aborting requests with synthetic response. |

---

## 3. Execution Precedence & Security Safeguards

When both `delay` and `abort` are active for a matching request, the gateway delays execution first, then aborts with the synthetic HTTP status code:
$$\mathbf{Request} \longrightarrow \mathbf{Delay\ (sleep)} \longrightarrow \mathbf{Abort\ (503\ Response)} \longrightarrow \mathbf{Client}$$
This accurately models real-world backend behavior such as database connection timeouts and cascading worker thread exhaustion.

### Anti-Leak & Slowloris Protections
- **Header Purging**: When `allow_header_override` is enabled, dynamic test tokens (`x-fault-delay-ms` and `x-fault-abort-code`) are stripped from request headers before forwarding upstream, preventing leakage into backend microservices.
- **Mandatory Scoping**: `allow_header_override` cannot be enabled globally without declaring either `client_ips` or `header_match`, preventing malicious clients on the public internet from exploiting dynamic delays.
- **Authoritative IP Extraction**: Evaluates edge `x-real-ip` first, falling back to nearest right-most proxy in `x-forwarded-for` to eliminate IP spoofing.

---

## 4. Configuration Examples

### Chaos Testing Manifest with Targeted Scope (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "http_fault_injection",
      "config": {
        "scope": {
          "client_ips": ["10.0.0.0/8"],
          "header_match": {
            "name": "x-chaos-test",
            "exact_match": "qa-stage"
          },
          "allow_header_override": true,
          "max_override_delay_ms": 3000
        },
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

### Staging Manifest with Dynamic QA Header Overrides (`plugins.json`)

Enables dynamic fault testing on-demand without constantly modifying configuration files. Downstream QA clients supply `x-fault-delay-ms` and `x-fault-abort-code`, scoped strictly to QA subnets:

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "http_fault_injection",
      "config": {
        "scope": {
          "client_ips": ["10.100.0.0/16"],
          "allow_header_override": true,
          "max_override_delay_ms": 5000
        },
        "delay": {
          "enabled": false,
          "fixed_delay_ms": 0,
          "percentage": 0
        },
        "abort": {
          "enabled": false,
          "http_status": 500,
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

---

## 5. Documentation Suite Links

For comprehensive architectural design, empirical Azure benchmarks, and security test matrices, refer to the dedicated documents:

- **[Architectural Specification](ARCHITECTURE.md)**: Internal data structures, disjoint interval IP matcher, and memory layout.
- **[Performance Benchmark Report](BENCHMARK.md)**: Live Azure VM cluster measurements, latency percentiles, and Little's Law verification.
- **[Test Plan & Security Verification](TEST.md)**: Automated unit test suite, Anti-Spoofing, Anti-Slowloris, and live cURL reproduction logs.


