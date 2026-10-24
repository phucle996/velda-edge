# Rate Limit Plugin (`rate_limit`)

High-throughput request pacing and traffic shaping engine with multi-algorithm support, dual-backend architecture (in-memory lock-striped concurrent state + distributed Redis synchronization via auxiliary background workers), flexible key resolution strategies, and resilient failover behaviors.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `rate_limit` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (Generates `Action::Reject(RejectReason::TooManyRequests)`) |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Hot-Path Overhead** | ~15ns local in-memory lock-striped concurrency; **Zero Network I/O** on traffic worker cores |

### What it Protects
Protects upstream backend services, connection pools, and edge proxy hardware against DDoS amplification, brute-force credential stuffing, API scraping, and upstream resource saturation.

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `algorithm` | String | **Yes** | `"token_bucket"` or `"sliding_window"` | `"token_bucket"` | Traffic pacing algorithm. |
| `backend` | String | **Yes** | `"memory"` or `"redis"` | `"memory"` (single-node) / `"redis"` (distributed) | State store backend for bucket tracking. |
| `rate_per_second` | Integer (u64) | **Yes** | `1 ..= 10000000` | `1000` | Sustained request rate allowed per second per client key. |
| `burst_capacity` | Integer (u64) | **Yes** | `1 ..= 10000000` ($\ge \text{rate\_per\_second}$) | `2000` | Maximum burst bucket capacity for absorbing sudden traffic spikes. |
| `key_strategy` | String | **Yes** | `"client_ip"`, `"header"`, `"route"`, or `"priority"` | `"priority"` | Strategy used to extract the client identification key. |
| `key_headers` | Array of strings | **Yes** | Non-empty array of valid header names (or `["client_ip"]`) | `["x-api-key", "client_ip"]` | Header names evaluated in order to extract client identity. |
| `fallback_behavior` | String | **Yes** | `"memory"`, `"allow"`, or `"deny"` | `"memory"` | Failover strategy if distributed Redis synchronization becomes unavailable. |

---

## 3. Headers Contract

### Request Headers Inspected
- Fields listed in `key_headers` (e.g. `x-api-key`, `authorization`).
- `"client_ip"` extracts the downstream peer IP from socket or `x-forwarded-for`.

### Response Headers Injected on Limit Exceeded (Virtual 3-Column Tagging)
When rate limit quota is exhausted, `RateLimitPlugin` tags the following headers as **Bit 1 (Bypass on Reject)** via `req.headers.insert_bypass(...)` and returns `Action::Reject(RejectReason::TooManyRequests)`:
- `Retry-After`: Estimated cooldown period in seconds until capacity becomes available.
- `x-ratelimit-reset`: Estimated cooldown period in seconds.
- `x-ratelimit-limit`: Configured `rate_per_second`.
- `x-ratelimit-remaining`: `0`.
- `x-request-id`: Automatically retained from pre-loaded Slot 0 Bit 1 Bypass headers for end-to-end trace correlation.

The downstream ingress engine converts `RejectReason::TooManyRequests` to:
- **HTTP/1.1, HTTP/2, HTTP/3**: `429 Too Many Requests` early error response containing the Bit 1 bypass headers (including `x-request-id`).
- **gRPC**: `RESOURCE_EXHAUSTED` status trailer containing the Bit 1 bypass metadata.

All internal Bit 0 request headers are dropped, preventing header reflection or leak of sensitive routing tags.

---

## 4. Configuration Examples

### In-Memory Token Bucket Manifest (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "rate_limit",
      "config": {
        "algorithm": "token_bucket",
        "backend": "memory",
        "rate_per_second": 1000,
        "burst_capacity": 2000,
        "key_strategy": "priority",
        "key_headers": [
          "x-api-key",
          "client_ip"
        ],
        "fallback_behavior": "memory"
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
    "ip_filter",
    "rate_limit",
    "jwt_auth"
  ]
}
```
