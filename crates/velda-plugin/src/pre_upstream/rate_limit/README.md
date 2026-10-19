# Rate Limit Plugin (`rate_limit`)

High-throughput request pacing and traffic shaping engine with multi-algorithm support, dual-backend architecture (in-memory lock-striped concurrent state + distributed Redis synchronization via auxiliary background workers), flexible key resolution strategies, and resilient failover behaviors.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `rate_limit` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Evaluated after routing and endpoint selection, immediately prior to upstream connection leasing |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (Can generate `Action::Respond` 429) |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Hot-Path Overhead** | ~15ns local in-memory lock-striped concurrency; **Zero Network I/O** on traffic worker cores |

### What it Protects
Protects upstream backend services, connection pools, and edge proxy hardware against DDoS amplification, brute-force credential stuffing, API scraping, and upstream resource saturation.

### Out of Scope
- **IP Subnet CIDR Filtering**: Owned by `ip_filter` in `pre_route`.
- **L7 Path / Host Routing**: Owned by `velda-router`.
- **Cryptographic TLS Handshakes**: Owned by `velda-tls`.

---

## 2. Configuration Field Specification

All unused or optional string/TLS fields must use the explicit `"none"` sentinel value when not configured.

| Field | Type | Required / Default | Constraints / Allowed Values | Description |
| :--- | :--- | :--- | :--- | :--- |
| `algorithm` | string | Optional (`"token_bucket"`) | `"token_bucket"`, `"sliding_window"`, `"fixed_window"` | Traffic shaping algorithm |
| `backend` | string | Optional (`"memory"`) | `"memory"`, `"redis"` | State store backend |
| `rate_per_second` | integer | **Required** | `> 0` | Sustained request rate allowed per second |
| `burst_capacity` | integer | **Required** | `>= rate_per_second` | Maximum burst bucket capacity |
| `key_strategy` | string | Optional (`"priority"`) | `"priority"`, `"composite"` | Client identification key strategy |
| `key_headers` | array[string] | **Required** | Non-empty array of headers or `"client_ip"` | Header resolution order or composite key fields |
| `fallback_behavior` | string | Optional (`"memory"`) | `"memory"`, `"allow"`, `"deny"` | Failover strategy when Redis is offline |
| `batch_sync_interval_ms` | integer | Optional (`50`) | `> 0` (milliseconds) | Interval for auxiliary Redis batch flushes |
| `batch_max_size` | integer | Optional (`1000`) | `> 0` | Maximum count of increments batched per flush |
| `redis_url` | string | Optional (`"none"`) | `"redis://host:port"` or `"none"` | Redis cluster/server connection URI |
| `redis_username` | string | Optional (`"none"`) | String or `"none"` | ACL username for Redis 6+ |
| `redis_password` | string | Optional (`"none"`) | String or `"none"` | Redis authentication secret |
| `redis_tls_ca` | string | Optional (`"none"`) | PEM string, file path, or `"none"` | Root CA certificate for Redis mTLS |
| `redis_tls_cert` | string | Optional (`"none"`) | PEM string, file path, or `"none"` | Client certificate for Redis mTLS |
| `redis_tls_key` | string | Optional (`"none"`) | PEM string, file path, or `"none"` | Client private key for Redis mTLS |
| `redis_timeout_ms` | integer / string | Optional (`2000`) | Milliseconds or `"none"` | TCP connection and command timeout |

---

## 3. Headers Contract

### Request Headers Inspected
- Fields listed in `key_headers` (e.g., `x-device-id`, `x-api-key`, `authorization`).
- `"client_ip"` extracts the downstream peer address (or `x-forwarded-for` / `x-real-ip`).

### Response Headers Injected on Rate Exceeded (`429 Too Many Requests`)
- `Retry-After`: Estimated cooldown period in seconds until capacity becomes available.
- `x-ratelimit-limit`: Configured `burst_capacity`.
- `x-ratelimit-remaining`: `0`.

---

## 4. Configuration Examples

### Minimal In-Memory Configuration
```json
{
  "id": "rate_limit",
  "config": {
    "rate_per_second": 100,
    "burst_capacity": 200,
    "key_headers": ["client_ip"]
  }
}
```

### Full Distributed Redis Configuration with Explicit Sentinels
```json
{
  "id": "rate_limit",
  "config": {
    "algorithm": "token_bucket",
    "backend": "redis",
    "rate_per_second": 500,
    "burst_capacity": 1000,
    "key_strategy": "priority",
    "key_headers": [
      "x-device-id",
      "x-api-key",
      "client_ip"
    ],
    "fallback_behavior": "memory",
    "batch_sync_interval_ms": 50,
    "batch_max_size": 1000,
    "redis_url": "redis://127.0.0.1:6379",
    "redis_username": "none",
    "redis_password": "none",
    "redis_tls_ca": "none",
    "redis_tls_cert": "none",
    "redis_tls_key": "none",
    "redis_timeout_ms": 2000
  }
}
```

---

## 5. Architectural Invariants & Failover

1. **Hot-Path Isolation**: `on_upstream_request` runs entirely against in-memory lock-striped shards (64 shards using fast FxHash). Hot-path execution takes ~15ns and makes **strictly zero network syscalls**.
2. **Auxiliary Batch Worker**: Redis increments are dispatched across an asynchronous unbounded channel and flushed in multi-command pipelines on auxiliary background supervisor threads.
3. **Resilient Failover**: If the Redis connection is interrupted, the plugin seamlessly switches behavior according to `fallback_behavior`:
   - `"memory"`: Falls back to local in-memory lock-striped rate limiting.
   - `"allow"`: Bypasses rate limiting during the outage to prioritize service availability.
   - `"deny"`: Rejects traffic with 429 to enforce strict backend protection.
