# API Key Authentication (`api_key_auth`) — Operations Runbook

## 1. Parameter Reference

| Field | Type | Default | Constraints | Description |
| :--- | :--- | :---: | :--- | :--- |
| `header_name` | `String` | **Required** | RFC 9110 token | Request header on `HeaderWorkspace` (applies identically to HTTP and gRPC). |
| `hide_credentials` | `bool` | `false` | — | Removes `header_name` before forwarding upstream. |
| `cache_ttl_secs` | `u64` | `300` | `1..=86400` | Positive per-core cache TTL in seconds (`coarse_sec()`). |
| `cache_slots_per_core` | `usize` | `256` | `256..=65536` | Positive cache slots per core (`16B/slot`), rounded up to power-of-2 (`256` = `4 KB/core`). |
| `keys` | `Vec<ApiKeyEntryConfig>` | **Required** | Non-empty | List of valid API keys. |
| `keys[].key` | `String` | **Required** | `1..=64` bytes | Secret API key token. |
| `keys[].expires_at` | `u64` | `0` | Unix epoch sec | Expiration timestamp (`0` = never expires). |
| `keys[].inject_headers` | `Map<String, String>` | `{}` | Valid RFC 9110 | Headers written (overwriting client headers) on match. |

---

## 2. Cache Sizing Guide (`k * 4096` Bytes / Core)

Each cache slot occupies **`16` bytes**. Total per-core memory is $\text{cache\_slots\_per\_core} \times 16$ bytes (always an exact multiple of `4,096` bytes):

| `cache_slots_per_core` | Memory / Core | Total for 8 Cores | Recommended Workload |
| :---: | :---: | :---: | :--- |
| `256` *(default)* | `4 KB` | `32 KB` | Standard B2B APIs (`<= 200` active keys) |
| `1024` | `16 KB` | `128 KB` | Medium SaaS (`200–1,000` active keys) |
| `4096` | `64 KB` | `512 KB` | High-cardinality multi-tenant (`1,000–10,000+` keys) |

---

## 3. Scenario-Based Multi-Key Recipes

### Recipe A: Multi-Tenant B2B API (Mixed Permanent, Expiring & Internal Keys)
```json
{
  "id": "api_key_auth",
  "config": {
    "header_name": "x-api-key",
    "hide_credentials": true,
    "cache_ttl_secs": 300,
    "cache_slots_per_core": 1024,
    "keys": [
      {
        "key": "sk-live-acme-corp-99887766554433221100",
        "expires_at": 0,
        "inject_headers": {
          "x-tenant-id": "acme",
          "x-plan": "enterprise"
        }
      },
      {
        "key": "sk-trial-beta-partner-aabbccddeeff0011",
        "expires_at": 1798761600,
        "inject_headers": {
          "x-tenant-id": "beta-partner",
          "x-plan": "trial"
        }
      },
      {
        "key": "sk-svc-billing-worker-5544332211aabbcc",
        "expires_at": 0,
        "inject_headers": {
          "x-tenant-id": "internal-billing",
          "x-plan": "system"
        }
      }
    ]
  }
}
```

### Recipe B: gRPC Service-to-Service Authentication (`x-grpc-api-key`)
gRPC metadata arrives in `HeaderWorkspace` identically to HTTP headers, so the exact same plugin protects gRPC routes:
```json
{
  "id": "api_key_auth",
  "config": {
    "header_name": "x-grpc-api-key",
    "hide_credentials": true,
    "cache_ttl_secs": 600,
    "cache_slots_per_core": 256,
    "keys": [
      {
        "key": "grpc-order-svc-token-01",
        "inject_headers": { "x-caller-service": "order-service" }
      },
      {
        "key": "grpc-payment-svc-token-02",
        "inject_headers": { "x-caller-service": "payment-service" }
      }
    ]
  }
}
```

---

## 4. Observability Signals

| Signal | External Wire Output | Internal Semantic |
| :--- | :--- | :--- |
| **HTTP Reject** | `401 Unauthorized` (`www-authenticate: ApiKey realm="velda-edge"`) | `RejectReason::Unauthorized` |
| **gRPC Reject** | `:status: 200`, `grpc-status: 16 (UNAUTHENTICATED)` | `RejectReason::Unauthorized` |

---

## 5. Incident Triage Playbook

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| **Valid key suddenly returns 401** | Key `expires_at` passed `coarse_sec()`. | Update `expires_at` (or rotate key) in `plugins.json` and sync. |
| **Key rejected during config load** | Key exceeds `64` bytes (`MAX_API_KEY_LEN`), is empty, or is duplicated. | Ensure all keys are unique and within `1..=64` bytes. |
| **Client spoofs `x-tenant-id` header** | Upstream header was not listed in `inject_headers` for overwrite. | Add `"x-tenant-id"` to `inject_headers` on each valid key entry. |
