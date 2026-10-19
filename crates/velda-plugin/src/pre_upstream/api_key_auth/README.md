# API Key Authentication Plugin (`api_key_auth`)

High-performance L7 ingress authentication engine for B2B APIs, server-to-server microservice integrations, and webhooks. Backed by an in-memory hash table and a lock-striped Dual Cache (`Positive Cache` + `Negative Cache`) to neutralize credential-stuffing and brute-force DoS attacks with sub-20ns hot-path evaluation.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `api_key_auth` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Hot-Path Overhead** | ~10 – 15ns; atomic ref-count header cloning; **0 heap allocations** |

### What it Protects & Solves
- **Shields Upstream Backends**: Rejects missing or invalid API keys at the edge with `401 Unauthorized`, saving 100% of backend CPU cycles, database queries, and thread pool resources.
- **Dual Cache Defense (Positive + Negative Caching)**:
  - **Positive Cache**: Caches validated keys and pre-compiled header values for ~15ns instant downstream injection.
  - **Negative Cache**: Temporarily records rejected or brute-forced key hashes (default 10s TTL), failing fast on repeated botnet probes in ~10ns.
- **Lock-Striped Sharding**: 64 independent mutex-guarded shards using [`rustc_hash::FxHasher`] to eliminate lock contention across traffic worker threads.
- **Consumer Identity & Metadata Injection**: Automatically injects pre-compiled consumer metadata (`x-consumer-id`, `x-consumer-tier`) into upstream request headers.
- **Credential Hygiene (`hide_credentials`)**: Strips client credentials (`X-API-Key`) prior to forwarding to upstream backend services.

---

## 2. Configuration Field Specification

| Field | Type | Required / Default | Constraints / Allowed Values | Description |
| :--- | :--- | :--- | :--- | :--- |
| `sources` | array[object] | Optional | Non-empty array of source objects | Key extraction locations evaluated in FIFO order. Default: `X-API-Key` and `Authorization: ApiKey `. |
| `hide_credentials` | boolean | Optional (`false`) | `true`, `false` | When true, strips matching authentication headers before forwarding upstream. |
| `cache_ttl_secs` | integer | Optional (`300`) | `> 0` (seconds) | Positive cache expiration TTL. |
| `negative_cache_ttl_secs` | integer | Optional (`10`) | `> 0` (seconds) | Negative cache duration for invalid keys. |
| `cache_shards` | integer | Optional (`64`) | `> 0` | Lock-striped cache shards (rounded to power of 2). |
| `cache_shard_capacity` | integer | Optional (`2048`) | `> 0` | Max entries per shard before LRU eviction. |
| `keys` | array[object] | **Required** | Array of key definition objects | List of authorized API keys and their metadata. |

### Source Object Format
- **Header**: `{"type": "header", "name": "x-api-key", "prefix": ""}`
- **Query**: `{"type": "query", "param": "api_key"}`

### Key Definition Object Format
- `key`: Required non-empty string.
- `consumer_id`: Optional identifier string (defaults to `key`).
- `inject_headers`: Optional key-value object of HTTP headers to inject into the upstream request.

---

## 3. Headers Contract

### Request Headers / Query Inspected
- Configured header sources (e.g. `X-API-Key`, `Authorization: ApiKey <key>`).
- Configured query parameters (e.g. `?api_key=<key>`).

### Response Generated on Violation (`401 Unauthorized`)
- Status: `401 Unauthorized`.
- Header: `WWW-Authenticate: ApiKey realm="velda-edge"` (or `error="invalid_key"`).
- Header: `Content-Type: application/json`.
- Body: `{"error":"Missing API key"}` or `{"error":"Invalid API key"}`.

### Request Headers Injected Upstream (on Success)
- All key-value pairs defined in `inject_headers` (e.g. `x-consumer-id`, `x-consumer-tier`).

---

## 4. Configuration Examples

### Minimal Production Configuration
```json
{
  "id": "api_key_auth",
  "config": {
    "hide_credentials": true,
    "keys": [
      {
        "key": "sk-live-8f93b2a1c0d4e5f67890",
        "consumer_id": "stripe-webhook",
        "inject_headers": {
          "x-consumer-id": "stripe-webhook",
          "x-consumer-tier": "enterprise"
        }
      }
    ]
  }
}
```

### Advanced Multi-Source Configuration with Dual Cache Tuning
```json
{
  "id": "api_key_auth",
  "config": {
    "sources": [
      { "type": "header", "name": "x-api-key" },
      { "type": "header", "name": "authorization", "prefix": "ApiKey " },
      { "type": "query", "param": "token" }
    ],
    "hide_credentials": true,
    "cache_ttl_secs": 600,
    "negative_cache_ttl_secs": 15,
    "cache_shards": 64,
    "cache_shard_capacity": 4096,
    "keys": [
      {
        "key": "sk-proj-codex-11223344",
        "consumer_id": "codex-client-01",
        "inject_headers": {
          "x-consumer-id": "codex-client-01",
          "x-consumer-tier": "tier_pro_plus",
          "x-rate-limit-tier": "high"
        }
      },
      {
        "key": "sk-proj-sandbox-55667788",
        "consumer_id": "sandbox-developer",
        "inject_headers": {
          "x-consumer-id": "sandbox-developer",
          "x-consumer-tier": "tier_free"
        }
      }
    ]
  }
}
```

---

## 5. Architectural & Hot-Path Invariants

1. **Deterministic Phase & FIFO Order**: Automatically bound to `pre_upstream` via `canonical_phase_for_id`. Executes in exact declaration sequence relative to peer plugins (e.g., evaluated before `rate_limit` and after `cors_preflight`).
2. **Sub-20ns Dual Caching**: Negative cache checks execute in ~10ns; Positive cache hits execute in ~15ns.
3. **Zero Heap Allocation on Hot Path**: Pre-computes all `HeaderName` and `HeaderValue` instances at startup. Serving valid requests clones pre-constructed byte references without memory allocations.
