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
  - **Negative Cache**: Temporarily records rejected or brute-forced key hashes (10s TTL), failing fast on repeated botnet probes in ~10ns.
- **Lock-Striped Sharding**: 64 independent mutex-guarded shards to eliminate lock contention across traffic worker threads.
- **Consumer Identity & Metadata Injection**: Automatically injects pre-compiled consumer metadata (`x-consumer-id`, `x-consumer-tier`) into upstream request headers.
- **Credential Hygiene (`hide_credentials`)**: Strips client credentials (`X-API-Key`) prior to forwarding to upstream backend services.

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All extraction sources, key entries, and cache settings must be declared intentionally.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `sources` | Array of objects | **Yes** | Non-empty array of source descriptors (`header` or `query`) | `[{"type":"header","name":"x-api-key","prefix":null}]` | Key extraction locations evaluated in sequential FIFO order. |
| `keys` | Array of objects | **Yes** | Non-empty array of key definitions | `[{"key":"...","consumer_id":"...","inject_headers":{}}]` | Configured active API keys, consumer identities, and metadata to inject. |
| `hide_credentials` | Boolean | **Yes** | `true` or `false` | `true` | When true, strips matching API key headers before forwarding upstream. |
| `cache_ttl_secs` | Integer (u64) | **Yes** | `1 ..= 86400` seconds | `300` (5 minutes) | Positive authentication cache lifetime in seconds. |

### Source Object Format
- **Header Source**:
  - `type`: `"header"`
  - `name`: Header name string (e.g. `"x-api-key"`, `"authorization"`)
  - `prefix`: Prefix string to strip (e.g. `"ApiKey "`, `"Bearer "`), or `null` if none
- **Query Source**:
  - `type`: `"query"`
  - `param`: Query parameter name string (e.g. `"api_key"`, `"token"`)

### Key Entry Format
- `key`: Non-empty secret string token
- `consumer_id`: Consumer identifier string, or `null` to use `key`
- `inject_headers`: Key-value map of headers to inject upstream upon successful authentication (`{}` if none)

---

## 3. Headers Contract

### Request Headers / Query Inspected
- Configured header sources (e.g. `X-API-Key`, `Authorization: ApiKey <key>`).
- Configured query parameters (e.g. `?api_key=<key>`).

### Response Generated on Violation (`401 Unauthorized`)
- Status: `401 Unauthorized`.
- Header: `WWW-Authenticate: ApiKey realm="velda-edge"`.
- Header: `Content-Type: application/json`.
- Body: `{"error":"Missing API key"}` or `{"error":"Invalid API key"}`.

### Request Headers Injected Upstream (on Success)
- All key-value pairs defined in `inject_headers` (e.g. `x-consumer-id: partner-service`).

---

## 4. Configuration Examples

### Production Manifest (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "api_key_auth",
      "config": {
        "sources": [
          {
            "type": "header",
            "name": "x-api-key",
            "prefix": null
          },
          {
            "type": "header",
            "name": "authorization",
            "prefix": "ApiKey "
          },
          {
            "type": "query",
            "param": "token"
          }
        ],
        "keys": [
          {
            "key": "sk-live-alpha-998877665544",
            "consumer_id": "stripe-webhook",
            "inject_headers": {
              "x-consumer-id": "stripe-webhook",
              "x-consumer-tier": "enterprise"
            }
          }
        ],
        "hide_credentials": true,
        "cache_ttl_secs": 300
      }
    }
  ]
}
```

### Route Attachment (`routes.json`)

```json
{
  "id": "api-webhooks-route",
  "match": {
    "path": "/webhooks/stripe"
  },
  "upstream_id": "webhooks-backend",
  "plugins": [
    "api_key_auth"
  ]
}
```
