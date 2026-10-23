# Canary Traffic Splitting Plugin (`canary`)

Production-grade traffic splitting and progressive delivery plugin evaluated during the `pre_upstream` phase. Enables safe, controlled rollouts between baseline and canary upstreams through weight-based splitting, multi-source consistent session stickiness, and byte-level conditional header rules.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `canary` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Strictly FIFO based on route `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Hot-Path Overhead** | Nanosecond-level ($< 10\ \text{ns}$); zero heap allocation, zero DFA/NFA regex, zero disk/network I/O |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or hardcoded default headers exist (e.g. no implicit `x-user-id` or `session_id`). All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `canary_upstream` | String | **Yes** | Non-empty string identifier of target canary upstream pool | `"orders-service-v2"` | Target canary upstream backend pool. |
| `weight` | Integer (u32) | **Yes** | `0 ..= 100` (%) | `10` (when rolling out) / `0` (rules only) | Percentage of traffic directed to the canary upstream. |
| `rules` | Array of objects | **Yes** | List of rule objects (`[]` if none) | `[{"name":"x-canary","operator":"equals","value":"always"}]` | Header matching rules that immediately force routing to canary. |
| `rules[].name` | String | **Yes** | Valid RFC 9110 header name | `"x-canary"` | Header name to inspect (case-insensitive). |
| `rules[].operator` | String | **Yes** | `"equals"`, `"prefix"`, `"suffix"`, `"contains"`, or `"exists"` | `"equals"` | Matching operator evaluated directly on raw byte slices. |
| `rules[].value` | String | **Yes** | Comparison string (or `""` if operator is `"exists"`) | `"always"` | Value to match against. |
| `hash_strategy` | String | **Yes** | `"priority"`, `"composite"`, or `"random"` | `"priority"` | Session hashing strategy. |
| `hash_sources` | Array of strings | **Yes** (when `hash_strategy` is `priority` or `composite`) | List of source tokens: `header:<name>`, `cookie:<name>`, `query:<name>`, `client_ip`, `uri_path` | `["header:x-user-id", "cookie:session_id", "client_ip"]` | User-defined sources evaluated for consistent session stickiness. Bare `header`, `cookie`, `query` without names are strictly forbidden. |
| `canary_header` | String | **Yes** | Valid RFC 9110 header name, or `"none"` | `"x-velda-canary"` | Informational marker header injected for backend visibility. When request goes to baseline, any client-supplied header with this name is strictly stripped to prevent spoofing. |
| `canary_header_value` | String | **Yes** | Non-empty printable ASCII string, or `"none"` | `"true"` | Value of the informational marker header. |

---

## 3. Security & Anti-Spoofing Guarantees

1. **Canary Marker Sanitization**:
   - When traffic is routed to **Canary**: `canary_header: canary_header_value` is injected into upstream request headers.
   - When traffic is routed to **Baseline**: any incoming `canary_header` sent by the client is **strictly removed**, preventing external clients from spoofing canary feature flags or telemetry on production baseline nodes.
2. **Authoritative Client IP Extraction**:
   - `HashSource::ClientIp` prioritizes `x-real-ip` (overwritten by Velda Edge downstream server from verified socket peer) or the rightmost entry in `x-forwarded-for`, preventing client-controlled XFF leftmost spoofing.
3. **Lock-Free Pseudo-Random Distribution**:
   - Uniform distribution is calculated via thread-local Xorshift64* PRNG, completely eliminating atomic cross-core MESI cache-line invalidation.

---

## 4. Configuration Examples

### Multi-Source Priority Fallback Rollout (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "canary",
      "config": {
        "canary_upstream": "orders-service-v2",
        "weight": 20,
        "rules": [
          {
            "name": "x-canary",
            "operator": "equals",
            "value": "always"
          },
          {
            "name": "x-user-email",
            "operator": "suffix",
            "value": "@velda.io"
          }
        ],
        "hash_strategy": "priority",
        "hash_sources": [
          "header:x-user-id",
          "cookie:session_token",
          "query:tenant_id",
          "client_ip"
        ],
        "canary_header": "x-velda-canary",
        "canary_header_value": "true"
      }
    }
  ]
}
```

### Composite Multi-Dimensional Rollout (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "canary",
      "config": {
        "canary_upstream": "orders-service-v2",
        "weight": 50,
        "rules": [],
        "hash_strategy": "composite",
        "hash_sources": [
          "header:x-tenant-id",
          "client_ip"
        ],
        "canary_header": "none",
        "canary_header_value": "none"
      }
    }
  ]
}
```
