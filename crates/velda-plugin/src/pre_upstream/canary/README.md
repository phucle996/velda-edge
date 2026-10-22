# Canary Traffic Splitting Plugin (`canary`)

Production-grade traffic splitting and progressive delivery plugin evaluated during the `pre_upstream` phase. Enables safe, controlled rollouts between baseline and canary upstreams through weight-based splitting, consistent session hashing, and conditional header rules.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `canary` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Strictly FIFO based on route `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Hot-Path Overhead** | Nanosecond-level ($< 15\ \text{ns}$); zero heap allocation, zero disk/network I/O |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `canary_upstream` | String | **Yes** | Non-empty string identifier of target canary upstream | `"orders-service-v2"` | Identifier of the target canary upstream pool. |
| `weight` | Integer (u32) | **Yes** | `0 ..= 100` (%) | `10` (when rolling out) / `0` (rules only) | Percentage of traffic directed to the canary upstream. |
| `rules` | Array of objects | **Yes** | List of rule objects (`[]` if none) | `[{"name":"x-canary","operator":"equals","value":"always"}]` | Header matching rules that immediately force routing to canary. |
| `rules[].name` | String | **Yes** | Valid RFC 9110 header name | `"x-canary"` | Header name to inspect (case-insensitive). |
| `rules[].operator` | String | **Yes** | `"equals"`, `"prefix"`, `"regex"`, or `"exists"` | `"equals"` | Matching operator. |
| `rules[].value` | String | **Yes** | Comparison string (or `""` if operator is `"exists"`) | `"always"` | Value to match against. |
| `hash_by` | String | **Yes** | `"random"`, `"client_ip"`, `"header"`, or `"cookie"` | `"client_ip"` | Consistent session hashing attribute for stickiness. |
| `canary_header` | String | **Yes** | Valid RFC 9110 header name | `"x-velda-canary"` | Informational header injected for upstream backend visibility. |
| `canary_header_value` | String | **Yes** | Non-empty string | `"true"` | Value of the informational marker header. |

---

## 3. Configuration Examples

### Progressive Rollout Manifest (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "canary",
      "config": {
        "canary_upstream": "orders-service-v2",
        "weight": 10,
        "rules": [
          {
            "name": "x-canary",
            "operator": "equals",
            "value": "always"
          },
          {
            "name": "x-user-role",
            "operator": "equals",
            "value": "beta_tester"
          }
        ],
        "hash_by": "client_ip",
        "canary_header": "x-velda-canary",
        "canary_header_value": "true"
      }
    }
  ]
}
```

### Route Attachment (`routes.json`)

```json
{
  "id": "checkout-route",
  "match": {
    "path": "/api/v1/checkout"
  },
  "upstream_id": "checkout-baseline",
  "plugins": [
    "jwt_auth",
    "canary",
    "access_log"
  ]
}
```
