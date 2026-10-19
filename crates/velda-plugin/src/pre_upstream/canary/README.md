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

## 2. Core Architecture & Mechanisms

### 2.1. Why Canary Deployment?
Deploying new microservice versions all-at-once (Big-Bang Deployment) poses severe availability risks: memory leaks, deadlocks, or unindexed database queries can instantly impact 100% of users. 

The `canary` plugin routes a small, controlled fraction of live traffic (e.g., 5% or 10%) to the new version (`canary_upstream`). If telemetry metrics remain healthy, operators can progressively increase the weight to 25%, 50%, and eventually 100%.

```
                             Downstream Client
                                     │
                                     ▼
                    ┌───────────────────────────────────┐
                    │  1. Rule-Based Overrides?         │
                    │     (e.g., X-User-Role: "beta")   ├──► Sang Canary (100%)
                    └────────────────┬──────────────────┘
                                     │ (Không khớp rule)
                                     ▼
                    ┌───────────────────────────────────┐
                    │  2. Consistent Hashing?           │
                    │     Hash(User-Id / Cookie / IP)   │
                    └────────────────┬──────────────────┘
                                     │
                                     ▼
                      [ Hash % 100 < Weight ? ]
                           ├─── True  ──► [ Upstream V2 (Canary) ]
                           └─── False ──► [ Upstream V1 (Baseline) ]
```

---

### 2.2. The 3 Routing Mechanisms

#### 1. Rule-Based Overrides (Highest Priority)
- Used for internal testing and dogfooding before exposing canary to the public (`weight: 0`).
- Matches exact values or prefixes on request headers (e.g., `x-canary: always` or `x-user-role: beta`).
- Matching requests bypass weight calculations and route directly to the canary upstream.

#### 2. Consistent Session Hashing (Stickiness)
- **The Split-Session Problem**: In multi-step workflows (such as e-commerce checkout or banking transactions), purely random routing risks routing step 1 to v1, step 2 to v2, and step 3 to v1, causing lost session state and transaction failures.
- **The Solution**: Consistent hashing using `FxHasher` over a stable client identifier:
  - `Header`: Hashes authenticated user ID (e.g., `x-user-id`, `x-account-id`).
  - `Cookie`: Hashes session token (e.g., `session_id`, `user_token`).
  - `ClientIp`: Hashes the downstream client IP address.
- **Invariant**: The same user or session **always maps to the exact same version** across multiple requests and devices.

#### 3. Weight-Based Splitting
- Controls the percentage of non-rule traffic steered to the canary upstream (`0` to `100`).
- Evaluated via `hash(key) % 100 < weight`. When `hash_by` is set to `random`, requests are uniformly distributed using an atomic counter.

---

### 2.3. Cooperation with `jwt_auth` (Account-Based Canary)

By following Velda Edge's **Linear FIFO** ordering, `jwt_auth` and `canary` cooperate seamlessly:

1. **Step 1 (`jwt_auth` runs first)**:
   - Validates the incoming JWT token signature.
   - Extracts claims (`sub` as user ID, `role`, `org`) and injects `X-User-Id: 98765` into the request headers.
2. **Step 2 (`canary` runs second)**:
   - Reads `X-User-Id` to perform consistent hashing.
   - Regardless of whether the user switches between 4G and Wi-Fi, their `user_id` remains constant, ensuring uninterrupted session stickiness.

---

### 2.4. Zero-Downtime Transition Lifecycle

When canary validation completes and operators decide to promote v2 to production:
1. Update the route's default `upstream_id` to `orders-v2`.
2. Remove the `canary` plugin from the route's `plugins` list.

**Why this transition has ZERO stutter (Zero Downtime / Zero Latency Spike)**:
- **Lock-Free Atomic Pointer Swap**: Velda Edge compiles the candidate runtime snapshot in the background. The active snapshot is replaced via `ArcSwap::store` in ~2 nanoseconds without restarting processes or workers.
- **Pre-warmed Connection Pool**: Because `orders-v2` was already serving canary traffic, its TCP sockets, TLS handshakes, and HTTP/2 multiplexed streams are already active and warm in the connection pool. No cold-start TLS handshake storms occur.
- **In-flight Safety**: Existing requests finish on the previous snapshot safely; new requests immediately use the new route.

---

### 2.5. Informational Marker Header (`X-Velda-Canary`)

When a request is routed to the canary upstream, the plugin injects an informational header (default: `X-Velda-Canary: true`):
- **Backend Log Tagging**: Microservices can tag access and application logs with canary status for targeted monitoring in Loki/Elasticsearch.
- **Dark Launching & Sandbox Mode**: Upstream services can inspect this header to disable real SMS dispatching or use sandbox payment gateways during testing.

---

## 3. Configuration Schema

As with all Velda Edge plugins, **never specify `phase` or `priority`** in user configuration. Execution phase is statically inferred as `pre_upstream`, and execution order follows the route's array order (FIFO).

```json
{
  "id": "canary",
  "config": {
    "canary_upstream": "orders-service-v2",
    "weight": 10,
    "rules": [
      {
        "header": "x-canary",
        "exact": "always"
      },
      {
        "header": "x-user-role",
        "exact": "beta_tester"
      }
    ],
    "hash_by": {
      "source": "header",
      "key": "x-user-id"
    },
    "canary_header": "x-velda-canary",
    "canary_header_value": "true"
  }
}
```

### Configuration Parameters

| Parameter | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `canary_upstream` | string | **Required** | Identifier of the target canary upstream pool. |
| `weight` | integer | `0` | Percentage of traffic directed to canary (`0` to `100`). |
| `rules` | object[] | `[]` | List of header-matching rules that force routing to canary. |
| `rules[].header` | string | **Required** | Header name to inspect (case-insensitive). |
| `rules[].exact` | string | `null` | Exact case-insensitive match value. |
| `rules[].prefix` | string | `null` | Prefix match value. |
| `rules[].exists` | boolean | `false` | Matches if the header is present, regardless of value. |
| `hash_by` | object \| string | `{"source": "random"}` | Consistent hashing key strategy (`"random"`, `"ip"`, `"header"`, `"cookie"`). |
| `hash_by.source` | string | `"random"` | Hashing source: `"random"`, `"client_ip"`, `"header"`, or `"cookie"`. |
| `hash_by.key` | string | `null` | Name of the header or cookie to hash. |
| `canary_header` | string | `"x-velda-canary"` | Informational header injected for upstream backend visibility. |
| `canary_header_value` | string | `"true"` | Value of the informational marker header. |

---

## 4. Route Attachment Examples

### Example 1: Progressive Delivery with Consistent User Stickiness
```json
{
  "id": "checkout-api-route",
  "path": "/api/v1/checkout",
  "upstream_id": "checkout-baseline",
  "plugins": [
    {
      "id": "jwt_auth",
      "config": { "secret": "auth-jwt-secret" }
    },
    {
      "id": "canary",
      "config": {
        "canary_upstream": "checkout-canary",
        "weight": 10,
        "hash_by": {
          "source": "header",
          "key": "x-user-id"
        }
      }
    },
    { "id": "access_log" }
  ]
}
```

### Example 2: Internal Dogfooding (0% Public Traffic, Beta Tester Rule)
```json
{
  "id": "billing-api-route",
  "path": "/api/v2/billing",
  "upstream_id": "billing-v1",
  "plugins": [
    {
      "id": "canary",
      "config": {
        "canary_upstream": "billing-v2",
        "weight": 0,
        "rules": [
          { "header": "x-canary", "exact": "always" },
          { "header": "x-internal-team", "prefix": "fintech-" }
        ]
      }
    }
  ]
}
```
