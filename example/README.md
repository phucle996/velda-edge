# Velda Edge — Configuration Specification & Examples

This directory contains the canonical, standardized configuration specifications and production examples for **Velda Edge**.

---

## 1. Architectural Model & Domain Invariants

Velda Edge configuration is structured around the **Hourglass Architecture**:

```
[ Ingress Top Cone: listeners.json ]
         │ (pre_route plugins: ip_filter, waf)
         ▼
[ In-Memory Waist: routes.json ]
         │ (pre_upstream plugins: jwt_auth, rate_limit)
         ▼
[ Upstream Bottom Cone: upstreams.json ]
         │ (select_endpoint, connection pooling, L4/L7 forward)
         ▼
[ Post-Response Hook Chain ]
         │ (post_response plugins: security_headers, cors, access_log)
         ▼
[ Client Response Encoding ]
```

### Domain Separation & Execution Phases

| Domain | Configuration File | Specification Document | Responsibility & Lifecycle |
| :--- | :--- | :--- | :--- |
| **Ingress Listeners** | [`listeners.json`](./listeners.json) | [`listeners.md`](./listeners.md) | Network sockets, L4 transport, L7 protocol family, TLS termination, and `pre_route` edge guards. |
| **Routing** | [`routes.json`](./routes.json) | [`routes.md`](./routes.md) | Path/Host matching trie, upstream target binding, `pre_upstream` request hooks, and `post_response` response hooks. |
| **Upstream Topologies** | [`upstreams.json`](./upstreams.json) | [`upstreams.md`](./upstreams.md) | Physical endpoint pools, DNS discovery, load balancing algorithms, health checks, and connection pooling. |
| **Plugin Catalog** | [`plugins.json`](./plugins.json) | [`plugins.md`](./plugins.md) | Catalog and parameter configurations for reusable plugins (`jwt_auth`, `rate_limit`, `ip_filter`, etc.). |
| **TLS Certificates** | [`tls.json`](./tls.json) | [`tls.md`](./tls.md) | Edge TLS certificates, private keys, client CA (mTLS), and ALPN protocols. |
| **Runtime Profile** | [`runtime.json`](./runtime.json) | [`runtime.md`](./runtime.md) | OS kernel tuning, TCP buffer sizes, DNS cache sizing, and overload shedding thresholds. |
| **Deployment Manifest** | [`manifest.json`](./manifest.json) | [`manifest.md`](./manifest.md) | Atomic synchronization manifest, schema versions, and domain file checksums. |

---

## 2. Core Configuration Rules

1. **Sequential Declaration Order (No Magic Priorities)**:
   Plugins are executed strictly in the order they are declared in the JSON array (`index 0` runs first, `index 1` runs second). The legacy `priority` numeric ranking has been eliminated to ensure predictable, linear runtime execution.
2. **Explicit Phase Scoping**:
   - `pre_route` plugins run on incoming requests **before** route resolution (configured on Listeners in [`listeners.json`](./listeners.json)).
   - `pre_upstream` plugins run on routed requests **before** upstream dispatch (configured on Routes in [`routes.json`](./routes.json)).
   - `post_response` plugins run on upstream responses **before** downstream client transmission (configured on Routes in [`routes.json`](./routes.json)).
3. **In-Memory Zero-IO Serving**:
   All JSON files in this directory are compiled into self-describing, checksummed binary artifacts (`*.bin`) by `velda-sync` during deployment. The Data Plane request-serving hot path operates exclusively from in-memory compiled structs with **zero disk I/O, zero JSON parsing, and zero RPC overhead**.
