# URL Rewrite Plugin (`url_rewrite`)

Production-grade, zero-allocation URI path and query string transformation plugin evaluated during the `pre_upstream` execution phase. Adapts public edge routing contracts into internal microservice endpoint structures with multi-level optimization and RFC 3986 path traversal protection.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `url_rewrite` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Strictly FIFO based on route `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::new(AccessLevel::Write, AccessLevel::None)` |
| **Payload Passthrough** | **100% Zero-Copy streaming**. Request and response bodies are never buffered |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Single Responsibility** | Operates strictly on URI (`path` and `query`). `Host` / `:authority` header mutation belongs to [`header_mutation`](../header_mutation/) |

---

## 2. Core Architecture & Invariants

```
               [ Downstream Client ]
                         │
                         ▼ (Public URI: GET /api/v1/orders/42?limit=10)
                 [ Router Match ] (Matches Route "orders-v1")
                         │
                         ▼
        [ pre_upstream: url_rewrite ]
        ├─ 1. Path Strategy (StripPrefix: "/api/v1" -> "/orders/42")
        ├─ 2. Query Strategy (Keep / Drop / Append)
        ├─ 3. RFC 3986 Sanitization (Collapse //, block /../ traversal)
        └─ 4. Hot-Path Fast-Bypass (0 alloc if unchanged)
                         │
                         ▼ (Upstream URI: GET /orders/42?limit=10)
               [ Upstream Microservice ]
```

### 2.1. Multi-Level Hot-Path Optimization Hierarchy
To eliminate Regex bottleneck risks and achieve nanosecond-scale processing on the serving path:

1. **Level 1: Specialized Non-Regex Fast-Paths (90% Production Traffic)**:
   - `strip_prefix`: Slices path via direct pointer offsets (`&path[prefix.len()..]`).
   - `replace_prefix`: Replaces matched prefix using pre-sized string concatenation.
   - `exact`: Performs $O(N)$ string equality and swaps to a pre-allocated static replacement.
   - **Cost**: $< 5\ \text{ns}$, 0 regex state machine invocations.
2. **Level 2: Pre-Tokenized Regex & Static Prefix Fast-Reject**:
   - Leading static literals (e.g. `^/service/([a-z]+)`) are extracted at config compile time. Non-matching paths are rejected via SIMD/byte comparison in $\sim 2\ \text{ns}$ before DFA evaluation.
   - Replacement templates (`$1`, `$2`) are pre-tokenized into `Vec<ReplacementToken>` at `instantiate` time. On hot path, capture slices are copied directly into a single pre-calculated `String::with_capacity` buffer without re-parsing `$` markers.
   - Built with deterministic Thompson DFA automata ($O(M \cdot N)$ linear time), preventing Catastrophic Backtracking and ReDoS vulnerabilities.
3. **Level 3: Zero-Change Bypass**:
   - If neither the path nor query string requires mutation, the plugin short-circuits immediately without cloning `req.uri`.

### 2.2. RFC 3986 Path Sanitization & Traversal Defense
When client paths contain redundant slashes or directory traversal markers (e.g. `/api/v1/../../etc/passwd`):
- Double slashes (`//`) are collapsed to single slashes (`/`).
- Relative directory markers (`/./`) are stripped.
- Path traversal (`/../`) is resolved hierarchically. If traversal attempts to climb above root `/`, the plugin **fails fast with `400 Bad Request`**, blocking the request at the edge before upstream dispatch.
- **Fast-Path**: If the path contains neither `//` nor `/.`, normalization is completely bypassed.

### 2.3. Query String Handling Policies
- `keep` (default): Preserves the client query string intact.
- `drop`: Completely strips query parameters before forwarding.
- `append`: Appends static query parameters to existing queries (e.g. `gateway=velda&cluster=prod`).

---

## 3. Configuration Schema & Examples

### Example 1: Strip Public Prefix (`strip_prefix`)
Strip `/api/v1` prefix so upstream microservice receives `/users/42`:

```json
{
  "id": "url_rewrite",
  "config": {
    "path": {
      "type": "strip_prefix",
      "prefix": "/api/v1"
    },
    "query": {
      "action": "keep"
    },
    "sanitize_path": true
  }
}
```

### Example 2: Replace Prefix (`replace_prefix`)
Translate legacy `/v1/payments` prefix to `/internal/v2/payments`:

```json
{
  "id": "url_rewrite",
  "config": {
    "path": {
      "type": "replace_prefix",
      "prefix": "/v1/payments",
      "replacement": "/internal/v2/payments"
    },
    "query": {
      "action": "keep"
    }
  }
}
```

### Example 3: Exact Path Rewrite (`exact`)
Map public legacy health check directly to Spring Actuator:

```json
{
  "id": "url_rewrite",
  "config": {
    "path": {
      "type": "exact",
      "path": "/health",
      "replacement": "/actuator/healthz"
    }
  }
}
```

### Example 4: Regex Rewrite with Capture Groups (`regex`)
Reorder path parameters into upstream query strings or nested paths:

```json
{
  "id": "url_rewrite",
  "config": {
    "path": {
      "type": "regex",
      "pattern": "^/users/([0-9]+)/orders/([0-9]+)$",
      "replacement": "/orders/$2/user/$1"
    },
    "query": {
      "action": "append",
      "query": "source=edge_gateway"
    },
    "sanitize_path": true
  }
}
```

---

## 4. Verification & Testing

Run the dedicated test suite verifying all path strategies, query policies, and traversal defenses:

```bash
rtk cargo test -p velda-plugin --lib pre_upstream::url_rewrite
```
