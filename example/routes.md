# `routes.json` — Routing & Execution Plan Specification

The `routes.json` file configures the razor-thin in-memory routing waist of Velda Edge. It maps incoming requests from listeners to logical upstreams, sets timeout thresholds, and specifies the phased plugin execution plan (`pre_upstream` and `post_response`).

---

## 1. Schema Definition & Top-Level Fields

```json
{
  "schema_version": 1,
  "routes": [ /* array of Route objects */ ]
}
```

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `schema_version` | `integer` | Yes | Schema version. Must be `1`. |
| `routes` | `array` | Yes | List of route configurations. Evaluated in order or via prefix trie. |

---

## 2. Route Object Specification

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `id` | `string` | Yes | Unique identifier for this route (e.g., `"api"`, `"grpc-users"`). |
| `kind` | `string` | Yes | Network layer kind: `"l7"` (HTTP/gRPC) or `"l4"` (TCP/UDP raw stream). |
| `listener` | `string` | Yes | ID of the listener binding this route (must match a valid listener in `listeners.json`). |
| `match` | `object` | Yes | Matching criteria (Host, Path, Prefix, Protocol). |
| `timeouts` | `object` | No | Route-specific timeout overrides. |
| `upstream` | `string` | Yes | ID of the target logical upstream pool in `upstreams.json`. |
| `session_affinity` | `object` | No | Strictly for `kind: "l4"` UDP routes. Configures stateful affinity (`{"type": "client_ip"}` or `{"type": "token", "offset": 0, "len": 8}`) for Game Tickets & NAT rebinding resilience. |
| `plugins` | `object` | No | Phased plugin execution chains (`pre_upstream` and `post_response`). |

---

## 3. Matching Criteria (`match`)

```json
{
  "host": "api.example.com",
  "path_prefix": "/api"
}
```

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `host` | `string` | No | Exact HTTP `Host` or `:authority` header match. |
| `path` | `string` | No | Exact path match (e.g., `"/events"`, `"/healthz"`). |
| `path_prefix` | `string` | No | Prefix path match using Radix Tree trie lookup (e.g., `"/api"`, `"/users."`). |
| `protocol` | `string` | No | Strictly for `kind: "l4"` routes (`"tcp"` or `"udp"`). |

---

## 4. Phased Plugin Architecture (`plugins`)

Velda Edge cleanly divides route-level plugins into two execution phases:

```json
"plugins": {
  "pre_upstream": [
    "jwt_auth",
    "rate_limit"
  ],
  "post_response": [
    "security_headers",
    "access_log"
  ]
}
```

### `pre_upstream` (Request Guard & Mutation Chain)
* Evaluated **after** route matching and endpoint selection, immediately **before** leasing a backend connection or streaming request bodies.
* **Execution Sequence**: Strictly sequential in declaration order (`index 0` runs first, `index 1` runs second).
* **Common Use Cases**:
  * Authentication & claims injection: `jwt_auth`
  * Per-user / per-route traffic shaping: `rate_limit`
  * Header decoration & stripping: `header_mutation`
* **Authority**: Returns `Action::Continue` to proceed, `Action::Respond` (e.g. 401, 429) to reject fail-fast, or `Action::Reject` to abort connection.

### `post_response` (Response Mutation Chain)
* Evaluated **after** receiving response headers/status from the upstream backend, immediately **before** downstream frame encoding.
* **Execution Sequence**: Strictly sequential in declaration order.
* **Common Use Cases**:
  * Browser security enforcement: `security_headers` (HSTS, CSP, Frame-Options)
  * Cross-Origin Resource Sharing: `cors`
  * Access telemetry & audit logging: `access_log`
