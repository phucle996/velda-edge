# IP / CIDR Subnet Access Filter Plugin (`ip_filter`)

High-performance L7 ingress gatekeeper enforcing client IP address and CIDR subnet access control prior to route lookup, body streaming, or upstream connection leasing.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `ip_filter` |
| **Canonical Phase** | `pre_route` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (Can generate `Action::Respond` 403) |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Hot-Path Overhead** | Single-digit nanoseconds; bitmask integer arithmetic; **0 heap allocations** |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `mode` | String | **Yes** | `"allow"` or `"deny"` | `"allow"` (for internal APIs) / `"deny"` (for public APIs) | Operational filtering mode. In `"allow"` mode, only specified CIDRs are permitted. In `"deny"` mode, specified CIDRs are blocked. |
| `allow_cidrs` | Array of strings | **Yes** | Array of valid IPv4/IPv6 CIDR or IP strings (`[]` if none) | `["10.0.0.0/8", "192.168.1.0/24"]` | Authorized networks when `mode: "allow"`, or exceptions when `mode: "deny"`. |
| `deny_cidrs` | Array of strings | **Yes** | Array of valid IPv4/IPv6 CIDR or IP strings (`[]` if none) | `[]` | Blocked networks when `mode: "deny"`, or exceptions when `mode: "allow"`. Deny rules always take absolute precedence. |
| `client_ip_source` | String | **Yes** | `"x_forwarded_for"` or `"x_real_ip"` | `"x_forwarded_for"` | Source header used to resolve client IP behind ingress load balancers. |

---

## 3. Headers Contract

### Request Headers Inspected
- `x-forwarded-for`: Inspected when `client_ip_source` is `"x_forwarded_for"`. Extracts the immediate downstream client IP.
- `x-real-ip`: Inspected when `client_ip_source` is `"x_real_ip"`.

### Response Generated on Violation (`403 Forbidden`)
- Status: `403 Forbidden`.
- Body: `Forbidden: IP address not allowed`.
- Short-circuits the pipeline immediately (`Action::Respond`), preventing further execution.

---

## 4. Configuration Examples

### Whitelist Mode (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "ip_filter",
      "config": {
        "mode": "allow",
        "allow_cidrs": [
          "10.0.0.0/8",
          "192.168.1.0/24",
          "2001:db8::/32"
        ],
        "deny_cidrs": [],
        "client_ip_source": "x_forwarded_for"
      }
    }
  ]
}
```

### Blacklist Mode with Allowed Exceptions (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "ip_filter",
      "config": {
        "mode": "deny",
        "allow_cidrs": [
          "192.168.1.100/32"
        ],
        "deny_cidrs": [
          "192.168.1.0/24",
          "203.0.113.0/24"
        ],
        "client_ip_source": "x_forwarded_for"
      }
    }
  ]
}
```

### Route Attachment (`routes.json`)

```json
{
  "id": "admin-internal-route",
  "match": {
    "path": "/admin"
  },
  "upstream_id": "admin-backend",
  "plugins": [
    "ip_filter",
    "resource_limit",
    "jwt_auth"
  ]
}
```

---

## 5. Architectural & Hot-Path Invariants

1. **Zero-Allocation Bitmask Math**: All CIDR entries are parsed and pre-compiled during initialization into bitmasks (`(u32, u32)` for IPv4 and `(u128, u128)` for IPv6). Hot-path matching performs pure bitwise `(ip & mask) == net`, requiring **0 heap allocations** and executing in under 10 nanoseconds.
2. **Deny Precedence**: If an IP matches both `allow_cidrs` and `deny_cidrs`, the deny rule always takes precedence.
