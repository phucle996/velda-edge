# IP / CIDR Subnet Access Filter Plugin (`ip_filter`)

High-performance L7 ingress gatekeeper enforcing client IP address and CIDR subnet access control prior to route lookup, body streaming, or upstream connection leasing.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `ip_filter` |
| **Canonical Phase** | `pre_route` |
| **Execution Order** | Stably sorted descending by route `priority` (Recommended `100+` for fail-fast gatekeeping) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (Can generate `Action::Respond` 403) |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Hot-Path Overhead** | Single-digit nanoseconds; bitmask integer arithmetic; **0 heap allocations** |

### What it Protects
- **Fail-Fast Access Control**: Rejects unauthorized requests immediately with `403 Forbidden`, saving 100% of CPU cycles that would otherwise be wasted on routing trie lookups, body streaming, or upstream connection pool leases.
- **CIDR Subnet Filtering (IPv4 & IPv6)**: Supports both individual IP addresses (`192.168.1.50`, `::1`) and broad network prefixes (`10.0.0.0/8`, `172.16.0.0/12`, `2001:db8::/32`).
- **Exception Handling & Lockdown**: Deny rules take absolute precedence over allow rules (e.g. allow entire VPC `10.0.0.0/8` while blocking compromised host `10.0.5.99`).

### Out of Scope
- **Header / Payload Inspection**: Does not inspect request bodies or arbitrary headers.
- **Cryptographic Authentication**: Token and signature validation is owned by `jwt_auth`.
- **Traffic Rate Pacing**: Request rate limiting is owned by `rate_limit`.
- **L4 Firewalling**: Low-level TCP socket drops belong to kernel/eBPF; this plugin operates at L7 HTTP/gRPC ingress.

---

## 2. Configuration Field Specification

Unused lists must use the explicit `"none"` sentinel value rather than empty arrays or nulls.

| Field | Type | Required / Default | Constraints / Allowed Values | Description |
| :--- | :--- | :--- | :--- | :--- |
| `mode` | string | Optional (`"allow"`) | `"allow"`, `"deny"` | Default filtering mode (`"allow"` = whitelist, `"deny"` = blacklist) |
| `allow_cidrs` | array[string] \| string | **Required** in allow mode | Array of CIDR strings or `"none"` | Authorized IPv4/IPv6 networks or individual IPs |
| `deny_cidrs` | array[string] \| string | **Required** in deny mode | Array of CIDR strings or `"none"` | Blocked IPv4/IPv6 networks or individual IPs |
| `client_ip_source` | string | Optional (`"direct"`) | `"direct"`, `"peer"`, `"x_forwarded_for"`, `"x_real_ip"` | Method used to extract downstream client IP address |

---

## 3. Headers Contract

### Request Headers Inspected
- `x-forwarded-for`: Inspected when `client_ip_source` is set to `"x_forwarded_for"`. Extracts the immediate downstream client IP.
- `x-real-ip`: Inspected when `client_ip_source` is set to `"x_real_ip"`.

### Response Generated on Violation (`403 Forbidden`)
- Status: `403 Forbidden`.
- Body: `Forbidden: IP address not allowed`.
- Short-circuits the pipeline immediately (`Action::Respond`), preventing further execution.

---

## 4. Configuration Examples

### Whitelist Mode (Corporate Office & VPN Access)
```json
{
  "id": "ip_filter",
  "priority": 100,
  "config": {
    "mode": "allow",
    "allow_cidrs": [
      "10.0.0.0/8",
      "192.168.1.0/24",
      "2001:db8::/32"
    ],
    "deny_cidrs": "none",
    "client_ip_source": "x_forwarded_for"
  }
}
```

### Blacklist Mode with Allowed Exceptions
```json
{
  "id": "ip_filter",
  "priority": 100,
  "config": {
    "mode": "deny",
    "allow_cidrs": [
      "192.168.1.100/32"
    ],
    "deny_cidrs": [
      "192.168.1.0/24",
      "203.0.113.0/24"
    ],
    "client_ip_source": "direct"
  }
}
```

---

## 5. Architectural & Hot-Path Invariants

1. **Zero-Allocation Bitmask Math**: All CIDR entries are parsed and pre-compiled during initialization into `(u32, u32)` (IPv4) or `(u128, u128)` (IPv6) tuples. Hot-path matching performs pure bitwise `(ip & mask) == net`, requiring **0 heap allocations** and executing in under 10 nanoseconds.
2. **Deny Precedence**: If an IP matches both `allow_cidrs` and `deny_cidrs`, the deny rule always takes precedence.
