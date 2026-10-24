# IP / CIDR Subnet Access Filter Plugin (`ip_filter`)

High-performance L7 ingress gatekeeper enforcing zero-trust client IP address and CIDR subnet access control prior to route lookup, body streaming, or upstream connection leasing.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `ip_filter` |
| **Canonical Phase** | `pre_route` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (Generates `Action::Reject(RejectReason::Forbidden)`) |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Hot-Path Overhead** | Single-digit nanoseconds (~2–5ns); byte-level decimal parsing; bitmask integer arithmetic; **0 heap allocations** |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `mode` | String | **Yes** | `"allow"` or `"deny"` | `"allow"` (for internal APIs) / `"deny"` (for public APIs) | Operational filtering mode. In `"allow"` mode, only specified CIDRs are permitted. In `"deny"` mode, specified CIDRs are blocked. |
| `allow_cidrs` | Array of strings | **Yes** | Array of valid IPv4/IPv6 CIDR or IP strings (`[]` if none) | `["10.0.0.0/8", "192.168.1.0/24"]` | Authorized networks when `mode: "allow"`, or exceptions when `mode: "deny"`. |
| `deny_cidrs` | Array of strings | **Yes** | Array of valid IPv4/IPv6 CIDR or IP strings (`[]` if none) | `[]` | Blocked networks when `mode: "deny"`, or exceptions when `mode: "allow"`. Deny rules take absolute precedence in `"allow"` mode. |
| `client_ip_source` | String | **Yes** | `"x_forwarded_for"` or `"x_real_ip"` | `"x_forwarded_for"` | Source header used to resolve client IP behind ingress load balancers. |

---

## 3. Headers Contract & Zero-Trust Anti-Evasion Hardening

### Request Headers Inspected
- `x-forwarded-for`: Inspected when `client_ip_source` is `"x_forwarded_for"`. Evaluates **every hop** across the forwarding chain (up to `MAX_XFF_HOPS = 16` hops and `MAX_XFF_HEADERS = 4` header lines) in a zero-allocation stack array (`[ParsedIp; 16]`).
- `x-real-ip`: Inspected when `client_ip_source` is `"x_real_ip"`. Strictly enforces a single header instance containing a single IP address (rejects duplicate `x-real-ip` headers or comma-separated lists).

### Header Parsing & CVE Defense Invariants
1. **Zero-Allocation Multi-Hop Chain Verification**:
   - In `"allow"` mode: **every hop** in `X-Forwarded-For` must match `allow_cidrs` and **no hop** may match `deny_cidrs`. Prepending a spoofed internal IP (`X-Forwarded-For: 10.0.0.1, 203.0.113.5`) is caught and rejected.
   - In `"deny"` mode: each hop is evaluated independently. An allow-exception IP (`allow_cidrs`) excuses **only that specific hop** and never grants a global bypass to subsequent hops (`X-Forwarded-For: 203.0.113.10, 203.0.113.5` is caught and rejected).
2. **Zero Cross-Header Fallback on Malformed Input**: If the configured primary header (`x-forwarded-for` or `x-real-ip`) is present in the request—even if empty or malformed—the parser **never** falls back to the secondary header. This prevents Cross-Header Fallback Poisoning.
3. **CVE-2021-28918 Octal / Leading-Zero IPv4 Defense**: Any multi-digit IPv4 octet starting with `'0'` (such as `010.0.0.1` or `0177.0.0.1`) is strictly rejected as `Malformed` (`403 Forbidden`) to eliminate octal-vs-decimal parser differentials between the edge gateway and downstream services.
4. **Validated Port & Bracket Stripping**: Safely strips client source ports (`192.168.1.1:54321`, `[2001:db8::1]:8080`) and single pairs of quotes (`"192.168.1.1"`), while strictly validating that any port suffix is a valid decimal integer in `1..=65535` with no leading zeros or userinfo smuggling (`10.0.0.1:80@203.0.113.5` and `[2001:db8::1]garbage` are rejected).
5. **Strict RFC 9110 OWS Trimming**: Only HTTP Optional Whitespace (`SP` `0x20` and `HTAB` `0x09`) is trimmed. Control characters (`\r`, `\n`, `\x0b`, `\x0c`, `\x00`) inside IP headers immediately trigger `403 Forbidden`.
6. **Universal IPv6-to-IPv4 Tunneling Canonicalization**: Automatically canonicalizes all four IPv6-embedded IPv4 transition mechanisms to native 32-bit IPv4 (`u32`) at both CIDR compilation time and request evaluation time:
   - RFC 4291 IPv4-mapped IPv6 (`::ffff:a.b.c.d` / `::ffff:0:0/96`)
   - RFC 2765 SIIT IPv4-translated IPv6 (`::ffff:0:a.b.c.d` / `::ffff:0:0:0/96`)
   - RFC 6052 Well-Known NAT64 (`64:ff9b::a.b.c.d` / `64:ff9b::/96`)
   - RFC 4291 §2.5.5.1 IPv4-compatible IPv6 (`::a.b.c.d`, excluding `::` and `::1`)

### Response Generated on Violation (`Action::Reject(RejectReason::Forbidden)`)
- **Action**: Returns `Action::Reject(RejectReason::Forbidden)` with zero runtime heap allocations.
- **Protocol-Native Downstream Mapping**:
  - **HTTP/1.1 & HTTP/2 & HTTP/3**: Downstream engine formats a precompiled `403 Forbidden` early response frame with `Cache-Control: no-store`.
  - **gRPC**: Downstream engine terminates the RPC stream with canonical `PERMISSION_DENIED` status trailer.
- **Information Disclosure Protection**: Automatically drops all Bit 0 internal/upstream headers from the error response, preventing header reflection or leak of sensitive routing/context tags.
- **Short-circuiting**: Immediately halts the pipeline, preventing body streaming or router trie traversal.

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

1. **Prefix Compaction & Subsumption Elimination**:
   - Broadest subnets (`/8`, `/16`) are sorted first to maximize early loop short-circuiting.
   - Child subnets completely contained within an existing broader subnet (e.g. `10.1.0.0/16` within `10.0.0.0/8`) are automatically pruned at both `velda-sync` normalization time and plugin compilation time, minimizing cache footprint and CPU cycle consumption.
2. **Cross-List Shadowing Detection (`velda-sync`)**:
   - Rejects exact duplicates across `allow_cidrs` and `deny_cidrs`.
   - In `"allow"` mode, rejects configurations where an `allow_cidr` is completely shadowed by a broader `deny_cidr` (e.g. `allow_cidrs: ["10.99.0.0/16"]` with `deny_cidrs: ["10.0.0.0/8"]`).
   - In `"deny"` mode, rejects configurations where a `deny_cidr` is completely shadowed by a broader `allow_cidr` (e.g. `deny_cidrs: ["203.0.113.5/32"]` with `allow_cidrs: ["203.0.113.0/24"]`).
3. **Pre-Baked Zero-Allocation Rejection**: Rejection responses are statically pre-baked with `Bytes::from_static` and pre-allocated header workspaces. Blocking bad traffic consumes **0 heap allocations** and executes in single-digit nanoseconds.
