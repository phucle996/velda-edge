# IP / CIDR Subnet Access Filter Plugin (`ip_filter`)

```text
+-----------------------------------------------------------------------------------+
|                        IP Filter Pre-Route Architecture                           |
|                                                                                   |
|  [Inbound L7 Request] (HTTP/1.1, HTTP/2, HTTP/3, gRPC)                            |
|         │                                                                         |
|         ▼                                                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Frontline Traffic Worker (velda-worker-{i}) — Pre-Route Phase               │  |
|  │  - Zero-Allocation Streaming Byte Parser (parse_ip_bytes, 56B stack frame)  │  |
|  │  - Right-to-Left X-Forwarded-For Evaluation (Anti-Spoofing / Anti-Smuggling)│  |
|  │  - Octal IPv4 Rejection (CVE-2021-28918) & IPv6 Tunnel Extraction (NAT64)   │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │                                         |
|                                         ▼                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Contiguous Flat Memory CIDR Engine (IpFilterEngine)                         │  |
|  │  - Partition 1 (Exact /32, /128): Equality == or O(log N) Binary Search     │  |
|  │  - Partition 2 (Subnets /1../31): Compile-Time SIMD Tiering                 │  |
|  │    ├─ x86-64-v3: AVX2 256-bit (8 rules/cycle)                               │  |
|  │    ├─ x86-64-v1: SSE2 128-bit (4 rules/cycle)                               │  |
|  │    └─ ARM64/All: 64-bit SWAR (2 rules/cycle)                                │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │                                         |
|                         Action::Reject(IpBlocked) / Continue                      |
+-----------------------------------------------------------------------------------+
```

The **`ip_filter`** plugin is Velda Edge's high-throughput network perimeter access controller. It enforces zero-trust IPv4 and IPv6 CIDR allowlists and denylists directly on raw header bytes before route lookup, body streaming, or upstream connection leasing.

---

## 1. What This Plugin Does

1. **Zero-Allocation Streaming IP Extraction**:
   - Extracts client IPs directly from `X-Forwarded-For` (right-to-left) or `X-Real-IP` byte slices without allocating strings, `Vec<IpAddr>`, or `SocketAddr` wrappers.
2. **Contiguous Flat Memory & Hierarchical SIMD Matching**:
   - Separates exact `/32` / `/128` rules from subnet masks (`/1..=/31`), evaluating subnets across 8 rules/cycle (AVX2), 4 rules/cycle (SSE2), or 2 rules/cycle (64-bit SWAR).
3. **Anti-Evasion & Canonical Tunnel Handling**:
   - Rejects leading-zero octal IPv4 confusion (CVE-2021-28918), duplicate header smuggling, and extracts embedded IPv4 addresses from IPv4-mapped (`::ffff:0:0/96`), 6to4 (`2002::/16`), Teredo (`2001:0000::/32`), and NAT64 (`64:ff9b::/96`) tunnels.
4. **Semantic Observability (`RejectReason::IpBlocked`)**:
   - Returns `Action::Reject(RejectReason::IpBlocked)`: generic HTTP `403 Forbidden` externally, `ip_blocked` internally.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/pre_route/ip_filter/`
- **Execution Stage**: `PreRoutePlugin` (compile-time fixed)
- **Runtime Placement**: Executed inline on Frontline Traffic Workers (`velda-worker-{i}`) as the first security gate before `resource_limit` and `waf_lite`.

---

## 3. Requirements & Prerequisites

1. **Explicit Configuration Contract**: `mode` (`"allow"` or `"deny"`), `allow_cidrs`, `deny_cidrs`, and `client_ip_source` must all be explicitly declared.
2. **Trusted Proxy Hygiene**: Ensure upstream load balancers append the real client IP cleanly to the configured `client_ip_source` header.

---

## 4. Quick Start Configuration

```json
{
  "id": "ip_filter",
  "config": {
    "mode": "deny",
    "allow_cidrs": ["10.0.0.0/8"],
    "deny_cidrs": ["192.0.2.10/32", "203.0.113.0/24"],
    "client_ip_source": "x_forwarded_for"
  }
}
```

---

## 5. Documentation Directory

- **[RUNBOOK.md](./RUNBOOK.md)**: Parameter specifications, zero-trust allowlist/blacklist presets, and troubleshooting playbooks.
- **[ARCHITECTURE.md](./ARCHITECTURE.md)**: Deep internals (Contiguous CIDR layout, AVX2/SSE2/SWAR matching, right-to-left streaming state machine).
- **[BENCHMARK.md](./BENCHMARK.md)**: Standardized 5-tier diagnostic benchmark matrix and verified `x86-64-v3` release assembly audit.
- **[TEST.md](./TEST.md)**: Unit, SIMD tier parity, and CVE adversarial test matrix.
