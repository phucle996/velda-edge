# L7 IP Filter & CIDR Firewall (`ip_filter`) — SRE Operational Runbook & Configuration Guide

This document is the operational playbook and configuration guide (**SRE Runbook & Tuning Guide**) for engineers configuring, operating, and troubleshooting the `ip_filter` pre-route plugin in Velda Edge.

---

## 1. Configuration Parameter Reference

`ip_filter` enforces a **100% Explicit Configuration Invariant**: `mode`, `allow_cidrs`, `deny_cidrs`, and `client_ip_source` must be explicitly declared in `plugins.json`.

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

### Parameter Specifications

| Parameter | Type | Required | Allowed Values / Constraints | Operational Meaning & Security Impact |
| :--- | :---: | :---: | :--- | :--- |
| **`mode`** | `String` | **Yes** | `"allow"` or `"deny"` | **`"allow"`**: Default-deny posture; only IPs matching `allow_cidrs` (and not in `deny_cidrs`) pass. **`"deny"`**: Default-allow posture; blocks IPs matching `deny_cidrs` (unless explicitly exempted by `allow_cidrs`). |
| **`allow_cidrs`** | `Vec<String>` | **Yes** | Valid IPv4/IPv6 addresses or CIDRs (`[]` allowed only in `"deny"` mode) | Whitelist entries (when `mode = "allow"`) or exception overrides (when `mode = "deny"`). Bare IPs expand to `/32` or `/128`. |
| **`deny_cidrs`** | `Vec<String>` | **Yes** | Valid IPv4/IPv6 addresses or CIDRs (`[]` allowed) | Blacklist entries (when `mode = "deny"`) or block overrides (when `mode = "allow"`). |
| **`client_ip_source`** | `String` | **Yes** | `"x_forwarded_for"` or `"x_real_ip"` | Selects which HTTP header stream is evaluated right-to-left (or single token) to resolve the client IP without heap allocation. |

---

## 2. Scenario-Based Configuration Recipes

### Scenario A: Internal Admin / Backoffice Zero-Trust Whitelist
*Goal: Reject all public traffic; permit only corporate VPN and internal VPC CIDR blocks.*

```json
{
  "id": "admin_ip_whitelist",
  "config": {
    "mode": "allow",
    "allow_cidrs": ["10.100.0.0/16", "172.16.50.0/24", "2001:db8:corp::/48"],
    "deny_cidrs": ["10.100.99.42/32"],
    "client_ip_source": "x_real_ip"
  }
}
```
* **Why this setup**: In `"allow"` mode, `deny_cidrs` is evaluated **first** (allowing surgical revocation of a single compromised IP inside a broad corporate subnet), followed by `allow_cidrs`. Missing headers fail closed (`RejectReason::IpBlocked`).

### Scenario B: Public Edge Abuse Blacklist with Exception Override
*Goal: Block known botnet/abuse CIDRs at the perimeter while keeping a specific partner IP reachable.*

```json
{
  "id": "public_ip_denylist",
  "config": {
    "mode": "deny",
    "allow_cidrs": ["198.51.100.42/32"],
    "deny_cidrs": ["198.51.100.0/24", "203.0.113.0/24"],
    "client_ip_source": "x_forwarded_for"
  }
}
```

---

## 3. Observability & Troubleshooting Playbook

### Telemetry & Access Log Signals
- **Wire Response**: HTTP `403 Forbidden` (`b"403 Forbidden\n"`), gRPC `PermissionDenied ("Forbidden")`.
- **Internal Access Log**: `completion=reject:ip_blocked`
- **Prometheus Metrics**: `velda_requests_rejected_total{reason="ip_blocked"}`

### Incident Triage

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| All requests rejected with `reason="ip_blocked"` in `"allow"` mode | Upstream proxy/CDN is not injecting the configured `client_ip_source` header, or `allow_cidrs` is missing the proxy hop IP. | Verify `x-forwarded-for` / `x-real-ip` presence at ingress; ensure `client_ip_source` matches the actual ingress header. |
| Config rejected by `velda-sync` (`contradictory CIDR`) | Identical CIDR appears in both `allow_cidrs` and `deny_cidrs`, or `allow_cidrs` is empty when `mode = "allow"`. | Remove the duplicate conflicting entry or populate `allow_cidrs`. |
| Unexpected IP bypass via multi-hop header | Malicious client injected spoofed IPs on the left side of `X-Forwarded-For`. | `ip_filter` streams `X-Forwarded-For` right-to-left and rejects malformed/octal/oversized hops immediately. Ensure upstream trusted proxies append cleanly at the right edge. |
