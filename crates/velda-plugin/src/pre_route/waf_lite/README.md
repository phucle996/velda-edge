# Web Application Firewall Lite Plugin (`waf_lite`)

High-performance, zero-regex L7 ingress perimeter firewall that inspects request URI paths, query strings, headers, and User-Agents at the edge before route matching, body reading, or upstream connection pool leasing.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `waf_lite` |
| **Canonical Phase** | `pre_route` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (or `Write` in `detect` mode) |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Hot-Path Overhead** | Single-digit to tens of nanoseconds ($< 50\ \text{ns}$); **0 heap allocations** |

### What it Protects
1. **Path Traversal / Directory Climbing**: Blocks `../`, `..%2f`, `%2e%2e/`, `/etc/passwd`, `/etc/shadow`, `/proc/self`, `boot.ini`, `.env`, `.git/`.
2. **SQL Injection (SQLi)**: Blocks `' or '1'='1`, `union select`, `union all select`, `; drop table`, `; exec(`, `benchmark(`, `waitfor delay`.
3. **Cross-Site Scripting (Reflected XSS)**: Blocks `<script`, `</script`, `javascript:`, `onerror=`, `onload=`, `<svg/onload`, `<img src=x`.
4. **Malicious Vulnerability Scanners**: Blocks automated tools by User-Agent (`sqlmap`, `nikto`, `acunetix`, `masscan`, `dirbuster`, `gobuster`, `wpscan`, `nuclei`, `nmap`).
5. **Remote Code Execution (RCE) / Log4j**: Blocks `${jndi:`, `$(curl`, `$(wget`, `; /bin/sh`, `; /bin/bash`.

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `mode` | String | **Yes** | `"block"` or `"detect"` | `"block"` | Operational mode. `"block"` immediately halts with `403 Forbidden`. `"detect"` allows traffic through while injecting warning audit headers. |
| `check_path` | Boolean | **Yes** | `true` or `false` (At least one check must be `true`) | `true` | Enables attack signature scanning on request URI path. |
| `check_query` | Boolean | **Yes** | `true` or `false` | `true` | Enables attack signature scanning on URL query parameters. |
| `check_user_agent` | Boolean | **Yes** | `true` or `false` | `true` | Enables scanner tool detection against HTTP `User-Agent`. |
| `check_headers` | Array of strings | **Yes** | Valid RFC 9110 header names (`[]` if none) | `["referer", "x-forwarded-host"]` | Additional request headers to scan for malicious injection payloads. |
| `custom_blocked_patterns` | Array of strings | **Yes** | Non-empty substring patterns (`[]` if none) | `["${jndi:", ".env", "wp-login.php"]` | Custom attack signatures to detect and block. |
| `custom_blocked_user_agents` | Array of strings | **Yes** | Non-empty user-agent substrings (`[]` if none) | `["badbot", "crawler-scanner"]` | Custom user-agent tool signatures to detect and block. |

---

## 3. Headers Contract

### Request Headers Inspected
- `User-Agent`: Inspected when `check_user_agent: true`.
- Custom headers defined in `check_headers` (e.g. `Referer`, `Origin`, `X-Forwarded-Host`).

### Response Generated on Violation (`403 Forbidden`)
- Status: `403 Forbidden`.
- Header: `Content-Type: application/json`.
- Body: `{"error":"Forbidden: Malicious payload detected"}`.
- Short-circuits the pipeline immediately (`Action::Respond`), preventing further route matching or backend dispatch.

### Request Headers Injected in `detect` Mode
- `x-waf-threat-detected`: `"true"`.
- `x-waf-threat-type`: String classification (`"path_traversal"`, `"sql_injection"`, `"xss"`, `"rce"`, `"malicious_scanner"`, `"custom_pattern"`).

---

## 4. Configuration Examples

### Production Manifest (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "waf_lite",
      "config": {
        "mode": "block",
        "check_path": true,
        "check_query": true,
        "check_user_agent": true,
        "check_headers": [
          "referer",
          "x-forwarded-host"
        ],
        "custom_blocked_patterns": [
          "${jndi:",
          ".env",
          "/etc/passwd"
        ],
        "custom_blocked_user_agents": [
          "badbot",
          "crawler-scanner"
        ]
      }
    }
  ]
}
```

### Route Attachment (`routes.json`)

```json
{
  "id": "api-orders-route",
  "match": {
    "path": "/api/v1/orders"
  },
  "upstream_id": "orders-backend",
  "plugins": [
    "ip_filter",
    "resource_limit",
    "cors_preflight",
    "waf_lite",
    "jwt_auth"
  ]
}
```

---

## 5. Architectural & Hot-Path Invariants

1. **Pre-Route Ingress Isolation**: Executes before route trie resolution. Scanners probing non-existent or administrative endpoints are neutralized without executing router logic.
2. **Zero Regex Backtracking**: Uses fast deterministic byte scanning, eliminating ReDoS (Regular Expression Denial of Service) risks.
3. **Zero Heap Allocation for Clean Traffic**: Clean requests traverse the scanner without memory allocations.
