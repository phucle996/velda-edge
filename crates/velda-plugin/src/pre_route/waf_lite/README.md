# Web Application Firewall Lite Plugin (`waf_lite`)

High-performance, zero-regex L7 ingress perimeter firewall that inspects request URI paths, query strings, headers, and User-Agents at the edge before route matching, body reading, or upstream connection pool leasing.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `waf_lite` |
| **Canonical Phase** | `pre_route` |
| **Execution Order** | Sequential FIFO based on route's `plugins` declaration order (First declared, first executed) |
| **Reverse Capability** | `PluginCapability::header_only(AccessLevel::Read)` (or `Write` in `detect_only` mode) |
| **Supported Protocols** | `ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3) |
| **Hot-Path Overhead** | Single-digit to tens of nanoseconds ($< 50\ \text{ns}$); **0 heap allocations** |

### What it Protects & Solves
- **Fails Fast Before Route Matching**: Blocks automated botnets and malicious exploit attempts immediately with `403 Forbidden`, saving 100% of CPU routing lookups, memory allocations, and backend connection leasing.
- **5 Core Attack Vectors Shielded**:
  1. **Path Traversal / Directory Climbing**: Blocks `../`, `..%2f`, `%2e%2e/`, `/etc/passwd`, `/etc/shadow`, `/proc/self`, `boot.ini`, `.env`, `.git/`.
  2. **SQL Injection (SQLi)**: Blocks `' or '1'='1`, `union select`, `union all select`, `; drop table`, `; exec(`, `benchmark(`, `waitfor delay`.
  3. **Cross-Site Scripting (Reflected XSS)**: Blocks `<script`, `</script`, `javascript:`, `onerror=`, `onload=`, `<svg/onload`, `<img src=x`.
  4. **Malicious Vulnerability Scanners**: Blocks automated tools by User-Agent (`sqlmap`, `nikto`, `acunetix`, `masscan`, `dirbuster`, `gobuster`, `wpscan`, `nuclei`, `nmap`).
  5. **Remote Code Execution (RCE) / Log4j**: Blocks `${jndi:`, `$(curl`, `$(wget`, `; /bin/sh`, `; /bin/bash`.
- **Anti-Evasion Normalization**: In-place fast percent-decoding (`%2e%2e` $\to$ `..`, `%20` $\to$ ` `) and case folding to defeat common URL-encoding evasion techniques.
- **Preserves Zero-Copy Direct Streaming**: Strictly inspects request headers and URI (`payload_access: None`), leaving request body streaming intact and unbuffered.

---

## 2. Configuration Field Specification

| Field | Type | Required / Default | Constraints / Allowed Values | Description |
| :--- | :--- | :--- | :--- | :--- |
| `mode` | string | Optional (`"block"`) | `"block"`, `"detect_only"` | Operational behavior (`"block"` returns 403, `"detect_only"` injects audit headers) |
| `check_path` | boolean | Optional (`true`) | `true`, `false` | Enables scanning on request URI path |
| `check_query` | boolean | Optional (`true`) | `true`, `false` | Enables scanning on URL query parameters |
| `check_user_agent` | boolean | Optional (`true`) | `true`, `false` | Enables scanning on HTTP `User-Agent` header |
| `check_headers` | array[string] | Optional (`[]`) | Array of HTTP header names | Additional custom headers to scan (e.g. `referer`, `x-forwarded-host`) |
| `custom_blocked_patterns` | array[string] | Optional (`[]`) | Array of substring patterns | Operator-defined custom attack signatures |
| `custom_blocked_user_agents` | array[string] | Optional (`[]`) | Array of user-agent strings | Operator-defined custom scanner signatures |

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

### Request Headers Injected in `detect_only` Mode
- `x-waf-threat-detected`: `"true"`.
- `x-waf-threat-type`: String classification (`"path_traversal"`, `"sql_injection"`, `"xss"`, `"rce"`, `"malicious_scanner"`, `"custom_pattern"`).

---

## 4. Configuration Examples

### Minimal Production Configuration (Blocking Mode)
```json
{
  "id": "waf_lite",
  "config": {
    "mode": "block",
    "check_path": true,
    "check_query": true,
    "check_user_agent": true
  }
}
```

### Advanced Configuration with Custom Patterns & Extra Headers
```json
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
      "/etc/passwd",
      ".env",
      "wp-login.php"
    ],
    "custom_blocked_user_agents": [
      "badbot",
      "crawler-scanner"
    ]
  }
}
```

### Staging Observability Configuration (Detect-Only Mode)
```json
{
  "id": "waf_lite",
  "config": {
    "mode": "detect_only",
    "check_path": true,
    "check_query": true
  }
}
```

---

## 5. Architectural & Hot-Path Invariants

1. **Pre-Route Ingress Isolation**: Executes before route trie resolution. Scanners probing non-existent or administrative endpoints are neutralized without executing router logic.
2. **Zero Regex Backtracking**: Uses fast deterministic byte scanning, eliminating ReDoS (Regular Expression Denial of Service) risks.
3. **Zero Heap Allocation for Clean Traffic**: Clean requests traverse the scanner without memory allocations.
