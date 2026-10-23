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
| **Memory Invariant** | 100% zero-copy, raw byte slice (`&[u8]`) scanning on stack (`[0u8; 1024]`); zero UTF-8 conversion overhead |

### What it Protects
1. **Path Traversal / Directory Climbing**: Blocks `../`, `..\\`, `..;/`, `..;\\`, `/etc/passwd`, `/etc/shadow`, `/proc/self`, `boot.ini`, `win.ini`, `.env`, `.git/`, `id_rsa`, with native defense against single, double, and triple percent-encoding (`%2e%2e%2f`, `%252e%252e%252f`, `%25252e%25252e%25252f`).
2. **SQL Injection (SQLi)**: Blocks `' or '1'='1`, `union select`, `union all select`, `union(`, `sleep(`, `xp_cmdshell`, `; drop table`, `benchmark(`, `waitfor delay`, `/*!`, with automatic form-urlencoded `+` to space conversion, whitespace collapsing, and bounded SQL comment stripping (`UNION/**/SELECT`, `UNION--\nSELECT`, `UNION#\nSELECT`).
3. **Cross-Site Scripting (Reflected XSS)**: Blocks `<script`, `</script`, `javascript:`, `onerror=`, `onload=`, `<svg/onload`, `<img src=x`, `<iframe src=`, `document.cookie`, `document.location`.
4. **Malicious Vulnerability Scanners**: Blocks automated tools by User-Agent (`sqlmap`, `nikto`, `acunetix`, `masscan`, `dirbuster`, `gobuster`, `wpscan`, `nuclei`, `nmap`, `zgrab`, `metasploit`, `nessus`, `shodan`, `openvas`, `burpcollaborator`).
5. **Remote Code Execution (RCE) / Log4j & Framework CVEs**: Blocks `${jndi:`, `}ndi:`, `${${`, `${lower:`, `${env:`, `/bin/sh`, `/bin/bash`, `;bash`, `|bash`, `&&bash`, `;whoami`, `|whoami`, `$(id)`, `` `id` ``, `${ifs}`, `$ifs`, `powershell`, `cmd.exe`, `sh -i`, `nc -e`, `eval(`, `system(`, Spring4Shell (`class.module.classloader`), Struts OGNL (`#_memberaccess`).
6. **Evasion, Comment Wrapping & Header Smuggling**:
   - **Two-Stage Pre/Post Comment-Strip Scanning**: Scans percent-decoded bytes *before* and *after* SQL comment folding so attackers cannot hide `<script>`, `${jndi:...}`, or `;whoami` inside `/* ... */` or `-- ... \n`.
   - **Cross-Parameter Comment Smuggling Immunity**: SQL comment folding (`/*...*/`, `--...\n`, `#...\n`) never crosses query parameter delimiters (`&`), HTML tags (`<`), or variable expressions (`$`), and never truncates trailing parameters on unterminated `-- `.
   - **Raw Non-ASCII Byte Scanning**: Scans `HeaderValue.as_bytes()` directly, preventing attackers from bypassing inspection by injecting `0x80`–`0xFF` bytes before payloads.
   - **Duplicate Header Smuggling**: Scans all occurrences of duplicate headers (`HeaderMap::get_all`) to prevent hiding attacks in secondary headers.
   - **Multi-Pass URL Decoding**: Recursive 3-pass percent decoding prevents nested `%25252e` $\to$ `%252e` $\to$ `%2e` $\to$ `.` evasion.

---

## 2. Built-in Attack Signatures & Scanner Dictionary

Velda Edge ships with **119 built-in attack signatures** and **15 malicious scanner User-Agents** in [`dictionary.rs`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/src/pre_route/waf_lite/dictionary.rs), compiled directly into the SIMD Aho-Corasick automaton. **You do NOT need to configure these patterns in `custom_blocked_patterns` or `custom_blocked_user_agents`.**

> [!TIP]
> **Automatic Deduplication ("Khử trùng")**:
> If any pattern specified in `custom_blocked_patterns` or `custom_blocked_user_agents` already exists in (or is a superstring of) a built-in dictionary entry, or is declared multiple times, `velda-sync` automatically eliminates the redundant entry during configuration compilation and persists the deduplicated list into `plugins.bin`.

### 2.1 Built-in Attack Signatures (`BUILTIN_ATTACK_SIGNATURES` — 119 Signatures)

| Category | Signature Substrings | Description & Protected CVEs |
| :--- | :--- | :--- |
| **Path Traversal & System Files (20)** | `../`, `..\`, `..;/`, `..;\`, `/etc/passwd`, `/etc/shadow`, `/etc/hosts`, `/etc/issue`, `/proc/self`, `/proc/version`, `boot.ini`, `win.ini`, `system.ini`, `.env`, `.git/`, `.svn/`, `.htaccess`, `web.config`, `id_rsa`, `id_ed25519` | Directory climbing, Tomcat path traversal (CVE-2018-1305, CVE-2020-1938), Linux/Windows system file access, SSH private keys, environment secrets. |
| **SQL Injection (26)** | `' or '1'='1`, `" or "1"="1`, `' or 1=1`, `" or 1=1`, `' or ''='`, `union select`, `union all select`, `union distinct select`, `union(`, `union all(`, `union distinct(`, `; drop table`, `; drop database`, `; exec(`, `; execute(`, `; shutdown`, `benchmark(`, `waitfor delay`, `sleep(`, `extractvalue(`, `updatexml(`, `xp_cmdshell`, `into outfile`, `into dumpfile`, `load_file(`, `/*!` | Standard SQLi, Parenthesized SQLi without spaces, MySQL/PostgreSQL time-based blind SQLi (`sleep(` covers both `sleep(` and `pg_sleep(`), Error-based SQLi, SQL Server DB RCE (`xp_cmdshell`), MySQL webshell dumps & versioned comment execution (`/*!`). |
| **Cross-Site Scripting (18)** | `<script`, `</script`, `javascript:`, `vbscript:`, `data:text/html`, `onerror=`, `onload=`, `onclick=`, `onmouseover=`, `onfocus=`, `onblur=`, `<svg/onload`, `<svg onload`, `<img src=x`, `<body onload`, `<iframe src=`, `document.cookie`, `document.location` | Reflected and DOM-based XSS vectors, inline event handlers, embedded SVG vectors, iframe injection, cookie theft. |
| **Remote Code Execution & CVEs (55)** | `${jndi:`, `}ndi:`, `jndi:ldap`, `jndi:rmi`, `jndi:dns`, `jndi:iiop`, `jndi:nis`, `${${`, `${lower:`, `${upper:`, `${::-`, `${env:`, `${sys:`, `${java:`, `/bin/sh`, `/bin/bash`, `/bin/zsh`, `;bash`, `; bash`, `\|bash`, `\| bash`, `&&bash`, `&& bash`, `;zsh`, `; zsh`, `\|zsh`, `\| zsh`, `&&sh`, `&& sh`, `;sh -`, `\|sh -`, `$(curl`, `$(wget`, `$(id`, `$(whoami`, `$(cat`, `$(uname`, `` `id` ``, `` `whoami` ``, `` `curl ` ``, `` `wget ` ``, `;whoami`, `; whoami`, `\|whoami`, `\| whoami`, `;cat `, `; cat `, `\|cat `, `\| cat `, `${ifs}`, `$ifs`, `/dev/tcp/`, `/dev/udp/`, `sh -i`, `nc -e`, `nc.exe`, `powershell`, `cmd.exe`, `certutil -urlcache`, `base64 -d`, `base64 --decode`, `<?php`, `eval(`, `passthru(`, `shell_exec(`, `system(`, `class.module.classloader`, `#_memberaccess`, `#context[` | Log4j (CVE-2021-44228) including nested and single-level lookup evasions (`${${lower:j}ndi:`, `${env:NaN:-j}ndi:`), shell spawning, Unix/Windows command injection (safe from `&shipping=` / `;id=` false positives), subshells, backticks, `${IFS}` whitespace evasion, reverse shells (`sh -i` covers `bash -i`), PowerShell/cmd.exe execution, PHP code injection, Spring4Shell (CVE-2022-22965), Struts OGNL RCE. |

### 2.2 Built-in Malicious Vulnerability Scanners (`BUILTIN_MALICIOUS_USER_AGENTS` — 15 Scanners)

| Scanner / Bot Substring | Tool Type | Description |
| :--- | :--- | :--- |
| `sqlmap` | SQLi Automaton | Automated SQL injection scanner and database takeover tool. |
| `nikto` | Web Vulnerability Scanner | Open-source web server scanner for dangerous files, CGIs, and outdated software. |
| `acunetix` | Commercial DAST | Automated enterprise vulnerability scanner. |
| `masscan` | Port & Banner Scanner | High-speed Internet-scale port scanner. |
| `dirbuster`, `gobuster` | Directory Brute-Forcer | Forced browsing and directory/file enumeration tools. |
| `wpscan` | CMS Scanner | Dedicated WordPress vulnerability assessment scanner. |
| `nuclei` | Fast Vulnerability Scanner | Template-based fast vulnerability scanner. |
| `nmap`, `zgrab` | Network & Banner Scanner | Network discovery and application banner grabbing probes. |
| `metasploit` | Exploitation Framework | Automated penetration testing and exploit delivery framework. |
| `nessus`, `openvas` | Vulnerability Management | Enterprise security scanning and configuration audit tools. |
| `burpcollaborator` | Out-of-Band DAST | Burp Suite out-of-band vulnerability detection server probe. |
| `shodan` | Internet Reconnaissance | Automated Internet-wide vulnerability and metadata scraping bot. |

---

## 3. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. All field values must be intentionally supplied according to your infrastructure requirements.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `mode` | String | **Yes** | `"block"` or `"detect"` | `"block"` | Operational mode. `"block"` immediately short-circuits with static `403 Forbidden`. `"detect"` allows traffic through while injecting warning audit headers. |
| `check_path` | Boolean | **Yes** | `true` or `false` (At least one check must be `true`) | `true` | Enables attack signature scanning on request URI path. |
| `check_query` | Boolean | **Yes** | `true` or `false` | `true` | Enables attack signature scanning on URL query parameters with `+` decoding. |
| `check_user_agent` | Boolean | **Yes** | `true` or `false` | `true` | Enables scanner tool detection across all HTTP `User-Agent` headers. |
| `check_headers` | Array of strings | **Yes** | Valid RFC 9110 header names (`[]` if none) | `["referer", "x-forwarded-host"]` | Additional request headers to scan across all occurrences for malicious injection payloads. |
| `custom_blocked_patterns` | Array of strings | **Yes** | Non-empty substring patterns (`[]` if none) | `["wp-login.php", "phpmyadmin"]` | Custom application-specific attack signatures to block (automatically deduplicated against built-ins). |
| `custom_blocked_user_agents` | Array of strings | **Yes** | Non-empty user-agent substrings (`[]` if none) | `["badbot", "custom-crawler"]` | Custom application-specific bot signatures to block (automatically deduplicated against built-ins). |

---

## 4. Headers Contract

### Request Headers Inspected
- `User-Agent`: Inspected when `check_user_agent: true` (all duplicate entries).
- Custom headers defined in `check_headers` (e.g. `Referer`, `Origin`, `X-Forwarded-Host`, `X-Forwarded-For`).

### Response Generated on Violation (`403 Forbidden`)
- Status: `403 Forbidden`.
- Header: `Content-Type: application/json`.
- Body: `{"error":"Forbidden: Malicious payload detected"}`.
- Short-circuits the pipeline immediately (`Action::Respond`), preventing further route matching, payload reading, or upstream connection leasing.

### Request Headers Injected in `detect` Mode
- `x-waf-threat-detected`: `"true"` (pre-baked static header value).
- `x-waf-threat-type`: String classification (`"path_traversal"`, `"sql_injection"`, `"xss"`, `"rce"`, `"malicious_scanner"`, `"custom_pattern"`).

---

## 5. Configuration Examples

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
          "wp-login.php",
          "phpmyadmin"
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
    "path_prefix": "/api/v1/orders"
  },
  "upstream": "orders-backend",
  "plugins": {
    "pre_route": [
      "waf_lite"
    ],
    "post_response": [
      "access_log"
    ]
  }
}
```

---

## 6. Architectural & Hot-Path Invariants

1. **Pre-Route Ingress Isolation**: Executes before route trie resolution. Scanners probing non-existent or administrative endpoints are neutralized without executing router logic or touching upstream connection pools.
2. **Zero Regex Backtracking & ReDoS Immunity**: Uses SIMD multi-pattern Aho-Corasick matching (`AhoCorasick` automaton), guaranteeing deterministic $O(N)$ execution time regardless of payload complexity or length.
3. **Zero Heap Allocation & Zero UTF-8 Validation on Hot Path**:
   - Clean and malicious requests traverse raw byte slices (`&[u8]`) directly.
   - For inputs $\le 1024$ bytes (99.99% of paths, queries, and headers), normalization runs in-place on a fixed stack buffer (`[0u8; 1024]`).
   - No `std::str::from_utf8` validation is performed; no heap allocation (`String::from_utf8_lossy`) is triggered even when non-ASCII or corrupt bytes are transmitted.
4. **Pre-Baked Static Responses**: Rejections use pre-allocated static byte slices (`L7Response::from_bytes`) cloned by pointer in nanoseconds.

---

## 7. Performance Overhead & Capacity Profile

Empirically measured on **Azure Dedicated Virtual Machines** (4 vCPUs AMD EPYC 7763, 64 concurrent connections) over 10 million requests:

| Benchmark Metric | Baseline (No WAF) | WAF Lite Active | Delta ($\Delta$) | Impact |
| :--- | :---: | :---: | :---: | :--- |
| **Clean Throughput (RPS)** | **44,387.24** | **43,801.79** | $-585.45\ \text{RPS}$ | **$-1.31\%$** (Negligible) |
| **P50 Latency (Median)** | **1.25 ms** | **1.27 ms** | $+0.02\ \text{ms}$ | **$+20\ \mu\text{s}$** (L3 Cache access duration) |
| **Attack Mitigation RPS** | — | **108,089.42** | $+63,702.18\ \text{RPS}$ | **$2.46\times$ proxy capacity multiplier** |
| **Attack Mitigation Latency** | — | **470.00 µs** | $-780.00\ \mu\text{s}$ | Sub-millisecond instant 403 rejection |
| **Heap Allocations** | $0$ | $0$ | $0$ | Zero heap allocations on stack buffer |

For complete benchmark tables, latency percentiles, and resource profiles, see [BENCHMARK.md](BENCHMARK.md).
