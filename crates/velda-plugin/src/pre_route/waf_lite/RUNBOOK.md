# Web Application Firewall Lite (`waf_lite`) — SRE Operational Runbook & Configuration Guide

This document is the operational playbook, configuration reference, and recommended signature dictionary guide (**SRE Runbook & Tuning Guide**) for engineers operating `waf_lite` in Velda Edge.

---

## 1. Configuration Parameter Reference

`waf_lite` follows a **100% Config-Driven (WYCIWIS)** invariant: zero hidden built-in signatures, zero `detect` header leaks, and hardcoded inspection boundaries (URI Path + Query, plus leading 32 KB of Request Body).

```json
{
  "id": "waf_lite",
  "phase": "pre_route",
  "config": {
    "patterns": [
      "../",
      "..\\",
      "/etc/passwd",
      "' or '1'='1",
      "union select",
      "<script",
      "${jndi:"
    ]
  }
}
```

### Parameter Specifications

| Parameter | Type | Required | Constraints | Operational Meaning & Security Impact |
| :--- | :---: | :---: | :--- | :--- |
| **`patterns`** | `Vec<String>` | **Yes** | Non-empty strings (whitespace-trimmed, auto-lowercased, sorted & deduplicated) | Substring signatures scanned via SIMD Aho-Corasick across URI Path, URI Query, and the first **32 KB (`32,768` bytes)** of Payload. Empty array `[]` acts as a zero-overhead pass-through. |

### Hardcoded Engine Invariants
- **Inspection Scope**: Always scans **URI Path** (without `+` $\to$ space), **URI Query** (with `+` $\to$ space), and **Leading Body** (capped at `32,768` bytes).
- **Rejection Action**: Always returns `Action::Reject(RejectReason::WafBlocked)` (HTTP `403 Forbidden` generic on wire, zero WAF header leakage).

---

## 2. Recommended WAF Starter Dictionaries (OWASP / Common L7 Vectors)

Because `waf_lite` ships with zero hidden built-in rules, you have complete control over false positives and rule coverage. Copy the categories below into your `patterns` configuration according to your application profile.

### 2.1 Path Traversal & Sensitive System Files
```json
[
  "../", "..\\", "..;/", "..;\\",
  "/etc/passwd", "/etc/shadow", "/etc/hosts", "/etc/issue",
  "/proc/self", "/proc/version",
  "boot.ini", "win.ini", "system.ini",
  ".env", ".git/", ".svn/", ".htaccess", "web.config", "id_rsa", "id_ed25519"
]
```

### 2.2 SQL Injection (SQLi)
```json
[
  "' or '1'='1", "\" or \"1\"=\"1", "' or 1=1", "\" or 1=1", "' or ''='",
  "union select", "union all select", "union distinct select",
  "union(", "union all(", "union distinct(",
  "; drop table", "; drop database", "; exec(", "; execute(", "; shutdown",
  "benchmark(", "waitfor delay", "sleep(", "extractvalue(", "updatexml(",
  "xp_cmdshell", "into outfile", "into dumpfile", "load_file(", "/*!"
]
```

### 2.3 Cross-Site Scripting (XSS)
```json
[
  "<script", "</script", "javascript:", "vbscript:", "data:text/html",
  "onerror=", "onload=", "onclick=", "onmouseover=", "onfocus=", "onblur=",
  "<svg/onload", "<svg onload", "<img src=x", "<body onload", "<iframe src=",
  "document.cookie", "document.location"
]
```

### 2.4 Remote Code Execution (RCE), Log4j & Framework CVEs
```json
[
  "${jndi:", "}ndi:", "${${", "${lower:", "${env:",
  "/bin/sh", "/bin/bash", ";bash", "|bash", "&&bash", ";whoami", "|whoami",
  "$(id)", "`id`", "${ifs}", "$ifs", "powershell", "cmd.exe", "sh -i", "nc -e",
  "eval(", "system(", "class.module.classloader", "#_memberaccess"
]
```

> [!TIP]
> **Avoiding False Positives on Random URL Tokens**:
> Always configure multi-character syntactic signatures (e.g., `"union select"`, `"<script"`, `"../"`, `"${jndi:"`) rather than single generic words like `"select"`, `"or"`, or `"id"`. Random UUIDs and Base64url tokens only use `[A-Za-z0-9_-]` and will not collide with syntactic patterns.

---

## 3. Scenario-Based Configuration Recipes

### Scenario A: Standard REST API & Web Gateway
*Goal: Comprehensive Path Traversal, SQLi, XSS, and RCE perimeter defense.*

```json
{
  "id": "waf_lite_api",
  "phase": "pre_route",
  "config": {
    "patterns": [
      "../", "..\\", "..;/", "..;\\",
      "/etc/passwd", "/etc/shadow", ".env", ".git/",
      "' or '1'='1", "\" or \"1\"=\"1", "' or 1=1",
      "union select", "union all select", "; drop table", "xp_cmdshell", "sleep(",
      "<script", "</script", "javascript:", "onerror=", "onload=", "<svg/onload",
      "${jndi:", "/bin/sh", "/bin/bash", "cmd.exe"
    ]
  }
}
```

### Scenario B: Ultra-Minimal Edge Shield (Zero False Positive Risk)
*Goal: High-throughput public endpoints where payloads may contain arbitrary text/HTML (e.g., CMS/Markdown editors), protecting strictly against server filesystem traversal and Log4j/RCE.*

```json
{
  "id": "waf_lite_minimal",
  "phase": "pre_route",
  "config": {
    "patterns": [
      "../", "..\\", "/etc/passwd", ".env", ".git/", "${jndi:", "class.module.classloader"
    ]
  }
}
```

---

## 4. Observability & Troubleshooting Playbook

### Telemetry & Access Log Signals
- **Wire Response**: HTTP `403 Forbidden` (`b"403 Forbidden\n"`), gRPC `PermissionDenied ("Forbidden")`. No `X-WAF-*` headers are emitted.
- **Internal Access Log**: `completion=reject:waf_blocked`
- **Prometheus Metrics**: `velda_requests_rejected_total{reason="waf_blocked"}`

### Incident Triage

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| Legitimate API requests return `403 Forbidden` with `reason="waf_blocked"` | An overly short/generic signature in `patterns` (e.g., `"select"`, `"or"`, `"eval("`) matched a valid query parameter or JSON field. | Replace generic words with syntactic multi-token patterns (e.g., `"union select"`, `"' or 1=1"`). |
| Attack payload in a >32 KB upload was not blocked | `waf_lite` intentionally caps body scanning at `32,768` bytes to prevent CPU DoS on large file uploads. | Ensure large upload routes enforce strict `Content-Type` validation and backend deserialization limits. |
| Config reload failed at `velda-sync` | `patterns` contains an empty or whitespace-only entry (`""`, `"   "`). | Remove blank entries from the `patterns` array and re-publish. |
