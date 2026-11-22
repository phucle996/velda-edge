# Web Application Firewall Lite Plugin (`waf_lite`)

```text
+-----------------------------------------------------------------------------------+
|                        WAF Lite Pre-Route Architecture                            |
|                                                                                   |
|  [Inbound L7 Request] (HTTP/1.1, HTTP/2, HTTP/3)                                  |
|         │                                                                         |
|         ▼                                                                         |
|  ┌─────────────────────────────────────────────────────────────────────────────┐  |
|  │ Frontline Traffic Worker (velda-worker-{i}) — Pre-Route Phase               │  |
|  │  - Empty Patterns Fast-Path: 3 instructions (cmpb + je -> Action::Continue) │  |
|  │  - 1. URI Path  (req.uri.path().as_bytes(),  convert_plus = false)          │  |
|  │  - 2. URI Query (req.uri.query().as_bytes(), convert_plus = true)           │  |
|  │  - 3. Body Slice (Branchless cmovbq cap <= 32,768 bytes)                    │  |
|  └──────────────────────────────────────┬──────────────────────────────────────┘  |
|                                         │                                         |
|                     [Fast-Path: needs_normalization()]                            |
|                      /                               \                            |
|                    NO (Zero-Copy)                    YES (Anti-Evasion)           |
|                    /                                   \                          |
|         Direct AhoCorasick::find()             1. 3-pass %XX decode (in-place)    |
|                                                2. Stage A scan (pre-strip)        |
|                                                3. Bounded SQL comment folding     |
|                                                4. Stage B scan (post-strip)       |
|                                         │                                         |
|                             [Pattern Matched?]                                    |
|                              /              \                                     |
|                            YES               NO                                   |
|                            /                   \                                  |
|         Action::Reject(WafBlocked)        Action::Continue                        |
|         (Wire: 403 Forbidden generic,     (Proceed to Routing / Upstream)         |
|          Telemetry: waf_blocked)                                                  |
+-----------------------------------------------------------------------------------+
```

The **`waf_lite`** plugin is Velda Edge's zero-regex, config-driven Layer 7 perimeter firewall. It inspects request URI paths, query strings, and the leading 32 KB of request payloads against user-supplied attack signatures using SIMD-accelerated Aho-Corasick matching before route lookup or upstream connection leasing.

---

## 1. What This Plugin Does

1. **100% Config-Driven (WYCIWIS — What You Configure Is What It Scans)**:
   - Ships with zero hidden built-in signatures or opaque dictionaries. Only patterns explicitly supplied in `patterns` are compiled and enforced.
2. **Unified URI & Capped Payload Inspection**:
   - Evaluates URI Path, URI Query, and the first **32 KB (`32,768` bytes)** of the request body via branchless capping (`cmovbq`), protecting CPU cores against large-upload DoS while catching leading injection payloads.
3. **Zero-Copy Fast Path & Anti-Evasion Normalization**:
   - Clean ASCII requests bypass decoding and scan directly on raw `&[u8]` slices with zero heap allocations.
   - Obfuscated requests undergo up to 3 passes of in-place `%XX` decoding, `+` $\to$ space conversion, and two-stage SQL comment folding (`/*...*/`, `--...\n`, `#...\n`).
4. **Zero Fingerprinting Leakage**:
   - Always rejects matches with `Action::Reject(RejectReason::WafBlocked)`. On the wire, clients receive a generic HTTP `403 Forbidden` with zero `X-WAF-*` headers, while internal telemetry records `waf_blocked`.

---

## 2. Where It Sits in Velda Edge

- **Subsystem**: `crates/velda-plugin/src/pre_route/waf_lite/`
- **Execution Stage**: `PreRoutePlugin` (compile-time fixed)
- **Runtime Placement**: Executed inline on Frontline Traffic Workers (`velda-worker-{i}`) immediately after L7 ingress decode, before routing and body streaming.
- **Recommended Pipeline Order**: `ip_filter` $\to$ `resource_limit` $\to$ `cors_preflight` $\to$ `waf_lite`.

---

## 3. Requirements & Prerequisites

1. **Explicit Signature Supply**: Because built-in dictionaries are omitted by design, an empty `patterns` list acts as a no-op pass-through. Populate `patterns` using the recommended starter dictionaries in [RUNBOOK.md](./RUNBOOK.md).
2. **Syntactic Patterns**: Prefer multi-token syntactic signatures (e.g., `"union select"`, `"<script"`, `"../"`, `"${jndi:"`) over short generic words to avoid false positives on URL tokens.

---

## 4. Quick Start Configuration

Declare the plugin in your `plugins.json`:

```json
{
  "id": "waf_lite",
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

---

## 5. Documentation Directory

- **[RUNBOOK.md](./RUNBOOK.md)**: Configuration reference, recommended OWASP/CRS starter dictionaries (Path Traversal, SQLi, XSS, RCE), production presets, and incident troubleshooting.
- **[ARCHITECTURE.md](./ARCHITECTURE.md)**: Deep internals (SIMD Aho-Corasick DFA, $\mathcal{O}(N)$ / $\mathcal{O}(1)$ complexity proofs, monotonic in-place pointer invariant, 40 B stack frame & L1 cache analysis).
- **[BENCHMARK.md](./BENCHMARK.md)**: Standardized 5-tier diagnostic benchmark matrix across 6 scenarios and verified `x86-64-v3` release assembly audit.
- **[TEST.md](./TEST.md)**: Unit, anti-evasion, and validation test matrix.
