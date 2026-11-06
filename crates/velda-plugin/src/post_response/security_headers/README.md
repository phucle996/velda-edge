# Security Response Headers Plugin (`security_headers`)

Client-side browser hardening, modern origin isolation (COOP, COEP, CORP), and OWASP L7 hygiene plugin for outgoing HTTP responses. Automatically injects pre-compiled security headers instructing user-agents to activate internal sandbox defenses against Man-In-The-Middle (MITM), Clickjacking, MIME-type confusion, Cross-Site Scripting (XSS), cross-origin information leakage, and microarchitectural side-channel attacks (Spectre / Meltdown).

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: Every configuration parameter must be declared explicitly in `plugins.json`. No silent fallbacks, implicit defaults, or hidden behaviors are permitted.
>
> **Explicit 'none' / null Sentinel**: When a security header is intentionally omitted or disabled, declare `"none"` or `null` explicitly in the configuration.
>
> **Zero-Allocation Hot Path**: All `HeaderName` and `HeaderValue` pairs are pre-computed at initialization time into an inline flat contiguous array (`[(HeaderName, HeaderValue); 12]`) aligned to 64-byte L1 CPU cacheline boundaries (`#[repr(align(64))]`). Serving a response executes a contiguous iteration with zero heap allocations and zero pointer indirection.

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `security_headers` |
| **Lifecycle Phase** | `post_response` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Hot Path Latency** | ~5ns to 12ns (Pre-baked inline array, O(1) atomic ref-count clone) |
| **Hardware Alignment** | Aligned to 64-byte L1 CPU cacheline boundary (`#[repr(align(64))]`) |

---

## 2. Configuration & Operational Runbook

For complete declarative manifests (`plugins.json`), route attachments (`routes.json`), validation invariants, and production operational playbooks, see:
- 📖 **[Operational & Configuration Runbook](RUNBOOK.md)**

---

## 3. Defense Invariants & Threat Models

1. **HSTS (RFC 6797)**: Prevents SSL-stripping downgrade attacks by enforcing HTTPS for a declared duration.
2. **X-Content-Type-Options (RFC 7231)**: Sets `nosniff` to disable browser MIME sniffing, preventing malicious scripts disguised as images from executing.
3. **X-Frame-Options (RFC 7034)**: Disables or restricts `<iframe>` embedding (`DENY` or `SAMEORIGIN`), neutralizing UI Redressing and Clickjacking exploits.
4. **Referrer-Policy (W3C)**: Restricts referrer leakage to external origins, preventing tokens and query parameters from exposing in logs.
5. **Content-Security-Policy (CSP W3C Level 3)**: Restricts authorized domains for scripts, styles, images, and child frames.
6. **Permissions-Policy (W3C)**: Disables unused browser hardware capabilities (camera, microphone, geolocation).
7. **COOP (RFC / W3C)**: Enforces process-level browsing context group isolation, preventing cross-window Spectre attacks.
8. **COEP (RFC / W3C)**: Mandates that cross-origin subresources explicitly opt-in to being embedded, securing high-precision timers and `SharedArrayBuffer`.
9. **CORP (W3C)**: Defends sensitive API endpoints and static assets from being read by cross-origin web apps.
10. **X-Permitted-Cross-Domain-Policies**: Locks down Adobe Flash and PDF cross-domain access policies to `"none"`.
11. **X-XSS-Protection**: Explicitly set to `"0"` per OWASP standards to prevent legacy reflective XSS auditors from being weaponized.

---

## 4. Hardware Optimizations & CPU Architecture

- **Single-Instruction Configuration Bitmask (`op_mask: u16`)**: Pre-compiled 16-bit ALU integer encoding all active security directives. A single `TEST AX, imm16` check verifies header activation in $\le 1$ CPU cycle.
- **Dual-Representation & Hardware Register Separation**: Small headers ($\le 16$ bytes like `nosniff`, `DENY`, `require-corp`, `0`) are injected directly as 128-bit hardware integer registers into `HeaderWorkspace`, requiring only 1 CPU `MOV` instruction into L1 cache without heap allocations or HeaderMap lookups. Large headers ($> 16$ bytes like CSP) use zero-copy string pointers in RAM.
- **64-Byte Cacheline Boundary Alignment (`#[repr(align(64))]`)**: Aligns the plugin struct to modern 64-byte L1 CPU cacheline boundaries, completely preventing false sharing across pinned worker threads under high multi-core load.
- **Flat Workflow Invariant**: Sequential linear execution from index 0 to `headers_len` on the hot path without arbitrary function calls or branching.

---

## 5. Documentation Suite Links

- **[Architectural Specification](ARCHITECTURE.md)**: Internal data structures, L1 cacheline alignment, memory layout, and zero-allocation execution.
- **[Operational Runbook](RUNBOOK.md)**: Production deployment guide, troubleshooting matrix, CSP debugging, and monitoring.
- **[Performance Benchmark Report](BENCHMARK.md)**: Live Azure bare-metal AMD EPYC 7763 latency measurements, RPS throughput, and hardware counters.
- **[Test Plan & Security Verification](TEST.md)**: Integration test suite, test scenarios, and defense validation.
