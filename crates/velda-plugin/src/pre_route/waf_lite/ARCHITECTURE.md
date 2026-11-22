# Web Application Firewall Lite (`waf_lite`) — Deep Architecture Specification

This document specifies the low-level memory layout, deterministic finite automaton (DFA) execution, anti-evasion invariants, and machine-code properties of **`waf_lite`**.

---

## 1. Execution Pipeline & Subsystem Boundaries

```text
+─────────────────────────────────────────────────────────────────────────────+
|                       WAF_LITE HOT-PATH ARCHITECTURE                        |
+─────────────────────────────────────────────────────────────────────────────+
|                                                                             |
|  WafLitePlugin::on_request(&self, req: &mut L7Request)                      |
|     │  [Stack Frame: 40 B (5 callee-saved pushes, 0 subq)]                  |
|     │                                                                       |
|     ├─► [matcher.pattern_ac.is_none()?] ──► YES (cmpb $-1 + je) ──► Continue|
|     │                                                                       |
|     ├─► 1. URI Path  (req.uri.path().as_bytes(),  convert_plus = false)     |
|     ├─► 2. URI Query (req.uri.query().as_bytes(), convert_plus = true)      |
|     └─► 3. Body Cap  (min(req.body.len(), 32768) via branchless cmovbq)     |
|                                │                                            |
|                                ▼                                            |
|                     WafMatcher::check_bytes(slice)                          |
|                                │                                            |
|                  [needs_normalization(slice) == false?]                     |
|                     /                                \                      |
|                   YES                                 NO                    |
|                   /                                     \                   |
|        Zero-Copy Direct                       In-Place Normalization        |
|        AhoCorasick::find(slice)               (Stack [u8; 1024] or <=32KB)  |
|                                               1. decode_percent_multi_pass  |
|                                               2. Stage A: AC::find(decoded) |
|                                               3. compact_whitespace_comments|
|                                               4. Stage B: AC::find(compact) |
|                                │                                            |
|                       Matched == true?                                      |
|                         /          \                                        |
|                       YES           NO                                      |
|                       /               \                                     |
|    Action::Reject(WafBlocked)     Action::Continue                          |
+─────────────────────────────────────────────────────────────────────────────+
```

---

## 2. Algorithmic & Memory Complexity Proofs

### 2.1 $\mathcal{O}(N)$ Serving Complexity Independent of Rule Count $K$
- During configuration compilation (`WafMatcher::new`), all $K$ normalized patterns (total byte length $M = \sum |p_i|$) are sorted (`sort_unstable`), deduplicated (`dedup`), and compiled into a single **SIMD-accelerated Aho-Corasick DFA** (Teddy vector matching on x86-64).
- On the request hot path, each byte $b_i$ of input length $N$ triggers $\mathcal{O}(1)$ state transitions. Total scan cost is $\mathcal{O}(N)$, independent of $K$.

### 2.2 Anti-DoS Payload Window Bounding ($\mathcal{O}(1)$ Upper Bound)
- Request body inspection is strictly bounded by `MAX_BODY_SCAN_BYTES = 32_768` (32 KB):
  $$\text{ScanLen}(L) = \min(L, 32\,768) \implies T_{\text{body}}(L) \le \alpha \cdot 32\,768 = \mathcal{O}(1)$$
- Compiled as a single branchless `cmpq $32768, %rsi` + `cmovbq %rsi, %rdx` sequence on x86-64.

---

## 3. Anti-Evasion Normalization Mechanics

### 3.1 Monotonic In-Place Pointer Invariant ($r_t \ge w_t$)
In `decode_percent_multi_pass`, `%XX` (3 bytes) decodes to 1 byte, while literal characters copy 1-to-1. By induction:
- Base: $r_0 = 0 \ge w_0 = 0$.
- Step: Either $(r_{t+1}, w_{t+1}) = (r_t + 3, w_t + 1)$ or $(r_t + 1, w_t + 1)$, preserving $r_t \ge w_t$.
- Therefore, writing `out[w] = b` **never overwrites unread input** `out[r..]`, permitting up to **3 nested decoding passes** (`%25252e` $\to$ `%252e` $\to$ `%2e` $\to$ `.`) strictly in-place on a single buffer (`[0u8; 1024]` on stack for URI/small payloads).

### 3.2 Two-Stage Comment Scanning & Delimiter Barriers
- **Stage A (Pre-Strip)**: Scans percent-decoded, ASCII-lowercased bytes *before* comment folding so payloads wrapped inside SQL comments (`/*<script>*/`, `/*${jndi:...}*/`) are caught immediately.
- **Stage B (Post-Strip)**: Collapses SQL inline comments (`/*...*/`, preserving MySQL executable `/*!`) and terminated line comments (`--...\n`, `#...\n`) into a single space, catching fragmented SQLi (`UNION/**/SELECT`).
- **Smuggling Barrier**: Comment scanning aborts upon encountering `&`, `<`, or `$`, preventing cross-parameter comment smuggling (`?a=/*&id=union+select&b=*/`).

---

## 4. Zero Information Leakage & Cache Footprint

- **Wire Indistinguishability**: `RejectReason::WafBlocked` (discriminant `17`) maps in `REJECT_STATIC_METAS` to `(403, "14", b"403 Forbidden\n")` and gRPC `PermissionDenied ("Forbidden")` — bit-for-bit identical to `RejectReason::Forbidden` (`2`) and `RejectReason::IpBlocked` (`18`). Mutual information leaked to external attackers is $I(\text{Source}; Y) = 0\text{ bits}$.
- **L1 Cache Footprint (`x86-64-v3`)**: `WafLitePlugin::on_request` uses a `40 B` stack frame (1 L1d cache line) and `178` ASM lines (~10 L1i cache lines), with 0 heap allocations (`alloc::*` = 0).
