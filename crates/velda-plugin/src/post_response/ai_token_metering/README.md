# AI Token Metering & Monetization Plugin (`ai_token_metering`)

Zero-allocation, wire-speed LLM inference token auditing, cost attribution, and consumer ledgering plugin evaluated during the Layer 7 `post_response` phase. Built strictly to conform with the OpenAI standard specification, it audits prompt, completion, total, and cached tokens across both Unary REST payloads and Server-Sent Events (SSE) streaming chunks.

---

## 1. What is `ai_token_metering`?

`ai_token_metering` is Velda Edge's canonical token telemetry and monetization engine for Generative AI and LLM API traffic. Operating in the `post_response` phase immediately following upstream LLM response completion, it extracts token quantities, evaluates precision micro-dollar pricing, and updates multi-tenant ledgers without inducing serving latency and without allocating heap memory.

Unlike traditional API gateways (such as Kong or Apache APISIX) that parse entire response bodies into heavy JSON Abstract Syntax Trees (AST) using dynamic languages (Lua/Python), `ai_token_metering` is architected for **Zero-Allocation Reverse Tail Window Scanning**, **Fixed-Point Integer Currency Math**, and **CPU Cache-Line Aligned Sharded State**.

---

## 2. Core Purpose & What It Does

1. **Standard Inference Usage Extraction**:
   Natively parses standard OpenAI-compatible usage envelopes (`"usage": { "prompt_tokens", "completion_tokens", "total_tokens", "prompt_tokens_details": { "cached_tokens" } }`) across both Unary REST JSON responses and trailing Server-Sent Events (SSE) chunks.
2. **Fixed-Point Micro-USD Billing**:
   Calculates exact monetary expenses in Micro-USD ($10^{-6}\text{ USD}$ or $\$0.000001$) using 128-bit unsigned integer arithmetic (`u128`), incorporating differential rates for base prompt tokens, cached prompt tokens, and completion tokens with zero floating-point rounding error.
3. **Multi-Tenant Sharded In-Memory Ledgering**:
   Records aggregated usage per tenant across 64 cache-line isolated ledger stripes (`#[repr(align(64))]`) in $< 10\ \text{ns}$ without lock contention or cross-core false sharing.
4. **Stack-Allocated Telemetry Header Injection**:
   Injects up to 7 downstream audit headers (`x-ai-model`, `x-ai-prompt-tokens`, `x-ai-completion-tokens`, `x-ai-total-tokens`, `x-ai-cached-tokens`, `x-ai-cost-micro-usd`, `x-ai-metering-status`) formatted directly on the thread stack via `itoa`.
5. **Asynchronous Distributed Redis Reconciliation**:
   Periodically aggregates local ledger stripe deltas and synchronizes them to distributed Redis via a dedicated companion thread (`velda-ai-sync`) running under Linux `SCHED_IDLE`.

---

## 3. Core Architectural Concepts ("Ý Tưởng Cốt Lõi")

```
[ Downstream Client ]
         │
         ▼
[ Frontline Traffic Worker (velda-worker-{id}) ]
  │
  ├─► Reverse Permission Check: header: Write, payload: Read
  │
  ├─► Reverse Tail Window Scanner (min 4KB tail scan in < 35ns, 0 Heap Alloc)
  │     • Matches b"\"usage\"" from buffer tail
  │     • Bypasses 95% of large prompt/completion context bodies
  │
  ├─► Micro-USD Integer Math (u128 Fixed-Point: No Floating-Point IEEE-754 Jitter)
  │     • Cost = ((T_prompt - T_cached)*Rp + T_cached*Rc + T_comp*Rm) / 1,000,000
  │
  ├─► 64-Stripe Sharded In-Memory Ledger (repr(align(64)), Lock < 10ns)
  │     • Zero False Sharing between CPU cores
  │
  └─► Stack itoa Header Injector (x-ai-* headers) ──► Downstream Wire Encoder
                                    │
                        (Periodic Background Drain)
                                    ▼
       [ Shadow Logistics Worker (velda-ai-sync, SCHED_IDLE) ]
                                    │ (Pipelined HINCRBY)
                                    ▼
                      [ Distributed Redis Server ]
```

### 3.1 Reverse Permission Granting (`PluginCapability`)
In accordance with Velda Edge's security and zero-copy invariants, `ai_token_metering` explicitly declares reverse permissions in Rust:
```rust
fn capability(&self) -> PluginCapability {
    PluginCapability::new(AccessLevel::Write, AccessLevel::Read)
}
```
- **`payload: AccessLevel::Read`**: Grants read-only inspection access to `resp.body.as_slice()`. The plugin never modifies body bytes.
- **`header: AccessLevel::Write`**: Grants permission to append `x-ai-*` telemetry headers to `resp.headers`.

### 3.2 Reverse Tail Window Scanning (< 4KB Tail Scan)
In generative AI inference, response bodies can range from 10KB to over 1MB. Traditional gateways parse the entire body from byte 0, allocating large AST trees.
- `ai_token_metering` exploits the OpenAI grammar invariant: the `"usage"` envelope is structured adjacent to the end of the response buffer.
- The scanner bounds inspection strictly to the trailing 4,096 bytes:
  $$\text{Scan Offset} = \max\left(0, \text{len} - 4096\right)$$
- Number parsing converts ASCII bytes directly to `u64` values in place. Execution completes in **$< 35\ \text{ns}$** with **$0\ \text{B}$ heap allocations**.

### 3.3 Micro-USD Fixed-Point Currency Math ($10^{-6}\text{ USD}$)
Floating-point arithmetic (`f32`/`f64`) produces non-deterministic rounding errors in high-volume billing.
- All pricing rates and accumulated balances are represented as integer Micro-USD ($1\ \mu\text{USD} = \$0.000001$).
- Intermediate calculations use `u128` registers. Even under $100{,}000{,}000$ tokens, products never overflow and divisions execute in $< 3\ \text{ns}$.

### 3.4 64-Stripe Cache-Line Aligned Ledger (Zero False Sharing)
- Multi-tenant ledgers are partitioned across 64 shards.
- Each shard is padded to 64 bytes (`#[repr(align(64))]`), matching host CPU L1/L2 cache line boundaries.
- Thread-per-core workers update adjacent consumer ledgers without invalidating each other's CPU cache lines.

### 3.5 Shadow Logistics Reconciler (Frontline Serving Independence)
- Frontline traffic workers **never** perform Redis network I/O or block on remote locks.
- A background companion thread (`velda-ai-sync`) running under `SCHED_IDLE` periodically sweeps atomic stripe deltas and flushes them to Redis via pipelined `HINCRBY` commands.

---

## 4. Non-Negotiable Invariants

| Invariant | Specification | Enforcement |
| :--- | :--- | :--- |
| **Zero-IO Hot Path** | Frontline workers never perform network calls, Redis writes, or blocking synchronization. | Enforced by architecture boundary; background thread owns Redis. |
| **Strict No-Fallback Auditing** | Missing catalog models are never assigned fallback rates. Status is set to `unpriced_model`, cost is `$0`, raw tokens are recorded. | Verified via unit tests (`test_plugin_unpriced_model_strict_no_fallback`). |
| **Reverse Tail Window** | Scanning bounds are capped at the final 4KB of the response buffer. Full-body AST parsing is strictly forbidden. | Enforced in `scanner.rs`. |
| **Zero Heap Allocation Hot Path** | Number extraction, model resolution, pricing arithmetic, and header injection allocate zero heap memory. | Verified via `perf stat` (0 bytes heap churn). |
| **Micro-USD Precision** | Currency math strictly uses integer fixed-point arithmetic on `u128` ($10^{-6}\text{ USD}$). Floating-point currency math is forbidden. | Enforced in `pricing.rs`. |

---

## 5. Documentation Navigation

- **[OPERATOR & CONFIGURATION RUNBOOK](RUNBOOK.md)**: Full JSON configuration schema, production model pricing catalogs, Redis distributed sync setup, Prometheus alerting rules, and memory sizing.
- **[ARCHITECTURAL SPECIFICATION](ARCHITECTURE.md)**: Detailed technical specification of the Reverse Tail Window scanner, fixed-point integer pricing algorithms, cacheline-aligned state sharding, and Redis reconciliation.
- **[BENCHMARK & PERFORMANCE SUITE](BENCHMARK.md)**: 9 comprehensive benchmark scenarios comparing Velda Edge against Kong and Apache APISIX on Microsoft Azure, longevity tests, and CPU symbol call graphs.
- **[TEST SUITE SPECIFICATION](TEST.md)**: Unit tests, integration test suites, fuzzing test cases, and chaos failure scenarios.
