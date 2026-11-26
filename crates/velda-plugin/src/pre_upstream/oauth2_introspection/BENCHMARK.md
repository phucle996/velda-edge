# Performance Benchmark Report: OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`)

Performance benchmark specification and compiler-verified `x86-64-v3` release assembly audit for the redesigned `oauth2_introspection` pre-upstream plugin.

---

## 1. Test Environment & Standardized 5-Tier Metric Taxonomy

### 1.1 Hardware & Network Testbed

*[TBD]* — Live end-to-end cluster throughput measurements remain pending until execution on dedicated benchmark nodes. Previous pre-refactor measurements have been cleared; compiler-verified release assembly (`x86-64-v3`) is audited in Section 4.

### 1.2 Standardized 5-Tier Diagnostic Matrix

1. **Tier 1 (Macro Network & L7 Delivery)**: Duration (s), Total Requests, Throughput (req/s), Wire Bandwidth (MB/s), Mean / P50 / P75 / P90 / P99 / P99.9 / Max Latency (ms), HTTP Status Code Distribution (`200` / `401` / `403` / `502`), Success Rate (%).
2. **Tier 2 (System Resource Footprint)**: Total / User / System / SoftIRQ CPU (%), Frontline Worker CPU (%), Shadow Worker CPU (%), Peak RSS (MB, strictly bounded below 10% host RAM ceiling), Heap Allocations / Request.
3. **Tier 3 (Kernel & OS Telemetry via `perf stat`)**: System Calls / Request, Context Switches / Request, Futex Calls / % of Total Syscalls.
4. **Tier 4 (CPU Microarchitecture Counters via `perf stat`)**: Instructions / Request, Cycles / Request, IPC, Branch Misprediction Rate (%), L1D / L2 / L3 LLC Hit & Miss Rates (%).
5. **Tier 5 (Plugin Mechanics)**: `IntrospectionCache::token_to_u128` SHA-256 latency, `_MM_HINT_T0` / `_MM_HINT_T2` overlap effectiveness, AVX2 64-way `TagLine` scan latency, `InFlightCoordinator` coalescing efficiency on concurrent cold misses, and cold `IdpKeepAliveClient` HTTPS Keep-Alive latency.

---

## 2. Benchmark Scenarios

- **Scenario 01 — Warm L3 SIMD Cache Hit (`cache_ttl_seconds: 300`)**: Measure synchronous `try_on_upstream_request` RPS, P99 latency, and zero heap allocation across `require_scopes` + `claims_to_headers` injection.
- **Scenario 02 — Stale-While-Revalidate Under Sustained Load (`cache_ttl_seconds: 30`, `3s` lead window)**: Measure frontline P99 latency isolation while `velda-shadow-{id}` refreshes active tokens asynchronously over HTTPS Keep-Alive.
- **Scenario 03 — Concurrent Cold Miss Single-Flight Stampede Protection**: Fire bursts of concurrent connections carrying identical unseen opaque tokens to verify `InFlightCoordinator` coalesces duplicate IdP queries.
- **Scenario 04 — High-Cardinality Saturation Under 10% Host RAM Ceiling**: Flood unique tokens past initial shard allocation to verify seamless power-of-two `grow_shard_doubling` up to 10% RAM followed by bounded O(1) clock-hand eviction with zero OOM or latency spikes.
- **Scenario 05 — Negative & Insufficient-Scope Flood (`401` / `403` Unified Tag Bit 7)**: Verify sub-20ns L3 rejection throughput when replaying revoked (`active: false`) or under-scoped tokens.

---

## 3. Macro Cluster Benchmark Results

| Metric | Scenario 01 | Scenario 02 | Scenario 03 | Scenario 04 | Scenario 05 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Throughput (req/s)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Median Latency P50 (ms)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Tail Latency P99 (ms)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions / Request** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **L3 LLC Hit Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

## 4. Verified Release Assembly & Symbol Audit (`x86-64-v3`)

Audited directly from compiler output emitted with:

```bash
cargo rustc --release -p velda-plugin --lib -- --emit=asm -C llvm-args=-x86-asm-syntax=intel -C target-cpu=x86-64-v3
```

### Symbol Profile

- **Artifact**: `target/release/deps/velda_plugin-8b8e94ce6cfd17cf.s`
- **Compiler**: `rustc 1.98.0 (88d9e12ae 2026-08-18)`
- **Warm Entry Symbol**: `_RNvXs2_NtNtNtCsivalMcbv5lH_12velda_plugin12pre_upstream20oauth2_introspection6pluginNtB5_25Oauth2IntrospectionPluginNtB9_17PreUpstreamPlugin23try_on_upstream_request` (lines `708557..=708749`)
- **L3 Cache Lookup Symbol**: `_RNvMs2_NtNtNtCsivalMcbv5lH_12velda_plugin12pre_upstream20oauth2_introspection5cacheNtB5_18IntrospectionCache6lookup` (lines `458163..=458416`)

### Observed Instruction Invariants

| Audit Check | Status | Verified Evidence in Emitted Assembly |
| :--- | :---: | :--- |
| **Zero Heap Allocation on Warm Path** | **Verified** | `0` calls to `__rust_alloc` in both `try_on_upstream_request` (`708557..=708749`) and `IntrospectionCache::lookup` (`458163..=458416`). |
| **L1D Control Prefetch (`_MM_HINT_T0`)** | **Verified** | Emits `prefetcht0 byte ptr [r12 + 64]` at line `708583` on entry to `try_on_upstream_request`. |
| **BMI2 Bit-Shift Indexing (`shrx`)** | **Verified** | Emits `shrx r15, r14, rcx` at line `458201` computing `group_idx = (fp >> shard_shift) & group_mask`. |
| **L3 Bucket Prefetch Before Lock (`_MM_HINT_T2`)** | **Verified** | Emits `prefetcht2 byte ptr [rcx + r10]` at line `458207` ahead of acquiring shard `RwLock::read()`. |
| **AVX2 64-Byte `TagLine` Scan & Lane Pop** | **Verified** | Emits `2` `vpand`, `2` `vpcmpeqb` (`ymm2`, `ymm0`), `2` `vpmovmskb`, `tzcnt rax, r9`, and `blsr r9, r9` (`L458234..=L458256`). |
