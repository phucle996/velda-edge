# Performance Benchmark Report: JWT Authentication Plugin (`jwt_auth`)

Performance specification and compiler-verified `x86-64-v3` release assembly audit for the `jwt_auth` pre-upstream plugin.

---

## 1. Test Environment & Standardized 5-Tier Metric Taxonomy

### 1.1 Hardware & Network Testbed

*[TBD]* — Live end-to-end cluster throughput measurements remain pending until execution on dedicated benchmark nodes. Compiler-verified release assembly (`x86-64-v3`) is audited in Section 4.

### 1.2 5-Tier Diagnostic Matrix

1. **Tier 1 (Macro Network & L7 Delivery)**: Duration (s), Total Requests, Throughput (req/s), Wire Bandwidth (MB/s), Mean/P50/P75/P90/P99/P99.9/Max Latency (ms), Status Code Distribution (`200` / `401` / `403`), Success Rate (%).
2. **Tier 2 (System Resource Footprint)**: Total/User/System/SoftIRQ CPU (%), Frontline Worker CPU (%), Shadow Worker CPU (%), Peak RSS (MB), Heap Allocations / Request.
3. **Tier 3 (Kernel & OS Telemetry via `perf stat`)**: System Calls / Request (`0` clock syscalls via `req.ingress_time_sec()`), Context Switches / Request, Futex Calls.
4. **Tier 4 (CPU Microarchitecture Counters via `perf stat`)**: Instructions / Request, Cycles / Request, IPC, Branch Misprediction Rate (%), L1D / L2 / L3 LLC Hit & Miss Rates (%).
5. **Tier 5 (Plugin Mechanics)**: `JwtCache::token_to_u128` SHA-256 cycles, `_MM_HINT_T0` / `_MM_HINT_T2` overlap effectiveness, 64-byte AVX2 `TagLine` scan latency, `JwksKeyTable::resolve_key` Cache Line 0 lookup cycles, and cold `ring` signature verification latency (`HS256` vs `EdDSA` vs `ES256` vs `RS256`).

---

## 2. Benchmark Scenarios

- **Scenario 01 — L3 SIMD Positive Cache Hit (`cache_ttl_secs: 300`)**: Measure steady-state RPS and P99 latency when active tokens hit the 64-way L3 SIMD bucket and project 3 claims via `insert_prehashed` (`0` heap allocations).
- **Scenario 02 — Cold Cryptographic Verification (`cache_ttl_secs: 0`, `HS256` / `EdDSA` / `ES256` / `RS256`)**: Measure raw per-request cryptographic verification throughput across algorithm families matching Slot 0 of `JwksKeyTable`.
- **Scenario 03 — Multi-Key Rotation Traversal (`Slot 0` vs `Slot 7`)**: Compare verification latency across 8 active JWKS slots within Cache Line 0 of `JwksKeyTable`.
- **Scenario 04 — Malformed Garbage Flood (2-Dot Structural Pre-Filter)**: Measure early `401` rejection throughput on random non-JWT strings dropped before SHA-256 or L3 cache access.
- **Scenario 05 — Forged Signature & Unauthorized Role Flood (Unified Bit-7 Reject Cache)**: Measure CPU protection when invalid signatures (`401`) and failed `assert_claims` (`403`) hit the unified L3 SIMD reject bit (`0x80 | tag7`).
- **Scenario 06 — Live `jwks_rotator` Key Rotation Under Sustained Traffic**: Measure frontline P99 tail stability while `jwks_rotator` updates `SharedJwksStore` and increments `epoch` on a Shadow Logistics Worker (`SCHED_IDLE`).

---

## 3. Macro Cluster Benchmark Results

| Metric | Scenario 01 | Scenario 02 | Scenario 03 | Scenario 04 | Scenario 05 | Scenario 06 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Throughput (req/s)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Median Latency P50 (ms)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Tail Latency P99 (ms)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions / Request** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **L3 LLC Hit Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

## 4. Verified Release Assembly & Symbol Audit (`x86-64-v3`)

Audited directly from compiler output emitted with:

```bash
cargo rustc --release -p velda-plugin --lib -- --emit=asm -C llvm-args=-x86-asm-syntax=intel -C target-cpu=x86-64-v3
```

### Symbol Profile

- **Artifact**: `target/release/deps/velda_plugin-d4e3c06c5e5398a9.s`
- **Compiler**: `rustc 1.98.0 (88d9e12ae 2026-08-18)`
- **Hot-Path Symbol**: `_RNvXs_NtNtNtCs43Oa2zYOuG3_12velda_plugin12pre_upstream8jwt_auth6pluginNtB4_13JwtAuthPluginNtB8_17PreUpstreamPlugin19on_upstream_request` (lines `710113..=711708`)

### Observed Instruction Invariants

| Audit Check | Status | Verified Evidence in Emitted Assembly |
| :--- | :---: | :--- |
| **Zero Heap Allocation in `on_upstream_request`** | **Verified** | `0` occurrences of `__rust_alloc` across lines `710113..=711708` (all cached positive/negative/forbidden paths and header injections execute with zero heap allocation). |
| **L1D Control Line Prefetch (`_MM_HINT_T0`)** | **Verified** | Emits `prefetcht0 byte ptr [rsi]` at line `710137` on the 64-byte `JwtCache` header at offset `0x00` of `JwtAuthPlugin`. |
| **BMI2 Bit-Shift Indexing (`shrx`)** | **Verified** | Emits 2 `shrx` single-cycle variable bit-shifts extracting `group_idx = (fp >> group_shift) & group_mask` without division or branches. |
| **L3 Bucket Prefetch Before Lock (`_MM_HINT_T2`)** | **Verified** | Emits `prefetcht2 byte ptr [rax + r12]` (lines `710940`, `711215`) on `&shard.groups_base[group_idx].tags` ahead of mutex acquisition. |
| **AVX2 64-Byte `TagLine` & Pre-Hashed Header Scan** | **Verified** | Emits `6` `vpand`, `8` `vpcmpeqb`, `8` `vpmovmskb`, `5` `tzcnt`, and `4` `blsr` instructions scanning 64-slot `TagLine` cache lines and pre-hashed `HeaderKey` slots. |
