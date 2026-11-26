# Performance Benchmark Report: Header Mutation Plugin (`header_mutation`)

Performance benchmark specification and verified `x86-64-v3` release assembly audit for the `header_mutation` pre-upstream plugin.

---

## 1. Test Environment & Standardized 5-Tier Metric Taxonomy

### 1.1 Production-Grade Hardware & Network Testbed

*[TBD]* — End-to-end cluster load measurements remain pending until benchmark execution on dedicated hardware. Verified `rustc 1.98.0` release assembly characteristics (`x86-64-v3`) are documented in Section 4 below.

### 1.2 5-Tier Diagnostic Matrix

1. **Tier 1 (Macro Network & L7 Delivery)**: Duration (s), Total Requests, Throughput (req/s), Wire Bandwidth (MB/s), Mean/P50/P75/P90/P99/P99.9/Max Latency (ms), Latency StdDev (ms), Status Code Distribution, Success Rate (%).
2. **Tier 2 (System Resource Footprint)**: Total/User/System/SoftIRQ CPU (%), Frontline Worker CPU (%), Shadow Worker CPU (%), Peak RSS (MB), Heap Allocations / Request.
3. **Tier 3 (Kernel & OS Telemetry via `perf stat`)**: System Calls / Request, Context Switches / Request, Futex Calls.
4. **Tier 4 (CPU Microarchitecture Counters via `perf stat`)**: Instructions / Request, Cycles / Request, IPC, Branch Mispredictions, Branch Misprediction Rate (%), L1D / LLC Cache Miss Rate (%).
5. **Tier 5 (Plugin Mechanics)**: `HeaderMutationPlugin::on_upstream_request` latency (ns), `contains_prehashed` SIMD tag probe cycles, `insert_prehashed` slot update vs tail append cycles, `HeaderWorkspace` capacity transitions.

---

## 2. Benchmark Scenarios

- **Scenario 01 — Baseline Static Set (1 Header)**: Single `set_headers` entry inserted onto an existing `HeaderWorkspace`.
- **Scenario 02 — Remove Hit + Miss Mix**: 4 `remove_headers` entries evaluated against a 16-header request where 2 match (including 1 `TAG_BYTE_BYPASS_BIT` slot) and 2 miss via `contains_prehashed`.
- **Scenario 03 — Combined Remove / Set / Append**: Simultaneous `remove_headers` (2), `set_headers` (3), and `append_headers` (1).
- **Scenario 04 — High-Header Workspace Compaction**: Removing 4 headers from a 64-header `HeaderWorkspace`.
- **Scenario 05 — Bypass-Tagged Overwrite (`x-request-id` / `host`)**: Overwriting pre-populated bypass-tagged slots via `insert_prehashed`.

---

## 3. Macro Cluster Benchmark Results

| Metric | Scenario 01 | Scenario 02 | Scenario 03 | Scenario 04 | Scenario 05 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Throughput (req/s)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Median Latency P50 (ms)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Tail Latency P99 (ms)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions / Request** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **IPC** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

## 4. Verified Release Assembly & Symbol Audit (`x86-64-v3`)

Audited directly from compiler output emitted with:

```bash
cargo rustc --release -p velda-plugin --lib -- --emit=asm -C llvm-args=-x86-asm-syntax=intel -C target-cpu=x86-64-v3
```

### Symbol Profile

- **Artifact**: `target/release/deps/velda_plugin-d4e3c06c5e5398a9.s`
- **Compiler**: `rustc 1.98.0 (88d9e12ae 2026-08-18)`
- **Target Symbol**: `_RNvXs_NtNtNtCs43Oa2zYOuG3_12velda_plugin12pre_upstream15header_mutation6pluginNtB4_20HeaderMutationPluginNtB8_17PreUpstreamPlugin19on_upstream_request` (lines `705548..=705855`, `304` assembly lines including directives/CFI)

### Observed Instruction Invariants

| Audit Check | Status | Evidence in Emitted Assembly |
| :--- | :---: | :--- |
| **`op_mask` Offset 0 Guard** | **Verified** | `movzx eax, byte ptr [rsi]; test al, al; je .LBB2459_26` reads `op_mask` directly at base pointer `[rsi + 0]`. |
| **SIMD `tag7` Pre-Hashed Scan (`OP_REMOVE`)** | **Verified** | Uses AVX2 `vpbroadcastd` (`0x7F`), `vpand`, `vpcmpeqb`, `vpmovmskb` + BMI1/BMI2 `shlx`, `tzcnt`, `blsr` (`32` header slots/iteration). |
| **Conditional Remove Only on Tag+Name Match** | **Verified** | `HeaderWorkspace::remove_by_name` is invoked only inside `.LBB2459_14` after SIMD `tag7` and `HeaderName` equality pass. |
| **Direct `OP_SET` Pre-Hashed Call** | **Verified** | Passes stored `tag7` directly in `edx` into `HeaderWorkspace::insert_internal_tag7` (`bypass = 0`). |
| **Direct `OP_APPEND` Call** | **Verified** | Calls `HeaderWorkspace::append_internal` directly when `(op_mask & 4) != 0`. |
| **Zero Function-Local Heap Allocator / TLS Calls** | **Verified** | Zero calls to `__rust_alloc`, `handle_alloc_error`, or `__tls_get_addr` anywhere inside `on_upstream_request`. |
