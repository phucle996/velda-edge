# Performance Benchmark Report: HMAC Authentication Plugin (`hmac_auth`)

Performance specification and verified `x86-64-v3` release assembly audit for the rewritten `hmac_auth` pre-upstream plugin.

---

## 1. Test Environment & Standardized 5-Tier Metric Taxonomy

### 1.1 Hardware & Network Testbed

*[TBD]* — Live end-to-end cluster throughput measurements remain pending until benchmark execution on dedicated hardware. Compiler-verified release assembly (`x86-64-v3`) is audited in Section 4.

### 1.2 5-Tier Diagnostic Matrix

1. **Tier 1 (Macro Network & L7 Delivery)**: Duration (s), Total Requests, Throughput (req/s), Wire Bandwidth (MB/s), Mean/P50/P75/P90/P99/P99.9/Max Latency (ms), Latency StdDev (ms), Status Code Distribution (`200` / `401`), Success Rate (%).
2. **Tier 2 (System Resource Footprint)**: Total/User/System/SoftIRQ CPU (%), Frontline Worker CPU (%), Shadow Worker CPU (%), Peak RSS (MB), Heap Allocations / Request.
3. **Tier 3 (Kernel & OS Telemetry via `perf stat`)**: System Calls / Request, Context Switches / Request, Futex Calls / % of Total Syscalls.
4. **Tier 4 (CPU Microarchitecture Counters via `perf stat`)**: Instructions / Request, Cycles / Request, IPC, Branch Mispredictions, Branch Misprediction Rate (%), L1D / LLC Cache Miss Rate (%).
5. **Tier 5 (Plugin Mechanics)**: `CredentialTable::lookup` latency (`pext` / byte-shift vs `RadixBuckets`), `HeaderWorkspace::get_prehashed` cycles, streaming `hmac::Context` sign latency, `NonceCache::check_and_record_fp` shard lock contention, `x-consumer-id` overwrite/scrub cycles.

---

## 2. Benchmark Scenarios

- **Scenario 01 — Baseline Header-Only HMAC-SHA256 (`validate_request_body: false`, `enforce_nonce: false`)**: Verify signature over method, path, empty query, and 3 signed headers with 1–8 static credentials.
- **Scenario 02 — Full Body SHA-256 + Single-Lock Nonce (`validate_request_body: true`, `enforce_nonce: true`)**: Full canonical signing on buffered JSON request bodies with `NonceCache` commit after verification.
- **Scenario 03 — Multi-Credential Scaling (`64` credentials, BitGather `_pext_u64` dispatch)**: Verify `CredentialTable` lookup stability across shared-prefix keys (`partner_*`).
- **Scenario 04 — Forged Signature Flood (Stateless Early Reject)**: Verify `reject_unauthorized` path before `NonceCache` mutex acquisition under invalid signature load.
- **Scenario 05 — Complex Query Parameter Canonicalization (`<= 16` vs `> 16` params)**: Stack sort path vs heap overflow fallback.

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
- **Hot-Path Symbol**: `_RNvXs_NtNtNtCs43Oa2zYOuG3_12velda_plugin12pre_upstream9hmac_auth6pluginNtB4_14HmacAuthPluginNtB8_17PreUpstreamPlugin19on_upstream_request` (lines `709729..=713463`)
- **Cold Reject Symbol**: `_RNvMNtNtNtCs43Oa2zYOuG3_12velda_plugin12pre_upstream9hmac_auth6pluginNtB2_14HmacAuthPlugin19reject_unauthorized` (`.text.unlikely`, lines `362038..=362122`)

### Observed Instruction Invariants

| Audit Check | Status | Evidence in Emitted Assembly |
| :--- | :---: | :--- |
| **BMI2 `pext` Credential Dispatch** | **Verified** | Emits hardware `pext rdi, rdx, qword ptr [r8 + 32]` for `BitGather` alongside `shl`/`shlx` AGU indexing for byte-window plans (`0` runtime hash calls). |
| **Pre-Hashed `HeaderWorkspace` Lookups** | **Verified** | All configured header probes (`access_key_header`, `timestamp_header`, `nonce_header`, `signature_header`, `content_sha256_header`, `content-length`, `transfer-encoding`) use AVX2 `vpand` + `shlx`/`tzcnt`/`blsr` tag scanning. |
| **Direct Streaming HMAC Context** | **Verified** | Invokes `ring::hmac::Context::with_key` and `Context::update` directly with zero intermediate `CanonicalBuffer` copy. |
| **Isolated Cold Reject Path** | **Verified** | Unauthorized rejections jump to `.text.unlikely` `HmacAuthPlugin::reject_unauthorized`. |
| **Conditional Alloc Restricted to `> 16` Query Overflow** | **Verified** | `__rust_alloc` in `on_upstream_request` appears only on the query overflow branch when a request contains `> 16` `&`-separated parameters. |
