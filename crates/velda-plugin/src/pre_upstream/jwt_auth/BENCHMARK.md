# Performance Benchmark Report: JWT Authentication Plugin (`jwt_auth`)

Performance specification for the unified `jwt_auth` pre-upstream plugin (both `internal` static and `external` ETP key-rotation modes).

---

## 1. Test Environment & Standardized 5-Tier Metric Taxonomy

### 1.1 Hardware & Network Testbed

*[TBD]* — Live cluster benchmark measurements and refreshed post-unification `x86-64-v3` release assembly audits will be populated when run on dedicated benchmark nodes.

### 1.2 5-Tier Diagnostic Matrix

1. **Tier 1 (Macro Network & L7 Delivery)**: Duration (s), Total Requests, Throughput (req/s), Wire Bandwidth (MB/s), Mean/P50/P75/P90/P99/P99.9/Max Latency (ms), Status Code Distribution (`200` / `401` / `403`), Success Rate (%).
2. **Tier 2 (System Resource Footprint)**: Total/User/System/SoftIRQ CPU (%), Frontline Worker CPU (%), Shadow Worker CPU (%), Peak RSS (MB), Heap Allocations / Request.
3. **Tier 3 (Kernel & OS Telemetry via `perf stat`)**: System Calls / Request (`0` clock syscalls via `req.ingress_time_sec()`), Context Switches / Request, Futex Calls.
4. **Tier 4 (CPU Microarchitecture Counters via `perf stat`)**: Instructions / Request, Cycles / Request, IPC, Branch Misprediction Rate (%), L1D / L2 / L3 LLC Hit & Miss Rates (%).
5. **Tier 5 (Plugin Mechanics)**: `JwtCache::token_to_u128` SHA-256 cycles, `_MM_HINT_T0` / `_MM_HINT_T2` overlap effectiveness, 64-byte AVX2 `TagLine` scan latency, 64-byte L1D `DirectoryLine` (`tags` + `algs_packed` + `valid_mask`) + `kid_ints` L3 lookup latency, and cold `ring` signature verification latency (`HS256` vs `EdDSA` vs `ES256` vs `RS256`).

---

## 2. Benchmark Scenarios

- **Scenario 01 — L3 SIMD Positive Cache Hit (`cache_ttl_secs: 300`)**: Measure steady-state RPS and P99 latency when active tokens hit the 64-way L3 SIMD bucket, validate `key_tag` against `JwksKeyTable`, and project 3 claims via `insert_prehashed` (`0` heap allocations).
- **Scenario 02 — Cold Cryptographic Verification (`cache_ttl_secs: 0`, `HS256` / `EdDSA` / `ES256` / `RS256`)**: Measure raw per-request cryptographic verification throughput across L3 Key Byte Arena slots.
- **Scenario 03 — Multi-Key Standby Traversal (`Slot 0` vs `Slot 15`)**: Compare verification latency across primary (`Slot 0`) and standby keys within L1D `DirectoryLine` and L3 `kid_ints` / `key_spans`.
- **Scenario 04 — Malformed Garbage Flood (2-Dot Structural Pre-Filter)**: Measure early `401` rejection throughput on random non-JWT strings dropped before SHA-256 or L3 cache access.
- **Scenario 05 — Forged Signature & Unauthorized Role Flood (Unified Bit-7 Reject Cache)**: Measure CPU protection when invalid signatures (`401`) and failed `assert_claims` (`403`) hit the unified L3 SIMD reject bit (`0x80 | tag7`).
- **Scenario 06 — Live `external` ETP Key Rotation Under Sustained Traffic**: Measure frontline P99 tail stability while Shadow Worker fetches keys via an `etp_id` adapter and swaps `JwksStore` (`ArcSwap<JwksKeyTable>`), preserving non-retired standby slot token caches.

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

## 4. Historical Release Assembly Audit Command (`x86-64-v3`)

Reproduce release assembly inspection on Linux `x86-64-v3` with:

```bash
cargo rustc --release -p velda-plugin --lib -- --emit=asm -C llvm-args=-x86-asm-syntax=intel -C target-cpu=x86-64-v3
```
