# Performance Benchmark Report: OAuth 2.0 Token Introspection Plugin (`oauth2_introspection`)

Performance benchmark specification and diagnostic matrix for the ETP-backed, Per-Core Two-Tier `oauth2_introspection` pre-upstream plugin.

---

## 1. Test Environment & Standardized 5-Tier Metric Taxonomy

### 1.1 Hardware & Network Testbed

*[Pending Re-Benchmark]* — Previous measurements from the legacy direct-URL / shared-cache client have been cleared after refactoring to `velda-etp` provider adapters, `inflight.rs`, and Per-Core Two-Tier (`Hot L3 <= 4 MiB` + `Cold DRAM <= 2% RAM`) cache lanes.

### 1.2 Standardized 5-Tier Diagnostic Matrix

1. **Tier 1 (Macro Network & L7 Delivery)**: Duration (s), Total Requests, Throughput (req/s), Wire Bandwidth (MB/s), Mean / P50 / P75 / P90 / P99 / P99.9 / Max Latency (ms), HTTP Status Code Distribution (`200` / `401` / `403` / `502`), Success Rate (%).
2. **Tier 2 (System Resource Footprint)**: Total / User / System / SoftIRQ CPU (%), Frontline Worker CPU (%), Shadow Worker CPU (%), Peak RSS (bounded under `4 MiB` Hot L3 + `<= 2% RAM` Cold DRAM ceiling), Heap Allocations / Request.
3. **Tier 3 (Kernel & OS Telemetry via `perf stat`)**: System Calls / Request, Context Switches / Request, Lockless/Core-Local `AtomicU8` MESI Residency (%).
4. **Tier 4 (CPU Microarchitecture Counters via `perf stat`)**: Instructions / Request, Cycles / Request, IPC, Branch Misprediction Rate (%), L1D / L2 / L3 LLC Hit & Miss Rates (%).
5. **Tier 5 (Plugin Mechanics)**: `IntrospectionCache::token_to_u128` SHA-256 latency, `_MM_HINT_T0` / `_MM_HINT_T2` prefetch effectiveness, AVX2/NEON/SWAR 64-way `TagLine` scan latency, Cold DRAM demotion/promotion overhead, `InFlightCoordinator` coalescing efficiency, and ETP `StreamPool` keep-alive latency.

---

## 2. Benchmark Scenarios

- **Scenario 01 — Warm Hot-L3 SIMD Cache Hit (`cache_ttl_seconds: 300`)**: Measure synchronous `try_on_upstream_request` RPS, P99 latency, and zero heap allocation across `required_scopes` + `claims_to_headers` injection on core-local lanes.
- **Scenario 02 — Stale-While-Revalidate Under Sustained Load (`cache_ttl_seconds: 30`, `3s` lead window)**: Measure frontline P99 latency isolation while `velda-shadow-{id}` refreshes active tokens asynchronously over ETP keep-alive pools (`revalidate_on_shadow`).
- **Scenario 03 — Concurrent Cold Miss Single-Flight Stampede Protection**: Fire bursts of concurrent connections carrying identical unseen opaque tokens across cores to verify `inflight::InFlightCoordinator` coalesces duplicate IdP queries.
- **Scenario 04 — High-Cardinality Session Overflow (`Hot L3 -> Cold DRAM` Spillover & Promotion)**: Flood distinct valid tokens beyond the 4 MiB Hot L3 cap mixed with `401` invalid sprays, confirming valid tokens spill to lazy Cold DRAM and re-promote on access while `401` entries never pollute Cold DRAM.
- **Scenario 05 — Multi-Provider ETP Latency Breakdown**: Measure cold miss latency across RFC 7662 (`keycloak`, `hydra`), `/userinfo` (`auth0`), and Vault (`/v1/auth/token/lookup`) ETP pools with baseline pre-warming and burst spare-page connections.

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

## 4. Release Assembly & Symbol Audit (`x86-64-v3`)

*[Pending Re-Audit]* — Re-run `cargo rustc --release -p velda-plugin --lib -- --emit=asm -C llvm-args=-x86-asm-syntax=intel -C target-cpu=x86-64-v3` and inspect `try_on_upstream_request` + `IntrospectionCache::lookup` after the Per-Core Two-Tier & ETP refactor to populate fresh verified line ranges.
