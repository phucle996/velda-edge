# Performance Benchmark Report: API Metering & Tenant Usage Accounting (`api_metering`)

Benchmark specification and verified release assembly audit for `api_metering`.

---

## 1. Verified Release Assembly & Symbol Audit (`x86-64-v3`)

Generated with:
```bash
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo rustc -p velda-plugin --release --lib -- --emit=asm
```

| Symbol | Role | Instructions | Stack Frame | Heap Alloc / Panic Stubs on Path |
| :--- | :--- | :---: | :---: | :--- |
| `ApiMeteringPlugin::on_upstream_request` | Hot-path entrypoint | `802` | `296 B` | `0` alloc / `0` bounds-check / `0` unwrap |
| `ShardedMeteringState::evaluate_compiled` | Shard probe + 0/1-window path | `647` | `88 B` | `0` alloc (`1` slice check on `windows.get(..4)` fallback, `0` bounds-check) |
| `evaluate_multi_window` | Multi-window (`2..=4`) dry-run & commit | `589` | `88 B` | `0` alloc / `0` bounds-check / `0` unwrap |
| `ShardArena::evict_batch_clock` | Saturated slab batch eviction | `43` | `0 B` | `0` alloc / `0` bounds-check |
| `ShardArena::evict_one_clock_slot` | Single-slot `<= 64` CLOCK eviction | `56` | `0 B` | `0` alloc / `0` bounds-check |
| `handle_wallet_shortfall` | `#[cold]` wallet insufficiency handler | `174` | `24 B` | `0` alloc / `0` bounds-check |
| `handle_overage_allowed` | `#[cold]` overage header tagging | `103` | `136 B` | `0` alloc / `0` bounds-check |
| `handle_exhausted` | `#[cold]` `402` JSON / `429` bypass builder | `439` | `344 B` | Isolated cold allocation (only on rejection) |

---

## 2. Production Load Scenarios (End-to-End Matrix)

Runtime comparison numbers against external gateways require isolated KVM load generation (`velda-gw` ↔ `velda-obs`):

| Scenario | Description | Status |
| :--- | :--- | :---: |
| **Scenario 01** | Single-tier `FixedWindow` + `cost_per_req_micro_credits` (`5,000 req/s`) | *[TBD]* |
| **Scenario 02** | Multi-tenant high-concurrency (`40,000 req/s`, `1,000` active tenants across `free`/`pro`/`enterprise`) | *[TBD]* |
| **Scenario 03** | Quota saturation `402 Payment Required` / `429` short-circuit (`100,000+ req/s`) | *[TBD]* |
| **Scenario 04** | Multi-window (`burst` + `monthly`) dry-run isolation & `TokenBucket` refill accuracy | *[TBD]* |
| **Scenario 05** | Background Redis `HINCRBY` shadow sync without frontline worker latency spikes | *[TBD]* |
