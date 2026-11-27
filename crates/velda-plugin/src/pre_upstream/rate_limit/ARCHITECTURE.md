# Internal Architecture Specification (`rate_limit`)

## 1. Execution & Memory Isolation Model

`rate_limit` separates **Frontline traffic evaluation** (`on_upstream_request`) from **Shadow State-of-Truth synchronization** (`providers/*/sync.rs`):

```text
  Frontline Core (velda-worker-{i})
    ├─► stack extract KeyDigestSet [max 4 tiers, 0 heap allocs]
    ├─► L1 CoreLane[lane][hash & 63]:
    │     AtomicU64 = [epoch12 | key_tag40 | rem_batch12]
    │     CAS decrement in L1 cache (< 5ns when lease_batch > 1)
    └─► L2 ShardArena[hash & 63]:
          16-way Set-Associative SlabSlot + O(1) CLOCK Eviction Hand
          │
          ▼ updates dirty_delta + synced_epoch
  Shadow Worker (velda-shadow-{i}, SCHED_IDLE)
    └─► collect_dirty_deltas() -> SoT provider client -> reconcile_cluster_totals()
```

## 2. Key Extraction & HashDoS Defense (`key.rs`)

- **Boot-Time CSPRNG Salt**: `CompiledKeyExtractor::new()` draws `[u64; 2]` from `ring::rand::SystemRandom`. Every key is hashed via `swar_keyed_hash(salt, tier_ordinal, tag, bytes)` using unaligned 8-byte SWAR chunks and 128-bit golden-ratio folded multiplication, preventing predictable hash-collision DoS against shard buckets.
- **Stack Hierarchical Cascade**: For `KeyStrategy::Hierarchical`, up to `MAX_HIERARCHICAL_TIERS = 4` cumulative digests `[src0, src0|src1, src0|src1|src2, src0|src1|src2|src3]` are formatted inside a fixed `[u8; 512]` stack buffer and returned in a stack `KeyDigestSet` without heap allocation.

## 3. Bounded Arena & Eviction Invariants (`arena.rs`)

- **Hardware-Sized Per-Core `CoreLane` & Lock-Free Fast-Reject**: `RateLimitArena` sizes L1 `CoreLane`s dynamically from `velda_core::hardware::probe_cpu()` and `velda_core::worker::get_allowed_cores()` (supporting 2 cores to 128+ cores via `thread_local! LOCAL_CORE_LANE`). Each 64-bit `AtomicU64` lane slot caches both positive micro-leases (packing exact `bucket_rem` and `batch_rem`) and exhausted fast-reject entries (eliminating mutex contention when hot keys are flooded even under small `burst_capacity`).
- **Set-SoA Cache Layout (`SlabSet` + `SlotBody`)**: Each 64-shard `ShardSlab` is partitioned into power-of-2 `SlabSet`s (`SET_WAYS = 16`, indexed via bitwise `& set_mask`). Every `SlabSet` packs `[u64; 16]` hashes contiguously into 2 cache lines (`128B`) probed via AVX2 `_mm256_cmpeq_epi64` on `x86_64` (with portable scalar fallback for non-AVX2/ARM64 targets), paired with 16 `64B` cache-line-aligned `SlotBody` state blocks.
- **Expired-First & Dirty/Tier-Aware `CLOCK` Eviction**: On a set miss, `find_or_evict_way` reclaims empty ways first, then expired clean ways (`dirty_delta == 0` past the active window), before running a bounded `O(SET_WAYS)` `CLOCK` sweep that prioritizes clean victims over dirty/tier-0 tenant slots.
- **Bitmask + Round-Robin `O(dirty_sets)` Sync Drain**: `dirty_shard_mask` (`AtomicU64`) skips clean shards lock-free, `dirty_set_words` + `SlabSet.dirty_mask` visit only dirty sets/ways via `trailing_zeros()`, and `sync_shard_cursor` drains dirty shards round-robin.
- **No `MAX_ENTRIES` Lockout or OOM DoS**: Bounded pre-allocation up front (`64..=MAX_ALLOWED_ENTRIES`), never growing dynamically on traffic floods.
- **Single-Read Ingress & Monotonic Coarse Timing**: Request serving reads `L7Request::ingress_time_nanos` once per request (`RateLimitArena::now_ms_from_ingress`), avoiding redundant clock reads across hierarchical tiers and keeping millisecond granularity for `TokenBucket` / `SlidingWindow`. Background reconciliation and zero-timestamp fallbacks use `CLOCK_MONOTONIC_COARSE` on Linux (and `Instant::now()` on non-Linux targets), with per-bucket monotonic clamping against cross-core ingress skew.

## 4. Soft Overdraft & Cluster Reconciliation (`algorithm.rs`)

- **Effective Capacity Formula**:
  $$\text{effective\_capacity} = \text{burst\_capacity} + \left\lfloor \frac{\text{burst\_capacity} \times \text{overdraft\_percent}}{100} \right\rfloor$$
- **Cluster Accounting Without Double-Counting**: Each active `SlabSlot` tracks `dirty_delta`, `node_synced_total`, and `cluster_other_used` per `synced_epoch`. When the Shadow Worker receives `cluster_total` from the configured SoT ETP, it computes:
  $$\text{cluster\_other\_used} = \max(0, \text{cluster\_total} - \text{node\_synced\_total})$$
  Hot-path checks compare local usage + `cluster_other_used` against `effective_capacity`, never issuing network I/O on request threads.

## 5. Pure Provider Dispatch Boundary (`providers/mod.rs`) & Topology Branches

`providers/mod.rs` performs pure enum dispatch (`RateLimitSotProvider`) over the 14 SoT storage ETPs with zero cross-provider shared helpers. Each provider folder (`redis`, `valkey`, `dragonfly`, `aws_elasticache`, `gcp_memorystore`, `upstash`, `memcached`, `dynamodb`, `postgres`, `mysql`, `nats`, `clickhouse`, `elasticsearch`, `s3`) separates its SRP concerns cleanly across protocol framing (`wire.rs` / `codec.rs` / `http.rs` / `sigv4.rs`), table DDL bootstrap (`schema.rs`), topology execution (`standalone.rs` / `cluster.rs`), orchestration (`client.rs`), and its shadow ticker/health loop (`sync.rs`).

Inside `velda-etp::{redis, valkey, aws_elasticache, gcp_memorystore}`, each ETP owns self-contained `standalone` and `cluster` topology branches controlled by `settings.mode`:
- Standalone pools maintain 1 `ColdConnectBlueprint` + 1 `EtpPageArena` with provider-specific auth (`AUTH`/`SELECT`, SigV4 `Action=connect`, or cached GCP OAuth2 bearer) and zero slot overhead.
- Cluster pools own isolated `slots.rs` modules running `CLUSTER SLOTS` on `seed_nodes`, exposing wait-free `ArcSwap` topology backed by hybrid `SlotMap` (`<= 64` contiguous 8B `SlotRange` intervals inline; `> 64` intervals in `#[cold]` heap-boxed `Dense4Bit` 8 KB or `DenseU8` 16 KB LUT), and patching slots dynamically on `-MOVED` / `-ASK`.
