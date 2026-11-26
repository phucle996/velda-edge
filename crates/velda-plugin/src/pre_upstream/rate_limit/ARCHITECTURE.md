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

- **No `MAX_ENTRIES` Lockout DoS**: Earlier implementations stopped inserting new IPs when shards filled up or ran `Vec::retain()` scans causing latency spikes. Now `ShardSlab` allocates a fixed `Box<[SlabSlot]>` partitioned into `SET_WAYS = 16`-slot associative sets.
- **Bounded Worst-Case O(1) Eviction**: Each set maintains a local `u8` `CLOCK` hand (`set_clocks[set_idx]`). An eviction sweep inspects at most `SET_WAYS + 1 = 17` contiguous `SlabSlot`s, clearing `ref_bit` on first pass and replacing the first cold slot cleanly.
- **Monotonic Timing (`Instant`)**: `RateLimitArena::now_ms()` computes elapsed milliseconds from a process-local `Instant` anchor (starting at offset `1000ms`), preventing NTP backward clock steps from breaking window rollovers or token refill deltas.

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
