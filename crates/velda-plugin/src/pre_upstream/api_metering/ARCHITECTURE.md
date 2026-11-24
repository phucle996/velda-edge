# Architecture & Accounting Invariants (`api_metering`)

## 1. Modular Ownership (`9` Bounded Files)

| Module | Responsibility |
| :--- | :--- |
| `mod.rs` | Public crate exports (`ApiMeteringPlugin`, `ApiMeteringConfig`, `ShardedMeteringState`, etc.). |
| `key.rs` | Division-free SWAR `as_chunks::<8>()` + SplitMix64 account hashing, 3-level `AccountKeyChain` (`bit 0 = has_child`), and zero-hash `resolve_tier()`. |
| `config.rs` | JSON parsing (`parse_metering_config`), `CompiledTier` compilation, and bounded node-grace calculation. |
| `arena.rs` | `(k + 1) * 4096 B` `PageSlab<T>` allocator, per-core `LaneArena` (`128B` `LocalAccountRecord`), and 64-shard `ShardArena` (`192B` `AccountState`) with CLOCK eviction. |
| `window.rs` | Inline `24B` `InlineWindowSlot` evaluation (`FixedWindow`, `SlidingWindow`, `TokenBucket`) with monotonic `u64` nanosecond anchor. |
| `lease.rs` | Per-core escrow hit check (`local_lease_covers_request`), O(1) commit (`commit_local_deduction`), and `#[cold]` node refill (`refill_from_node_shard`). |
| `algorithm.rs` | `ShardedMeteringState` orchestrating 2-phase Check-Then-Commit across `levels[0..depth]`, plus `collect_deltas` / `reconcile_balances`. |
| `redis.rs` | Background `RedisEtp` `HINCRBY` pipelined shadow sync (`run_shadow_metering_sync`). |
| `plugin.rs` | `PreUpstreamPlugin` trait implementation (`on_upstream_request`, `attach_etp`, `run_background`) and `#[cold]` response builders. |

---

## 2. Execution & Memory Architecture

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                          api_metering HOT PATH (Frontline Worker)                      │
├────────────────────────────────────────────────────────────────────────────────────────┤
│  1. key.rs (0 heap allocs, 0 division):                                                │
│     ├── resolve_account_keys(): L0 -> L1 -> L2 (`account_headers`, max 3 levels)       │
│     │     tagged 63-bit key: `(hash63 << 1) | has_child` (bit 0: 0=leaf, 1=child)      │
│     │     all missing/empty -> None -> #[cold] 400 Bad Request (fail-closed, 0 locks)  │
│     └── resolve_tier(): len == len && eq_ignore_ascii_case() against CompiledTier[]    │
│                                                                                        │
│  2. arena.rs (Per-Core LaneArena + Node-Global 64 Shards, `(k + 1) * 4096 B`):         │
│     ├── Per-Core Lane[core_id]: lazy LevelArena[0..3]                                  │
│     │     └── 16B AccountIndexSlot -> 128B LocalAccountRecord (0 cross-core MESI!)     │
│     ├── Node-Global Shards[64]: 16B Index -> 192B AccountState (wallet + windows +      │
│     │     node grace budget `clamp(limit / 200, 2, 1000)`)                             │
│     └── Adaptive Lease: `1` req for `< 256` remaining, `(avail / 128).clamp(1, 32)`    │
│                                                                                        │
│  3. algorithm.rs (2-Phase Check-Then-Commit across Hierarchy):                         │
│     ├── Phase 1 (Check/Lease): verify L0 -> ... -> leaf, capture `record_idx`          │
│     │     (abort with 0 commits if any level is exhausted)                             │
│     └── Phase 2 (Commit)     : O(1) `record_mut_by_idx(p.record_idx)` ->               │
│                                commit_local_deduction() across active levels           │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Cache-Line Geometry (`PageSlab` Aligned to `4096` Bytes)

Both `LaneLevelArena` and `ShardArena` store a 2-tier layout (`16B` `AccountIndexSlot` probe table + dense `POD` record slab) allocated via `(k + 1) * 4096 B` `PageSlab<T>` (`alloc_zeroed`):

- **Per-Core `LocalAccountRecord` (`128` Bytes = `2` cache lines, `align(64)`)**:
  - Holds `tagged_key`, `raw_hash`, `tier_fingerprint`, `reconcile_epoch`, `local_credits_lease`, `local_balance_snapshot`, `local_credits_spent_unflushed`, and `[local_window_lease; 4]`, `[local_window_epoch; 4]`, `[local_window_rem_hint; 4]`.
  - Protected only by the thread-local core's `LaneArena` (`0` cross-core cache-line bouncing on steady-state lease hits).
- **Node-Global `AccountState` (`192` Bytes = `3` cache lines, `align(64)`)**:

```text
Offset   Field                              Size   Notes
────────────────────────────────────────────────────────────────────────────
0x00     account_hash                        8B    0 = empty, 1 = tombstone, >=2 = live
0x08     tier_fingerprint                    8B    Detects dynamic tier/window changes
0x10     last_access_sec                     8B    30-day inactivity TTL clock
0x18     balance_micro_credits               8B    Signed i64 (10^-6 credits)
0x20     delta_micro_credits_deducted        8B    Uncommitted local deductions
0x28     in_flight_delta                     8B    Leased to active Redis pipeline
0x30     windows[0..4] (InlineWindowSlot)   96B    4 * 24B inline windows (no pointers)
0x90     grace_credits_remaining             8B    Bounded node grace credit budget
0x98     grace_window_remaining[0..4]       16B    4 * u32 per-window grace budget
0xA8     reconcile_epoch                     4B    Sync epoch for stale-lease invalidation
0xAC     window_count                        1B    Active windows (0..=4)
0xAD     (struct tail padding to 64B align) 19B
────────────────────────────────────────────────────────────────────────────
Total: 192 Bytes (`AccountIndexSlot` holds `clock_ref` in Tier 1 16B probe line)
```

- `InlineWindowSlot` (`24` bytes) replaces `std::time::Instant` (`16` bytes) with a monotonic `u64` nanosecond offset anchored at `ShardedMeteringState::start_instant`, keeping slots `alloc_zeroed`-safe and eliminating `Instant::now()` calls when `has_token_bucket == false`.

---

## 4. Complexity, Safety & Consistency Invariants

1. **Zero-Trap Hot Path**: Account segment hashing uses `slice::as_chunks::<8>()` + SplitMix64 (`key.rs`) with zero division (`ud2 = 0`). Phase 2 commits in `O(1)` via `record_mut_by_idx(p.record_idx)` (`records.as_mut_slice().get_mut(record_idx)`), avoiding a second hash-table probe per level while `LaneArena` is held across both phases.
2. **Bounded Critical Section & Cardinality**: Each `ShardArena` (`64` shards) and `LaneLevelArena` caps growth at `MAX_ACCOUNTS_PER_SHARD = 32_768` slots (`2,097,152` node-global slots across 64 shards). Under key-flood attacks, `evict_one_clock_slot` probes at most `64` slots (`O(1)` worst-case latency).
3. **Lossless Redis Reconciliation**:
   - `collect_deltas()` moves `delta_micro_credits_deducted` into `in_flight_delta` and zeroes `delta_micro_credits_deducted`.
   - If Redis fails, `revert_in_flight_deltas()` restores `in_flight_delta` back to `delta_micro_credits_deducted`.
   - On Redis ACK, `reconcile_balances()` sets `balance_micro_credits = global_balance.saturating_sub(delta_micro_credits_deducted)` and clears `in_flight_delta`, preserving concurrent local deductions that occurred while the Redis RPC was in flight.
4. **Fail-Closed Identity & Header/JSON Safety**: Requests with all `account_headers` missing or blank return `400 Bad Request` before touching any shard lock. Inbound `x-metering-*` headers are overwritten via `HeaderWorkspace::insert`; `402` JSON bodies escape `tier` and `window` identifiers via `push_json_escaped`.
