# Architecture & Accounting Invariants (`api_metering`)

## 1. Execution & Memory Architecture

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                          api_metering HOT PATH (Frontline Worker)                      │
├────────────────────────────────────────────────────────────────────────────────────────┤
│  1. key.rs (0 heap allocs):                                                            │
│     ├── resolve_account_keys(): L0 (tenant) -> L1 (team) -> L2 (user)                  │
│     │     tagged 63-bit key: `(hash63 << 1) | has_child` (bit 0: 0=leaf, 1=child)      │
│     │     all missing/empty -> None -> #[cold] 400 Bad Request (fail-closed, 0 locks)  │
│     └── resolve_tier(): trimmed ASCII case-insensitive match against CompiledTier[]    │
│                                                                                        │
│  2. arena.rs (Per-Core LaneArena + Node-Global 64 Shards, `(k + 1) * 4096 B`):         │
│     ├── Per-Core Lane[core_id]: lazy LevelArena[0..3] (non-contiguous `(k+1)*4096 B`)  │
│     │     └── 16B AccountIndexSlot -> 128B LocalAccountRecord (0 cross-core MESI!)     │
│     ├── Node-Global Shards[64]: 16B Index -> 192B AccountState (wallet + windows +      │
│     │     node grace budget `clamp(limit / 200, 2, 1000)`)                             │
│     └── Adaptive Lease: `1` req for `< 256` remaining, `(avail / 128).clamp(1, 32)`    │
│                                                                                        │
│  3. algorithm.rs (2-Phase Check-Then-Commit across Hierarchy):                         │
│     ├── Phase 1 (Check/Lease): verify L0 -> ... -> leaf (abort 0 commits if any fails) │
│     └── Phase 2 (Commit)     : deduct local_credits_lease & local_window_lease         │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. `AccountState` Cache-Line Geometry (`192` Bytes, `align(64)`)

Every account occupies a single contiguous `192`-byte (`3` cache line) POD slot with **zero** heap-allocated `Vec<WindowState>`:

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
0x90     window_count                        1B    Active windows (0..=4)
0x91     (struct tail padding to 64B align) 47B
────────────────────────────────────────────────────────────────────────────
Total: 192 Bytes (`AccountIndexSlot` holds `clock_ref` in Tier 1 16B probe line)
```

- `InlineWindowSlot` (`24` bytes) replaces `std::time::Instant` (`16` bytes) with a monotonic `u64` nanosecond offset anchored at `ShardedMeteringState::start_instant`, keeping slots `alloc_zeroed`-safe and eliminating `Instant::now()` calls when `has_token_bucket == false`.

---

## 3. Complexity & Distributed Consistency Invariants

1. **Bounded Critical Section**: Each shard lock protects a contiguous open-addressed probe + integer arithmetic (~15–30 ns, zero heap allocs inside lock). Under key-flood attacks (`> 32,768` live keys/shard), `evict_one_clock_slot` probes at most `64` slots (`O(1)` worst-case latency).
2. **Lossless Redis Reconciliation**:
   - `collect_deltas()` moves `delta_micro_credits_deducted` into `in_flight_delta` and zeroes `delta_micro_credits_deducted`.
   - If Redis fails, `revert_in_flight_deltas()` restores `in_flight_delta` back to `delta_micro_credits_deducted`.
   - On Redis ACK, `reconcile_balances()` sets `balance_micro_credits = global_balance.saturating_sub(delta_micro_credits_deducted)` and clears `in_flight_delta`, preserving concurrent local deductions that occurred while the Redis RPC was in flight.
3. **Header Spoofing & JSON Safety**: Inbound `x-metering-*` headers are overwritten via `HeaderWorkspace::insert`; `402` JSON bodies escape `tier` and `window` identifiers via `push_json_escaped`.
