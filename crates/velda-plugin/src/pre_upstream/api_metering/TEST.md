# Verification & Test Report (`api_metering`)

---

## 1. Verified Automated Tests

### Commands Run
```bash
cargo test -p velda-plugin --test api_metering_test --test distributed_sync_test
```

### Observed Results
- **`crates/velda-plugin/tests/api_metering_test.rs` — 16/16 passed (`0.14s`)**:
  - `test_api_metering_free_tier_rejection_402` — Free tier exhaustion triggers `Action::Respond(402)` with JSON payload and `x-metering-exhausted-window`.
  - `test_api_metering_pro_tier_rejection_429` — Pro tier exhaustion triggers `Action::Reject(TooManyRequests)` with Bit-1 bypass headers.
  - `test_api_metering_enterprise_overage_passthrough` — Wallet exhaustion under `Passthrough` records negative balance and sets `x-metering-overage: true`.
  - `test_api_metering_prepaid_micro_credits_deduction` — Exact $10^{-6}$ micro-credit deduction (`1000000 -> 500000 -> 0`) followed by `402 credit_balance`.
  - `test_api_metering_payg_empty_windows_omits_bogus_window_headers` — Verifies `AccessLevel::Write` and omits `x-metering-remaining` / `x-metering-window` when `windows: []`.
  - `test_api_metering_tier_header_resolution_and_dynamic_tier_switch` — Verifies trimmed ASCII case-insensitive tier resolution, fallback, and `tier_fingerprint` reset.
  - `test_api_metering_dry_run_isolation_multi_window` — Confirms window 2 rejection mutates neither window 1 counters nor wallet credits.
  - `test_api_metering_overage_passthrough_still_advances_windows` — Verifies window counters advance during wallet overage under `Passthrough`.
  - `test_api_metering_in_flight_delta_revert_and_concurrent_reconcile` — Verifies `revert_in_flight_deltas` and `reconcile_balances` concurrent safety.
  - `test_api_metering_from_config_json_escape_and_window_limit_guard` — Rejects `> 4` windows and verifies `402` JSON escaping.
  - `test_api_metering_shard_growth_and_clock_eviction_bounded` — Stress-inserts `40,000` keys (`> MAX_ACCOUNTS_PER_SHARD = 32,768`), verifying slab resize and bounded CLOCK eviction.
  - `test_api_metering_missing_account_header_rejects_400` — Verifies missing/blank `account_headers` reject fail-closed with `400 Bad Request`.
  - `test_api_metering_etp_wiring_and_shadow_sync` — Verifies `etp_id` validation, `wire_plugin_etps` `RedisEtp` attachment, and mock RESP2 `HINCRBY` sync.
  - `test_api_metering_hierarchical_3_level_2phase_commit` — Verifies 3-level `tenant -> team -> user` tagged lookup (`bit 0`) and 2-phase Check-Then-Commit.
  - `test_api_metering_node_grace_overdraft_bounded` — Verifies `grace_enabled: true` grants bounded `clamp(limit/200, 2, 1000)` node grace before `402`.
  - `test_api_metering_multi_core_hot_org_concurrent_leases` — Spawns `8` concurrent threads against one hot `org` (`limit=1000, grace_enabled=true`), verifying `1000..=1005` accepted.
- **`crates/velda-plugin/tests/distributed_sync_test.rs` — 3/3 passed (`0.00s`)**:
  - `test_redis_pipelined_hincrby_mock_server` — RESP2 `HINCRBY` batch serialization and integer parsing against mock TCP server.
  - `test_quota_delta_collection_and_reconciliation` — `ShardedQuotaState` delta collection and global count reconciliation.
  - `test_metering_delta_collection_and_reconciliation` — `ShardedMeteringState::collect_deltas` and `reconcile_balances` enforcement.

---

## 2. Isolated KVM Live Verification (`velda-gw` $\rightarrow$ `velda-obs`)

- **Missing / Blank Account Header**: `curl` without `x-tenant-id` or with whitespace-only `x-tenant-id` returned `HTTP/1.1 400 Bad Request` (`"error":"bad_request"`).
- **1-Level `free` Exhaustion (`limit = 5`)**: Requests 1–5 returned `200 OK` (`pong`); request 6 returned `402 Payment Required` (`"tier":"free","window":"burst"`).
- **3-Level Hierarchy Enforcement (`x-tenant-id: test-hier`, `x-team-id: eng`, `limit = 5`)**: `alice` completed 5 × `200 OK`; `bob`'s 1st request under the same tenant/team immediately returned `402 Payment Required`.
- **Load Stability (`wrk -t4 -c64 -d10s`, ~3.38M total requests)**: `0` socket/HTTP errors, `0` WARN/ERROR entries in `velda-edge` logs.
