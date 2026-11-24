# Verification & Test Specification (`api_metering`)

Automated test suites verifying multi-tier routing, micro-credit wallet accounting, `402`/`429` rejections, overage debt tracking, dynamic tier swaps, multi-window dry-run isolation, 3-level hierarchical 2-phase Check-Then-Commit, cardinality eviction, and Redis delta reconciliation.

---

## 1. Test Verification Matrix

### `crates/velda-plugin/tests/api_metering_test.rs` (`16` tests)

| Test Identifier | Verification Scenario | Status |
| :--- | :--- | :---: |
| `test_api_metering_free_tier_rejection_402` | Free tier exhaustion triggers `Action::Respond(402)` with structured JSON payload and `x-metering-exhausted-window`. | **PASS** |
| `test_api_metering_pro_tier_rejection_429` | Pro tier exhaustion triggers `Action::Reject(TooManyRequests)` with Bit-1 bypass headers (`x-metering-tier`, `x-metering-exhausted`). | **PASS** |
| `test_api_metering_enterprise_overage_passthrough` | Prepaid wallet exhaustion under `Passthrough` drops balance negative and sets `x-metering-overage: true`. | **PASS** |
| `test_api_metering_prepaid_micro_credits_deduction` | Exact $10^{-6}$ micro-credit deduction (`1000000 -> 500000 -> 0`) followed by `402 credit_balance`. | **PASS** |
| `test_api_metering_payg_empty_windows_omits_bogus_window_headers` | Verifies `AccessLevel::Write` capability and confirms `x-metering-remaining` / `x-metering-window` are omitted when `windows: []`. | **PASS** |
| `test_api_metering_tier_header_resolution_and_dynamic_tier_switch` | Verifies trimmed ASCII case-insensitive tier resolution, unknown-tier fallback, and `tier_fingerprint` reset on same-window-count tier swap. | **PASS** |
| `test_api_metering_dry_run_isolation_multi_window` | Confirms window 2 rejection mutates neither window 1 counters nor prepaid wallet credits. | **PASS** |
| `test_api_metering_overage_passthrough_still_advances_windows` | Verifies window counters keep advancing during wallet overage under `Passthrough`. | **PASS** |
| `test_api_metering_in_flight_delta_revert_and_concurrent_reconcile` | Verifies `revert_in_flight_deltas` recovery on Redis failure and `reconcile_balances` preservation of concurrent local deductions. | **PASS** |
| `test_api_metering_from_config_json_escape_and_window_limit_guard` | Rejects `> 4` windows in `from_config` and verifies valid JSON escaping in `402` error payloads. | **PASS** |
| `test_api_metering_shard_growth_and_clock_eviction_bounded` | Stress-inserts `40,000` keys into shard 0 (`> MAX_ACCOUNTS_PER_SHARD = 32,768`), verifying dynamic slab growth and bounded CLOCK eviction. | **PASS** |
| `test_api_metering_missing_account_header_rejects_400` | Verifies missing or blank `account_headers` (even with `x-forwarded-for`) reject fail-closed with `400 Bad Request`. | **PASS** |
| `test_api_metering_etp_wiring_and_shadow_sync` | Verifies missing `etp_id` rejection, `wire_plugin_etps` `RedisEtp` attachment, and mock RESP2 `HINCRBY` reconciliation. | **PASS** |
| `test_api_metering_hierarchical_3_level_2phase_commit` | Verifies 3-level `tenant -> team -> user` tagged lookup (`bit 0`) and 2-phase Check-Then-Commit across levels. | **PASS** |
| `test_api_metering_node_grace_overdraft_bounded` | Verifies `grace_enabled: true` grants bounded `clamp(limit/200, 2, 1000)` node grace before `402`. | **PASS** |
| `test_api_metering_multi_core_hot_org_concurrent_leases` | Spawns `8` concurrent threads against one hot `org` (`limit=1000, grace_enabled=true`), verifying `1000..=1005` accepted. | **PASS** |

### `crates/velda-plugin/tests/distributed_sync_test.rs` (`3` tests)

| Test Identifier | Verification Scenario | Status |
| :--- | :--- | :---: |
| `test_redis_pipelined_hincrby_mock_server` | Verifies RESP2 `HINCRBY` batch serialization and integer parsing against a mock TCP RESP server. | **PASS** |
| `test_quota_delta_collection_and_reconciliation` | Verifies `ShardedQuotaState` delta collection and global count reconciliation. | **PASS** |
| `test_metering_delta_collection_and_reconciliation` | Verifies `ShardedMeteringState::collect_deltas` and `reconcile_balances` credit enforcement. | **PASS** |

---

## 2. Running Automated Tests

```bash
cargo test -p velda-plugin --test api_metering_test --test distributed_sync_test
```

Latest run observed `16 passed; 0 failed` (`0.14s`) in `api_metering_test` and `3 passed; 0 failed` (`0.00s`) in `distributed_sync_test`.
