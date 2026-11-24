# API Metering & Monetization Plugin (`api_metering`)

Per-request hierarchical multi-tier API monetization, prepaid micro-credit ($10^{-6}$) wallet deduction, and macro-quota enforcement (`FixedWindow`, `SlidingWindow`, `TokenBucket`) during the `pre_upstream` phase.

```text
Inbound L7Request
  │
  ├─► resolve_account_keys() : Hierarchical chain L0 -> L1 -> L2 (`account_headers`, max 3)
  │                            SWAR + SplitMix64 (0 div) -> tagged `(hash63 << 1) | has_child`
  │                            (all missing/blank -> #[cold] 400 Bad Request, 0 locks)
  ├─► resolve_tier()         : Zero-alloc length + ASCII case-insensitive match on `tier_header`
  │                            (fallback: `default_tier`)
  └─► ShardedMeteringState::evaluate_chain_compiled()
        ├── Phase 1 (Check/Lease): Per-Core LaneArena (128B) ─► miss ─► Node-Global Shard[64] (192B)
        ├── Phase 2 (Commit)     : O(1) direct `record_mut_by_idx()` across active levels
        ├── Allowed              ─► Inject x-metering-tier, x-metering-balance (+ window headers) -> Continue
        ├── OverageAllowed       ─► #[cold] Inject x-metering-overage: true + negative balance   -> Continue
        └── Exhausted            ─► #[cold] Reject402 (Action::Respond 402 JSON) or Reject429 (Bit-1 bypass)
```

---

## 1. What This Plugin Does

- **Hierarchical Multi-Tenant Identity**: Evaluates up to `3` ordered headers in `account_headers` (e.g. `x-tenant-id` $\to$ `x-team-id` $\to$ `x-user-id`, or `tenant` $\to$ `tier` $\to$ `customer_id`). Requests missing all configured account headers fail closed with `400 Bad Request`.
- **Integer Micro-Credit Wallet ($10^{-6}$)**: Charges `cost_per_req_micro_credits` per allowed request using exact 64-bit signed integers (`1 credit = 1,000,000 micro-credits`), preventing floating-point drift.
- **Macro Quota Enforcement**: Evaluates up to `4` inline quota windows per tier (`FixedWindow`, `SlidingWindow`, `TokenBucket`) atomically across all present hierarchy levels before committing any deduction.
- **Per-Core Escrow + Node Grace**: Serves steady-state hits from thread-local `LaneArena` shards with zero cross-core lock bouncing, refilling adaptive micro-leases from 64-shard `NodeGlobal` state and optional bounded node grace (`clamp(limit / 200, 2, 1000)`).
- **Exhaustion Policies**: Supports `Reject402` (`402 Payment Required` JSON body), `Reject429` (`Action::Reject(RejectReason::TooManyRequests)` with Bit-1 bypass headers), and `Passthrough` (records negative overage debt while still advancing windows).
- **Distributed Shadow Sync**: Optionally runs a background `RedisEtp` `HINCRBY` pipeline (`run_shadow_metering_sync`) with `in_flight_delta` conservation so network failures revert uncommitted batches cleanly.

---

## 2. Where It Sits in Velda Edge

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `api_metering` |
| **Phase** | `pre_upstream` (`PreUpstreamPlugin`) |
| **Worker Placement** | Hot path on Frontline Traffic Workers (`velda-worker-{i}`); optional Redis sync on Shadow Logistics Workers (`pbww: 1..=100`) |
| **Capability** | `PluginCapability::header_only(AccessLevel::Write)` (injects `x-metering-*` headers, zero payload buffering) |
| **Supported Protocols** | `SupportedProtocols::ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |

---

## 3. Requirements & Prerequisites

- L7 listener (`http` or `grpc`) with `HeaderWorkspace`.
- For multi-gateway wallet synchronization (`distributed.enabled = true`): a configured `redis` ETP entry in `etp.json` referenced by `etp_id`, reachable over TCP supporting RESP2 `HINCRBY`.

---

## 4. Quick Start Configuration

```json
{
  "id": "api_metering",
  "config": {
    "account_headers": ["x-tenant-id", "x-team-id", "x-user-id"],
    "tier_header": "x-plan-tier",
    "default_tier": "free",
    "initial_balance_micro_credits": 1000000,
    "tiers": {
      "free": {
        "cost_per_req_micro_credits": 0,
        "exhausted_action": "402",
        "windows": [
          {
            "name": "monthly_cap",
            "window_seconds": 2592000,
            "limit": 10000,
            "algorithm": "fixed_window"
          }
        ]
      }
    }
  }
}
```

---

## 5. Documentation Directory

- [RUNBOOK.md](RUNBOOK.md) — Parameter reference, configuration recipes, observability signals, and incident triage.
- [ARCHITECTURE.md](ARCHITECTURE.md) — 9-module layout, `LaneArena` (`128B`) + `ShardArena` (`192B`) geometry, 2-phase Check-Then-Commit, and reconciliation proofs.
- [BENCHMARK.md](BENCHMARK.md) — Verified `x86-64-v3` release assembly audit, KVM `velda-gw` $\leftrightarrow$ `velda-obs` throughput/latency comparison, and 304k-account memory probe.
- [TEST.md](TEST.md) — Automated integration and distributed sync verification results.
