# API Metering & Monetization Plugin (`api_metering`)

Per-request multi-tier API monetization, prepaid micro-credit ($10^{-6}$) wallet deduction, and macro-quota enforcement (`FixedWindow`, `SlidingWindow`, `TokenBucket`) during the `pre_upstream` phase.

```text
Inbound L7Request
  │
  ├─► resolve_account_hash() : First non-empty match in `account_headers`
  │                            (all missing/blank -> #[cold] 400 Bad Request, 0 locks)
  ├─► resolve_tier()         : Zero-alloc ASCII case-insensitive match on `tier_header`
  │                            (fallback: `default_tier`)
  └─► ShardedMeteringState::evaluate_compiled()  [64 cache-aligned ShardArenas, k * 4096 B]
        ├── Allowed        ─► Inject x-metering-tier, x-metering-balance (+ window headers if > 0) -> Continue
        ├── OverageAllowed ─► #[cold] Inject x-metering-overage: true + negative balance         -> Continue
        └── Exhausted      ─► #[cold] Reject402 (Action::Respond 402 JSON) or Reject429 (Bit-1 bypass)
```

---

## 1. What This Plugin Does

- **Identity & Plan Resolution**: Identifies each tenant via the first non-empty header in `account_headers` (`x-tenant-id`, `x-org-id`, `x-account-id`) and resolves its SLA plan via `tier_header` (`x-plan-tier`).
- **Integer Micro-Credit Wallet ($10^{-6}$)**: Charges `cost_per_req_micro_credits` per allowed request using exact 64-bit signed integers (`1 credit = 1,000,000 micro-credits`), preventing floating-point drift.
- **Macro Quota Enforcement**: Evaluates up to `4` inline quota windows per tier (`FixedWindow`, `SlidingWindow`, `TokenBucket`) atomically before deducting wallet credits.
- **Exhaustion Policies**: Supports `Reject402` (`402 Payment Required` JSON body), `Reject429` (`Action::Reject(RejectReason::TooManyRequests)` with Bit-1 bypass headers), and `Passthrough` (records negative overage debt while still advancing windows).
- **Distributed Shadow Sync**: Optionally runs a background Redis `HINCRBY` pipeline (`run_shadow_metering_sync`) with `in_flight_delta` conservation so network failures revert uncommitted batches cleanly.

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
- For multi-gateway wallet synchronization (`distributed.enabled = true`): reachable Redis instance over TCP supporting RESP2 `HINCRBY`.

---

## 4. Quick Start Configuration

```json
{
  "id": "api_metering",
  "config": {
    "account_headers": ["x-tenant-id", "x-org-id"],
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
- [ARCHITECTURE.md](ARCHITECTURE.md) — `k * 4096`-byte `ShardArena` layout, `192B` `AccountState` geometry, and delta reconciliation proofs.
- [BENCHMARK.md](BENCHMARK.md) — Benchmark matrix and verified `x86-64-v3` release assembly audit.
- [TEST.md](TEST.md) — Automated integration and distributed sync verification results.
