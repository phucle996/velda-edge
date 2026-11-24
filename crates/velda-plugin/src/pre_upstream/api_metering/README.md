# API Metering & Monetization Plugin (`api_metering`)

`api_metering` enforces **per-request usage quotas**, **prepaid micro-credit billing** ($10^{-6}$ credits), and **hierarchical limits** (such as `tenant -> team -> user` or `tenant -> tier -> customer_id`) right before a request is forwarded upstream.

In steady state, almost every request is checked and billed **in-memory on the local CPU core** (~`1 µs` overhead for 1-level, ~`3.8 µs` for 3-level) with zero Redis round-trips or heap allocations on the hot path.

```text
  Incoming Request
         │
         ▼
  [1. Identify Caller] ──► All account headers missing/blank? ──► 400 Bad Request (fail-closed)
         │
         ▼ (1 to 3 cumulative hierarchy keys: L0 -> L1 -> L2)
  [2. Pick Plan Tier]  ──► Match `tier_header` (e.g. free / pro / enterprise, or `default_tier`)
         │
         ▼
  [3. Check & Deduct]  ──► Per-Core Cache (~97% hits) ──► miss ──► Node Shard (64-way)
         │
         ├── Allowed        ──► Add `x-metering-*` headers ──► Forward Upstream
         ├── OverageAllowed ──► Tag `x-metering-overage: true` ──► Forward Upstream
         └── Exhausted      ──► Return `402` JSON / `429` Too Many Requests
```

---

## 1. Key Capabilities

- **Hierarchical Multi-Tenant Quotas**: Enforces up to `3` ordered levels (`account_headers`), e.g. `["x-tenant-id", "x-team-id", "x-user-id"]`. If a parent level (like the tenant) runs out of quota, all child users are blocked immediately without deducting partial credits from other levels.
- **Exact Integer Wallet ($10^{-6}$ Credits)**: Deducts `cost_per_req_micro_credits` using 64-bit signed integers (`1 credit = 1,000,000 micro-credits`) so billing never suffers floating-point rounding drift.
- **Up to 4 Concurrent Windows per Tier**: Mix `fixed_window`, `sliding_window`, and `token_bucket` (e.g. a 60-second burst limit alongside a 30-day monthly cap).
- **Predictable Exhaustion Policies**: Choose per tier between `402 Payment Required` (JSON diagnostic body), `429 Too Many Requests`, or `passthrough` (allow traffic while recording overage debt).
- **Optional Multi-Gateway Redis Sync**: A background Shadow Worker periodically batches local deductions to Redis (`HINCRBY`) without slowing down live traffic.

---

## 2. Where It Runs in Velda Edge

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `api_metering` |
| **Pipeline Phase** | `pre_upstream` (runs after routing, before upstream forwarding) |
| **Execution Threads** | Hot path on Frontline Traffic Workers (`velda-worker-{i}`); optional Redis sync on Shadow Logistics Workers (`velda-shadow-{i}`) |
| **Access Scope** | `PluginCapability::header_only(AccessLevel::Write)` (reads/writes headers only; never buffers request bodies) |
| **Protocols** | HTTP/1.1, HTTP/2, HTTP/3, and gRPC (`SupportedProtocols::ALL_L7`) |

---

## 3. Prerequisites

- An L7 listener (`http` or `grpc`).
- Upstream authentication (such as `api_key_auth`, `jwt_auth`, or `hmac_auth`) or trusted ingress headers supplying at least the first header in `account_headers`.
- *(Optional)* A `redis` entry in `etp.json` referenced by `etp_id` (omit `etp_id` for node-local-only metering).

---

## 4. Quick Start

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

## 5. Documentation Map

- **[ARCHITECTURE.md](ARCHITECTURE.md)** — Visual guide to the 3-tier escrow model, 2-phase hierarchical Check-Then-Commit, cache-line memory layout, and Redis reconciliation.
- **[RUNBOOK.md](RUNBOOK.md)** — Configuration parameters, practical recipes, response/header signals, and incident troubleshooting.
- **[BENCHMARK.md](BENCHMARK.md)** — `velda-benchmark` 5-tier performance spec, KVM `velda-gw` $\leftrightarrow$ `velda-obs` results, 304k-account memory probe, and `x86-64-v3` ASM audit.
- **[TEST.md](TEST.md)** — Automated test suite (`16` integration + `3` distributed sync) and live KVM verification results.
