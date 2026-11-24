# API Metering & Monetization (`api_metering`) — Operations Runbook

## 1. Parameter Reference

| Field | Type | Default | Constraints | Description |
| :--- | :--- | :---: | :--- | :--- |
| `account_headers` | `Vec<String>` | `["x-tenant-id", "x-org-id", "x-account-id"]` | Valid RFC 9110 header names (`1..=3` levels) | Ordered hierarchy chain (`L0 -> L1 -> L2`). Present levels are hashed cumulatively (`L0`, `L0\|L1`, `L0\|L1\|L2`) and enforced atomically; missing all levels rejects `400 Bad Request`. |
| `account_header` | `String` | — | Valid RFC 9110 header name | Single-header shorthand used when `account_headers` array is omitted. |
| `tier_header` | `String` | `"x-plan-tier"` | Valid RFC 9110 header name | Header selecting the active tier (trimmed, ASCII case-insensitive). |
| `default_tier` | `String` | `"free"` | Lowercased | Fallback tier when `tier_header` is absent, empty, or unrecognized. |
| `initial_balance_micro_credits` | `i64` | `0` | Signed 64-bit integer | Starting wallet credit balance for newly seen accounts ($1\text{ credit} = 1{,}000{,}000$). |
| `tiers` | `Object` | Built-in `free`, `pro`, `enterprise` | Non-empty map (falls back to defaults if empty) | Map of tier name to tier policy object. |
| `tiers.<name>.cost_per_req_micro_credits` | `u64` | `0` | `0..=i64::MAX` | Micro-credits deducted per allowed request. |
| `tiers.<name>.exhausted_action` | `String` | `"402"` | `"402"` / `"reject_402"`, `"429"` / `"reject_429"` / `"too_many_requests"`, `"allow"` / `"passthrough"` / `"overage"` | Action on wallet or window exhaustion. |
| `tiers.<name>.grace_enabled` | `bool` | `false` | — | Enables bounded node-level grace (`clamp(limit / 200, 2, 1000)`) to absorb multi-core lease skew. |
| `tiers.<name>.grace_requests` | `Option<u64>` | `None` | `>= 0` | Explicit node-level grace request budget (overrides `clamp(limit / 200, 2, 1000)` when set). |
| `tiers.<name>.windows` | `Array<Object>` | `[]` | `0..=4` windows (`MAX_WINDOWS_PER_TIER = 4`) | Inline quota windows evaluated atomically before credit deduction. |
| `tiers.<name>.windows[].name` | `String` | `"window_<idx>"` | — | Window label reported in `x-metering-window` and `402` payload. |
| `tiers.<name>.windows[].window_seconds` | `u64` | `2592000` (30 days) | `>= 0` (`0` = pass-through) | Window duration in seconds. |
| `tiers.<name>.windows[].limit` | `u64` | `10000` | `>= 0` | Max request count (or bucket capacity) within `window_seconds`. |
| `tiers.<name>.windows[].algorithm` | `String` | `"fixed_window"` | `"fixed"` / `"fixed_window"`, `"sliding_window"`, `"token"` / `"token_bucket"` | Window accounting algorithm. |
| `etp_id` | `String` | `None` (local-only) | Omit for local-only, or match a `redis` entry in `etp.json` | Presence of `etp_id` enables background `RedisEtp` `HINCRBY` balance reconciliation. |
| `sync_interval_ms` | `u64` | `1000` | `>= 10` ms | Batch reconciliation interval in milliseconds when `etp_id` is set. |
| `key_prefix` | `String` | `"velda"` | Non-empty string | Redis hash key prefix (`<key_prefix>:metering:consumed`) when `etp_id` is set. |
| `pbww` | `u32` | `35` (or `0` if local-only) | `1..=100` | Background Shadow Worker workload weight when `etp_id` is set. |

---

## 2. Scenario-Based Configuration Recipes

### Recipe A: Pure Prepaid Pay-As-You-Go (`payg`, Wallet-Only)
With `windows: []`, only `x-metering-tier` and `x-metering-balance` are forwarded upstream (`x-metering-remaining` and `x-metering-window` are omitted):
```json
{
  "id": "api_metering",
  "config": {
    "account_headers": ["x-tenant-id"],
    "default_tier": "payg",
    "initial_balance_micro_credits": 5000000,
    "tiers": {
      "payg": {
        "cost_per_req_micro_credits": 2500,
        "exhausted_action": "402",
        "windows": []
      }
    }
  }
}
```

### Recipe B: Hierarchical 3-Level SaaS (`tenant -> team -> user`) with Multi-Gateway Redis Sync
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
        "grace_enabled": false,
        "windows": [
          { "name": "daily", "window_seconds": 86400, "limit": 500, "algorithm": "fixed_window" }
        ]
      },
      "enterprise": {
        "cost_per_req_micro_credits": 500,
        "exhausted_action": "passthrough",
        "grace_enabled": true,
        "windows": [
          { "name": "burst", "window_seconds": 60, "limit": 5000, "algorithm": "token_bucket" },
          { "name": "monthly", "window_seconds": 2592000, "limit": 10000000, "algorithm": "sliding_window" }
        ]
      }
    },
    "etp_id": "redis-metering",
    "key_prefix": "velda:prod",
    "sync_interval_ms": 250,
    "pbww": 20
  }
}
```

---

## 3. Observability Signals

| Path | External Wire Output | Internal Signal / Headers |
| :--- | :--- | :--- |
| **Allowed (with windows)** | Upstream receives `x-metering-tier`, `x-metering-remaining`, `x-metering-window`, `x-metering-balance` | `Action::Continue` |
| **Allowed (`windows: []`)** | Upstream receives `x-metering-tier`, `x-metering-balance` | `Action::Continue` |
| **Overage (`Passthrough`)** | Upstream receives `x-metering-tier`, `x-metering-overage: true`, `x-metering-balance: -...` | `Action::Continue` |
| **Rejected (Missing Account)** | HTTP `400 Bad Request` JSON (`"error":"bad_request"`) | `Action::Respond(400)` (zero shard locks) |
| **Rejected (`Reject402`)** | HTTP `402 Payment Required` JSON (`error`, `message`, `tier`, `window`, `reset_seconds`) + `x-metering-tier`, `x-metering-exhausted-window`, optional `retry-after` | `Action::Respond(402)` |
| **Rejected (`Reject429`)** | HTTP `429 Too Many Requests` + Bit-1 bypass headers (`retry-after`, `x-metering-tier`, `x-metering-exhausted: true`) | `Action::Reject(RejectReason::TooManyRequests)` |

---

## 4. Incident Triage Playbook

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| `ApiMeteringPlugin::from_config` fails at reload | Tier defines `> 4` windows (`MAX_WINDOWS_PER_TIER = 4`), blank/non-string `etp_id`, or invalid header name | Reduce windows per tier to `<= 4`, either omit `etp_id` (local-only) or set a valid `redis` ETP ID from `etp.json`, and verify RFC 9110 header names. |
| Tenant sees `400 Bad Request` (`Missing required tenant account header`) | All headers in `account_headers` were missing or whitespace/empty | Ensure upstream auth plugin (`api_key_auth`, `jwt_auth`, `hmac_auth`) runs before `api_metering` and injects `x-tenant-id`. |
| Distributed balance diverges during Redis outage | Redis TCP unreachable or timing out; `run_shadow_metering_sync` logs warning | Pending deltas are restored via `revert_in_flight_deltas` automatically; verify `etp.json` Redis endpoint connectivity and `sync_interval_ms`. |
