# API Metering & Monetization Plugin (`api_metering`)

High-performance, zero-float pre-upstream accounting engine designed for API monetization, multi-tier SLAs (`free`, `pro`, `enterprise`), prepaid wallet balance tracking, and monthly macro-quotas. Evaluated during the `pre_upstream` phase with authoritative rejection (`402 Payment Required` or `429 Too Many Requests`).

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `api_metering` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Pre-upstream FIFO pipeline (evaluated after `jwt_auth` and before upstream dispatch) |
| **Access Level** | `PluginCapability::header_only(AccessLevel::Read)` (or `Action::Respond` 402 on exhaustion) |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Hot-Path Overhead** | Nanosecond-level (~20 ns); 100% Zero-Float, integer micro-credits ($10^{-6}$) |
| **Concurrency Model** | 64 cache-aligned stripes (`#[repr(align(64))]`), atomic CAS |
| **Rejection Contract** | Returns structured JSON `402 Payment Required` or Bit-1 bypass `429 Too Many Requests` |

---

## 2. Configuration Specification

| Field | Type | Required | Constraints / Default | Description |
| :--- | :---: | :---: | :--- | :--- |
| `account_headers` | Array of strings | No | `["x-tenant-id", "x-org-id", "x-account-id"]` | Ordered list of header names evaluated to resolve tenant account identity. |
| `account_header` | String | No | Single string shortcut | Alternative single header format (e.g. `"x-tenant-id"`). |
| `tier_header` | String | No | `"x-plan-tier"` | Header name specifying client service tier (e.g. `"pro"`). |
| `default_tier` | String | No | `"free"` | Fallback tier when tier header is absent or unrecognized. |
| `initial_balance_micro_credits` | i64 | No | `0` | Initial wallet balance credited to newly discovered tenant accounts. |
| `tiers` | Object | **Yes** | Map of tier definitions | Service tier policies indexed by tier name. |

### Tier Policy Object Schema

| Field | Type | Required | Default | Description |
| :--- | :---: | :---: | :--- | :--- |
| `cost_per_req_micro_credits` | u64 | No | `0` | Base cost deducted per request in micro-credits ($1 \text{ credit} = 1,000,000 \mu\text{-credits}$). |
| `exhausted_action` | String | No | `"402"` | Policy executed when quota or balance is depleted: `"402"`, `"429"`, or `"passthrough"`. |
| `windows` | Array of objects | No | `[]` | Macro-level accounting windows (e.g. monthly 1,000,000 requests). Uses same window schema as `consumer_quota`. |

---

## 3. Supported Exhausted Actions

1. **`"402"` / `"reject_402"` (Subscription or Wallet Exhausted)**:
   - Immediately short-circuits with `Action::Respond` carrying HTTP status `402 Payment Required`.
   - Returns standardized diagnostic JSON payload:
     ```json
     {
       "error": "payment_required",
       "message": "API tier quota or prepaid credit balance exhausted",
       "tier": "free",
       "window": "monthly_allowance",
       "reset_seconds": 182340
     }
     ```
   - Sets headers: `content-type: application/json; charset=utf-8`, `x-metering-tier`, and `retry-after`.
2. **`"429"` / `"too_many_requests"` (Temporary Window Cooldown)**:
   - Rejects with `Action::Reject(RejectReason::TooManyRequests)` and tags Bit-1 bypass headers (`x-metering-tier`, `x-metering-exhausted`, `Retry-After`).
3. **`"passthrough"` / `"overage"` (Pay-as-you-go Debt Billing)**:
   - Permits request through to upstream backend and records negative credit balance (`negative_balance`).
   - Injects downstream telemetry header: `x-metering-overage: true` and `x-metering-balance: -100000`.

---

## 4. Production Manifest Example (`plugins.json`)

```json
{
  "id": "api_metering",
  "config": {
    "account_headers": [
      "x-tenant-id",
      "x-org-id"
    ],
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
      },
      "pro": {
        "cost_per_req_micro_credits": 100,
        "exhausted_action": "429",
        "windows": [
          {
            "name": "monthly_cap",
            "window_seconds": 2592000,
            "limit": 1000000,
            "algorithm": "sliding_window"
          }
        ]
      },
      "enterprise": {
        "cost_per_req_micro_credits": 100,
        "exhausted_action": "passthrough",
        "windows": []
      }
    }
  }
}
```

---

## 5. Telemetry & Upstream Forwarding Headers

- `x-metering-tier`: Canonical tier under which the request was authenticated (e.g. `pro`).
- `x-metering-remaining`: Remaining quota requests available in current accounting cycle.
- `x-metering-balance`: Current wallet balance in micro-credits (can be negative for overage).
- `x-metering-overage`: Injected as `true` when request is permitted under overage debt contract.
