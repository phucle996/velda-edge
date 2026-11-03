# Consumer Quota Plugin (`consumer_quota`)

High-performance, zero-allocation pre-upstream fair-use and member rate-capping plugin designed for LLM APIs, multi-member team organizations, and tiered public consumption. Evaluated immediately before upstream dispatch, enforcing rolling sliding windows, fixed windows, and token buckets with RFC 6585 Bit-1 bypass rejection headers.

---

## 1. Overview & Pipeline Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `consumer_quota` |
| **Canonical Phase** | `pre_upstream` |
| **Execution Order** | Evaluated after routing & client identity extraction (`jwt_auth`/`api_key_auth`), before upstream dispatch |
| **Access Level** | `PluginCapability::header_only(AccessLevel::Read)` (or `Action::Reject` on exhaustion) |
| **Supported Protocols** | `ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC) |
| **Hot-Path Overhead** | Nanosecond-level (~15 ns); 100% Zero-Float, integer division & basis points |
| **Concurrency Model** | 64 cache-aligned stripes (`#[repr(align(64))]`), zero cross-core cache invalidation |
| **Rejection Contract** | `Action::Reject(RejectReason::TooManyRequests)` (`429`) with Bit-1 bypass headers |

---

## 2. Configuration Specification

> [!IMPORTANT]
> **Identity Extraction Invariant**: The plugin reads client identity directly from [`HeaderWorkspace`](file:///home/phucle/Desktop/velda-edge/crates/velda-core/src/buffer/header.rs). Incoming client headers must be authenticated and stamped authoritatively by upstream auth plugins (`jwt_auth`, `api_key_auth`, or `hmac_auth`).

| Field | Type | Required | Constraints / Default | Description |
| :--- | :---: | :---: | :--- | :--- |
| `key_headers` | Array of strings | No | `["x-user-id", "x-consumer-id"]` | Ordered list of header names evaluated to resolve caller identity. If all absent, falls back to `x-forwarded-for`. |
| `key_header` | String | No | Single string shortcut | Alternative single header format (e.g. `"x-member-id"`). |
| `windows` | Array of objects | **Yes** | Array of window definitions | Array of quota limits evaluated simultaneously against the caller. |

### Quota Window Definition Object

| Field | Type | Required | Constraints / Allowed Values | Description |
| :--- | :---: | :---: | :--- | :--- |
| `name` | String | **Yes** | Alphanumeric identifier | Unique name of the window (e.g. `"rolling_5h"`, `"daily_cap"`). |
| `window_seconds` | u64 | **Yes** | $> 0$ | Duration of the accounting window in seconds (e.g. `18000` for 5 hours, `86400` for 1 day). |
| `limit` | u64 | **Yes** | $\ge 1$ | Maximum number of allowed requests within the configured window. |
| `algorithm` | String | **Yes** | `"sliding_window"`, `"fixed_window"`, `"token_bucket"` | Rate counting algorithm applied to this window. |

---

## 3. Supported Algorithms

1. **`sliding_window` (Recommended for AI / Rolling Quotas)**:
   - Uses an integer basis point approximation ($0..10,000$) between current epoch and previous epoch counters.
   - Smooths out boundary burst attacks (e.g., sending all 50 requests at minute 59 and another 50 at minute 01).
   - Zero float math: calculated strictly as `weight_bp = (time_into_epoch * 10_000) / window_seconds`.
2. **`fixed_window` (Standard Daily / Monthly Caps)**:
   - Resets count completely when `now / window_seconds != curr_epoch`.
   - Ultra-fast 1-instruction comparison; ideal for calendar-based resets (midnight daily limits).
3. **`token_bucket` (Burst Smoothing)**:
   - Continuous fractional refill measured in integer nano-tokens ($10^{-9}$ tokens).
   - Allows configurable burst capacity while smoothly rate-limiting sustained loads.

---

## 4. Production Manifest Example (`plugins.json`)

```json
{
  "id": "consumer_quota",
  "config": {
    "key_headers": [
      "x-user-id",
      "x-consumer-id"
    ],
    "windows": [
      {
        "name": "ai_rolling_5h",
        "window_seconds": 18000,
        "limit": 50,
        "algorithm": "sliding_window"
      },
      {
        "name": "daily_member_cap",
        "window_seconds": 86400,
        "limit": 1000,
        "algorithm": "fixed_window"
      }
    ]
  }
}
```

---

## 5. Telemetry & Rejection Headers

### On Allowed Request (`Action::Continue`)
The plugin injects downstream telemetry tags directly into the request header block:
- `x-quota-remaining`: Minimum remaining request count across all active windows.
- `x-quota-window`: Name of the most constrained quota window.

### On Exhausted Request (`Action::Reject`)
When any configured window limit is reached, Velda Edge aborts immediately with `Action::Reject(RejectReason::TooManyRequests)` and injects RFC 6585 Bit-1 bypass headers into downstream response:
- `Retry-After`: Remaining seconds until the constrained window resets or refills.
- `x-quota-remaining`: `0`
- `x-quota-window`: Name of the exhausted window.
