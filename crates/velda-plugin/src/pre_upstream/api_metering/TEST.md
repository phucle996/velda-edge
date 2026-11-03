# Verification & Test Specification (`api_metering`)

Automated test suites for verifying multi-tier resolution, micro-credit wallet accounting, 402 short-circuiting, and debt tracking under `crates/velda-plugin/tests/api_metering_test.rs`.

---

## 1. Test Verification Matrix

| Test Identifier | Objective / Verification Scenario | Expected Outcome | Status |
| :--- | :--- | :--- | :---: |
| `test_free_tier_rejection_402` | Verifies free tier quota exhaustion triggers HTTP 402 with JSON diagnostic payload. | Requests 1–2 pass. Request 3 returns `Action::Respond(402)` with `{"error":"payment_required"}` body. | **PASS** |
| `test_pro_tier_rejection_429` | Verifies pro tier exhaustion triggers HTTP 429 with Bit-1 bypass telemetry. | Request 2 rejected with `Action::Reject(TooManyRequests)` and `x-metering-exhausted: true`. | **PASS** |
| `test_enterprise_overage_passthrough` | Verifies pay-as-you-go policy allows requests beyond balance while tracking debt. | Balance drops to negative (`-100000`), request passes with `x-metering-overage: true`. | **PASS** |
| `test_prepaid_micro_credits_deduction` | Verifies exact deduction of micro-credits ($10^{-6}$) and rejection upon balance exhaustion. | Balance decrements `1000000 -> 500000 -> 0`. Request 3 rejected with 402 `credit_balance`. | **PASS** |
| `test_tier_header_resolution` | Verifies casing normalization and fallback to `default_tier` when header is absent. | Case-insensitive match (`"PRO" -> "pro"`) and correct policy resolution. | **PASS** |
| `test_dry_run_isolation` | Verifies that when window 2 fails, window 1 counters and wallet balance are NOT mutated. | Balance and window 1 counters remain intact after dry-run rejection. | **PASS** |

---

## 2. Running Automated Tests

```bash
cargo test -p velda-plugin --test api_metering_test
```
