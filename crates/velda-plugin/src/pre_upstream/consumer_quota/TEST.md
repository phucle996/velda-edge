# Verification & Test Specification (`consumer_quota`)

Automated test suites for verifying functional correctness, boundary limits, and multi-tenant isolation under `crates/velda-plugin/tests/consumer_quota_test.rs`.

---

## 1. Test Verification Matrix

| Test Identifier | Objective / Verification Scenario | Expected Outcome | Status |
| :--- | :--- | :--- | :---: |
| `test_sliding_window_burst` | Verifies rolling sliding window burst consumption and decremental remaining count. | Requests 1–3 pass with remaining `2 -> 1 -> 0`. Request 4 rejected with `429 TooManyRequests`. | **PASS** |
| `test_bit1_bypass_headers` | Verifies RFC 6585 Bit-1 bypass header injection upon quota rejection. | Response contains `Retry-After`, `x-quota-remaining: 0`, and `x-quota-window`. `has_bypass_headers()` is `true`. | **PASS** |
| `test_multi_member_isolation` | Verifies independent rate-limiting buckets for distinct members within a shared tenant space. | Alice exhausting her quota has zero impact on Bob's remaining quota capacity. | **PASS** |
| `test_client_ip_fallback` | Verifies automatic fallback to `x-forwarded-for` when `x-user-id` is absent. | Quota is enforced per client IP address correctly. Distinct IPs have isolated quotas. | **PASS** |
| `test_token_bucket_refill` | Verifies fractional token replenishment across elapsed duration. | Tokens refill based on integer nano-tokens without precision drift. | **PASS** |
| `test_cardinality_lazy_sweep` | Verifies automatic eviction of stale entries inactive for $> 24 \text{ hours}$. | Shard map memory usage remains bounded under high-cardinality simulated traffic. | **PASS** |

---

## 2. Running Automated Tests

```bash
cargo test -p velda-plugin --test consumer_quota_test
```
