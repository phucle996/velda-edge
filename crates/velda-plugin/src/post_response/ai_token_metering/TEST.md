# AI Token Metering Plugin — Test & Verification Suite

This document outlines the testing architecture, automated test suites, edge-case coverage, and live verification recipes for the `ai_token_metering` plugin.

---

## 1. Testing Architecture & Test Pyramid

Testing for `ai_token_metering` follows a three-tier pyramid:

```
         ▲
        / \     Level 3: Live Azure Distributed Verification
       /   \    (HTTP/1.1, HTTP/2, HTTP/3, Redis Pipelined HINCRBY)
      /-----\
     /       \   Level 2: Plugin Engine & Factory Integration Tests
    /         \  (Header Injection, Strict Invariants, Protocol Flags)
   /-----------\
  /             \ Level 1: In-Memory Unit Tests
 /_______________\ (Scanner, Micro-USD Math, 64-Stripe Ledger)
```

---

## 2. Automated Test Suite Matrix

Automated tests are located in [`tests/ai_token_metering_test.rs`](file:///home/phucle/Desktop/velda-edge/crates/velda-plugin/tests/ai_token_metering_test.rs).

| Test Name | Target Component | Verifications & Invariants |
|---|---|---|
| `test_unary_chat_completion_metering` | Scanner & Plugin Pipeline | Parses standard OpenAI Unary JSON payload. Verifies prompt (1540), completion (320), total (1860), and cached (1024) tokens. Verifies cost computation and telemetry header injection. |
| `test_sse_streaming_chunk_metering` | Reverse Tail Scanner | Tests SSE streaming payload containing multiple `data: {...}` lines with usage in the terminal chunk. Ensures scanner navigates trailing chunk boundaries accurately. |
| `test_prompt_cache_discount_pricing` | Micro-USD Engine | Validates cached token discount formula: $(T_{\text{prompt}} - T_{\text{cached}}) \times R_p + T_{\text{cached}} \times R_c + T_{\text{comp}} \times R_m$. Verifies exact fixed-point integer rounding. |
| `test_strict_unpriced_model_warning` | Strict Compliance Catalog | Validates strict no-fallback invariant: unknown model flags `x-ai-metering-status: unpriced_model`, sets cost to `0`, and records tokens in unpriced audit ledger. |
| `test_sharded_ledger_concurrency_and_deltas` | 64-Stripe Sharded Ledger | Tests multi-threaded recording, 64-stripe hash distribution, false sharing prevention, and atomic delta draining via `drain_deltas()`. |
| `test_zero_usage_or_corrupted_payload` | Fault Resilience & Scanner | Validates behavior on missing `"usage"` block, non-JSON bytes, empty payloads, and truncated chunks without crashing or allocating. Injects `status: no_usage_found`. |

---

## 3. Running Automated Tests

To run the dedicated plugin unit test suite:

```bash
cargo test -p velda-plugin --test ai_token_metering_test -- --nocapture
```

### Complete Subsystem Verification

```bash
# 1. Format verification
cargo fmt --check

# 2. Workspace check
cargo check -p velda-plugin -p velda-edge -p velda-sync

# 3. Comprehensive unit tests across velda-plugin (326 tests)
cargo test -p velda-plugin

# 4. Strict clippy analysis (Zero warnings permitted)
cargo clippy -p velda-plugin -p velda-edge -p velda-sync --all-targets --all-features -- -D warnings
```

---

## 4. Live Cloud Verification Recipes (Azure)

### 4.1 HTTP/1.1 REST Unary Verification

```bash
curl -i -s http://172.16.0.5/v1/chat/completions \
  -H "x-consumer-id: tenant-alpha-01"
```

**Expected Response Headers**:
```http
HTTP/1.1 200 OK
content-type: application/json
x-ai-model: deepseek-chat
x-ai-prompt-tokens: 1540
x-ai-completion-tokens: 320
x-ai-total-tokens: 1860
x-ai-cached-tokens: 1024
x-ai-cost-micro-usd: 175
x-ai-metering-status: success
```

### 4.2 HTTP/2 Verification

```bash
curl -k -i -s --http2 https://172.16.0.5/v1/chat/completions \
  -H "x-consumer-id: tenant-alpha-01"
```

**Expected Response**:
```http
HTTP/2 200
x-ai-model: deepseek-chat
x-ai-prompt-tokens: 1540
x-ai-completion-tokens: 320
x-ai-total-tokens: 1860
x-ai-cached-tokens: 1024
x-ai-cost-micro-usd: 175
x-ai-metering-status: success
```

### 4.3 HTTP/3 (QUIC) Verification

```bash
/usr/local/bin/h3bench -u https://172.16.0.5:443/v1/chat/completions \
  -n 10 -c 1 \
  -H "x-consumer-id: tenant-alpha-01"
```

**Expected Output**:
```
Starting h3bench: requests=10, concurrency=1, target=172.16.0.5:443:/v1/chat/completions
Requests:       10 / 10 (Success: 100%)
Throughput:     > 300 req/s
HTTP 2xx:       10
```

### 4.4 Unpriced Model Strict Compliance Verification

```bash
curl -i -s http://172.16.0.5/v1/chat/completions/unpriced \
  -H "x-consumer-id: tenant-alpha-01"
```

**Expected Response Headers**:
```http
HTTP/1.1 200 OK
x-ai-model: llama-3-70b-instruct
x-ai-prompt-tokens: 2000
x-ai-completion-tokens: 500
x-ai-total-tokens: 2500
x-ai-cost-micro-usd: 0
x-ai-metering-status: unpriced_model
```

### 4.5 Distributed Redis Reconciliation Check

On Redis node (`172.16.0.4`):

```bash
redis-cli HGETALL velda:ai:prompt_tokens
redis-cli HGETALL velda:ai:completion_tokens
redis-cli HGETALL velda:ai:cost_micro_usd
```

**Expected Output**:
Key-value mappings showing consumer hash keys incremented by aggregated prompt tokens, completion tokens, and Micro-USD costs.

---

## 5. Failure Injection & Resilience Verification

| Scenario | Injected Condition | Expected Behavior |
|---|---|---|
| **Truncated Body** | Response cut off mid-stream (`Content-Length` mismatch). | Scanner aborts gracefully; `x-ai-metering-status: no_usage_found` injected; zero panics. |
| **Empty Usage** | `"usage": {}` with no inner fields. | Fields default to `0`; cost evaluates to `0`; ledger remains consistent. |
| **Redis Offline** | Redis stopped (`systemctl stop redis-server`). | Frontline serving worker continues at full speed with 0ms added latency; local ledger retains deltas until Redis returns. |
| **Non-AI Route** | Request hits standard API `/api/v1/users`. | Plugin is not bound on route; zero CPU overhead. |
