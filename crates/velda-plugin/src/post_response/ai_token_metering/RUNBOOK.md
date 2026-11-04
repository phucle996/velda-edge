# AI Token Metering & Monetization Plugin (`ai_token_metering`) — Operator & Configuration Runbook

This runbook provides complete operational guidance, configuration reference, production pricing catalogs, Redis distributed aggregation setups, and monitoring procedures for the **`ai_token_metering`** post-response plugin in Velda Edge.

---

## 1. Configuration Reference (`plugins.json`)

The `ai_token_metering` plugin is declared under the `plugins` array in `plugins.json`:

```json
{
  "plugins": [
    {
      "id": "ai_token_meter",
      "plugin": "ai_token_metering",
      "config": {
        "consumer_id_headers": [
          "x-consumer-id",
          "x-tenant-id",
          "x-user-id",
          "x-api-key"
        ],
        "inject_telemetry_headers": true,
        "models": {
          "deepseek-chat": {
            "prompt_micro_usd_per_million": 140000,
            "completion_micro_usd_per_million": 280000,
            "cached_prompt_micro_usd_per_million": 14000
          },
          "deepseek-reasoner": {
            "prompt_micro_usd_per_million": 550000,
            "completion_micro_usd_per_million": 2190000,
            "cached_prompt_micro_usd_per_million": 140000
          },
          "gpt-4o": {
            "prompt_micro_usd_per_million": 2500000,
            "completion_micro_usd_per_million": 10000000,
            "cached_prompt_micro_usd_per_million": 1250000
          },
          "gpt-4o-mini": {
            "prompt_micro_usd_per_million": 150000,
            "completion_micro_usd_per_million": 600000,
            "cached_prompt_micro_usd_per_million": 75000
          },
          "claude-3-5-sonnet": {
            "prompt_micro_usd_per_million": 3000000,
            "completion_micro_usd_per_million": 15000000,
            "cached_prompt_micro_usd_per_million": 300000
          }
        },
        "distributed": {
          "enabled": true,
          "redis_addr": "172.16.0.4:6379",
          "sync_interval_ms": 1000,
          "key_prefix": "velda"
        }
      }
    }
  ]
}
```

### Parameter Specification

| Field | Type | Required | Constraints | Recommended | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `consumer_id_headers` | `array[string]` | **Yes** | 1 to 8 header names | `["x-consumer-id", "x-tenant-id"]` | Ordered list of request headers inspected to identify the tenant. Falls back to `"anonymous"` if all missing. |
| `inject_telemetry_headers`| `boolean` | **Yes** | `true` or `false` | `true` | Injects downstream `x-ai-*` headers formatted via stack-allocated `itoa`. |
| `models` | `map[string, object]`| **Yes** | Non-empty map of model rate objects | See catalog below | Model pricing rates in Micro-USD ($10^{-6}\text{ USD}$) per 1,000,000 tokens. |
| `models.<model>.prompt_micro_usd_per_million` | `integer` | **Yes** | $\ge 0$ | e.g. `2500000` (\$2.50 / 1M) | Cost per 1,000,000 uncached prompt tokens in Micro-USD. |
| `models.<model>.completion_micro_usd_per_million` | `integer` | **Yes** | $\ge 0$ | e.g. `10000000` (\$10.00 / 1M) | Cost per 1,000,000 completion tokens in Micro-USD. |
| `models.<model>.cached_prompt_micro_usd_per_million` | `integer` | **Yes** | $\ge 0$ | e.g. `1250000` (\$1.25 / 1M) | Cost per 1,000,000 cached prompt tokens in Micro-USD. |
| `distributed.enabled` | `boolean` | **Yes** | `true` or `false` | `true` (prod), `false` (dev) | Enables background Redis reconciliation thread (`velda-ai-sync`). |
| `distributed.redis_addr` | `string` | Cond. | Valid `host:port` | `"127.0.0.1:6379"` | TCP target address for distributed Redis aggregation. |
| `distributed.sync_interval_ms`| `integer` | Cond. | $100..=60000$ | `1000` | Background reconciliation interval in milliseconds. |
| `distributed.key_prefix` | `string` | Cond. | 1 to 32 characters | `"velda"` | Prefix for Redis hash keys (`<prefix>:ai:usage:<consumer>`). |

---

## 2. Micro-USD Currency & Precision Model

To guarantee financial auditing consistency, all rates and totals are stored as **Micro-USD** ($1\ \mu\text{USD} = \$10^{-6}\ \text{USD} = \$0.000001$).

### Conversion Formula
$$\text{Micro-USD} = \text{USD Price} \times 1{,}000{,}000$$

| Model | Official Provider Price / 1M | Micro-USD Rate per 1,000,000 |
| :--- | :--- | :--- |
| **DeepSeek Chat Prompt** | $\$0.14\ / \text{1M}$ | `140000` |
| **DeepSeek Chat Cached Prompt** | $\$0.014\ / \text{1M}$ | `14000` |
| **DeepSeek Chat Completion** | $\$0.28\ / \text{1M}$ | `280000` |
| **GPT-4o Prompt** | $\$2.50\ / \text{1M}$ | `2500000` |
| **GPT-4o Cached Prompt** | $\$1.25\ / \text{1M}$ | `1250000` |
| **GPT-4o Completion** | $\$10.00\ / \text{1M}$ | `10000000` |
| **Claude 3.5 Sonnet Prompt** | $\$3.00\ / \text{1M}$ | `3000000` |
| **Claude 3.5 Sonnet Completion** | $\$15.00\ / \text{1M}$ | `15000000` |

### Billing Calculation Formula
$$\text{Cost} = \left\lfloor \frac{(T_{\text{prompt}} - T_{\text{cached}}) \times R_p + T_{\text{cached}} \times R_c + T_{\text{comp}} \times R_m}{1{,}000{,}000} \right\rfloor$$

All arithmetic is evaluated in 128-bit unsigned integers (`u128`), completely preventing integer overflow and IEEE-754 floating-point drift.

---

## 3. Strict No-Fallback Auditing Policy

Velda Edge strictly rejects silent fallbacks or implicit pricing assumptions:
1. **Unpriced Models**: If an upstream provider returns a model not defined in `models` (e.g. `gpt-4o-unknown`):
   - The response is marked with header: `x-ai-metering-status: unpriced_model`.
   - Charge is set to `$0` (`x-ai-cost-micro-usd: 0`).
   - All raw token counts (`prompt`, `completion`, `total`, `cached`) are still recorded in full for auditing.
   - Counter `unpriced_requests_total` is incremented to trigger SRE alerts.
2. **Missing Usage Block**: If the response is truncated or upstream omits the usage envelope:
   - Header is set: `x-ai-metering-status: no_usage_found`.
   - No cost is recorded.

---

## 4. Injected Telemetry Headers Reference

When `inject_telemetry_headers: true` is configured, downstream responses receive:

| Header Name | Type | Example | Description |
| :--- | :--- | :--- | :--- |
| `x-ai-model` | String | `deepseek-chat` | Resolved canonical model name. |
| `x-ai-prompt-tokens` | Integer | `1540` | Total prompt tokens consumed. |
| `x-ai-completion-tokens` | Integer | `320` | Completion tokens produced. |
| `x-ai-total-tokens` | Integer | `1860` | Aggregate token total. |
| `x-ai-cached-tokens` | Integer | `1024` | Prompt tokens served from cache (omitted if 0). |
| `x-ai-cost-micro-usd` | Integer | `318` | Total request cost in Micro-USD (\$0.000318). |
| `x-ai-metering-status` | Enum | `success` | `success`, `unpriced_model`, or `no_usage_found`. |

---

## 5. Distributed Redis Aggregation Setup

The plugin uses an autonomous background companion thread (`velda-ai-sync`) running under Linux `SCHED_IDLE` to reconcile local 64-stripe ledger deltas to distributed Redis:

```
[ Frontline Worker Core 0 ] ──► Shard 0 (Lock < 10ns)
[ Frontline Worker Core 1 ] ──► Shard 1 (Lock < 10ns)
                                    │
                                (Every 1000ms)
                                    ▼
       [ Shadow Worker Thread: velda-ai-sync (SCHED_IDLE) ]
                                    │
                         (Pipelined HINCRBY Commands)
                                    ▼
       [ Redis Hash: <prefix>:ai:usage:<consumer_id> ]
       • requests              • prompt_tokens
       • completion_tokens     • cached_tokens
       • total_tokens          • cost_micro_usd
```

### Redis Key Schema
- **Key**: `<key_prefix>:ai:usage:<consumer_id>` (e.g. `velda:ai:usage:tenant-alpha-01`)
- **Type**: Redis Hash (`HINCRBY`)
- **Fields**:
  - `requests`: Total completed requests count
  - `prompt_tokens`: Cumulative prompt tokens
  - `completion_tokens`: Cumulative completion tokens
  - `cached_tokens`: Cumulative cached prompt tokens
  - `total_tokens`: Total tokens sum
  - `cost_micro_usd`: Accumulated expenses in Micro-USD

---

## 6. Route Attachment (`routes.json`)

Attach `ai_token_meter` to OpenAI, DeepSeek, or Anthropic proxy routes:

```json
{
  "routes": [
    {
      "id": "route_openai_proxy",
      "match": {
        "path": {
          "prefix": "/v1/chat/completions"
        }
      },
      "plugins": [
        "waf_guard",
        "consumer_quota",
        "ai_token_meter"
      ],
      "upstream": "upstream_openai"
    }
  ]
}
```

---

## 7. Metrics & SRE Alerting Rules

The plugin exposes atomic telemetry counters for operational health:

| Metric | Type | Description | Alert Condition |
| :--- | :--- | :--- | :--- |
| `ai_token_metering.metered_requests_total` | Counter | Total successful requests metered and priced. | Monitor trend vs total traffic |
| `ai_token_metering.metered_tokens_total` | Counter | Total tokens audited across all models. | Volume spikes |
| `ai_token_metering.metered_cost_micro_usd_total` | Counter | Total revenue/cost tracked in Micro-USD. | Financial accounting |
| `ai_token_metering.unpriced_requests_total` | Counter | Requests using models missing from catalog. | **`rate > 0` $\to$ Trigger SRE alert** |
| `ai_token_metering.unpriced_tokens_total` | Counter | Tokens consumed by unpriced models. | Investigate unbilled usage |

### Prometheus Alerting Rule Example
```yaml
groups:
  - name: velda_ai_metering
    rules:
      - alert: AIModelUnpricedTrafficDetected
        expr: rate(ai_token_metering_unpriced_requests_total[5m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Unpriced AI model traffic detected on Velda Edge"
          description: "Traffic is using models not present in the pricing catalog. Add the model to plugins.json to prevent $0 billing."
```

---

## 8. Sizing & Memory Footprint

The memory footprint of `ai_token_metering` is bounded and independent of request throughput:
- **64-Stripe In-Memory Ledger**: 64 shards $\times 64$ bytes alignment $= 4,096\ \text{bytes}$ ($4\ \text{KB}$ RAM).
- **Pricing Catalog**: $< 10\ \text{KB}$ RAM for 100 model definitions.
- **Reverse Tail Window Scanner**: $0\ \text{B}$ heap memory (borrows trailing 4KB slice of response buffer in place).
- **Total RAM Footprint**: **$< 35\ \text{MB}$** under full 30,000 RPS workload.
