# Operations Runbook (`openai_token_quota`)

## 1. Deployment & Configuration

1. Ensure `consumer_id_headers` is explicitly configured in `plugins.json` (no default header list is assumed):

```json
{
  "plugins": [
    {
      "id": "openai_token_quota",
      "config": {
        "consumer_id_headers": ["x-tenant-id", "x-user-id"],
        "ledger_slots": 16384
      }
    }
  ]
}
```

2. Size `ledger_slots` (128 bytes per slot, rounded up to the next power of 2 in `64..=1048576`):
   - `4096` slots = `512 KB` (`128` OS pages)
   - `16384` slots = `2 MB` (`512` OS pages)
   - `65536` slots = `8 MB` (`2048` OS pages)

---

## 2. Observability & Counters

- `counted_requests_total`: Total responses where a `"usage"` object was extracted and recorded.
- `counted_tokens_total`: Cumulative `total_tokens` recorded across all consumers.
- `SharedUsageArena::read_consumer("tenant.user")`: Returns `(prompt_tokens, completion_tokens, total_tokens, cached_tokens)` in bounded O(1) (`<= 8` cache-line probes).

---

## 3. Troubleshooting

| Symptom | Cause | Action |
| :--- | :--- | :--- |
| `x-ai-metering-status: no_usage_found` | Upstream response omitted `"usage"` (e.g. error JSON or SSE without `stream_options.include_usage: true`) or streaming pipeline executed before body completion. | Verify upstream payload includes `"usage"` on buffered routes, or enable `include_usage: true` on OpenAI requests. |
| Config rejected at sync/startup | `consumer_id_headers` missing/empty, invalid RFC 9110 header name, or `ledger_slots` outside `64..=1048576`. | Fix `plugins.json` and re-publish snapshot. |
| Frequent slot evictions in `SharedUsageArena` | Active concurrent `tenant.user` keys exceed `ledger_slots` load factor. | Increase `ledger_slots` to the next power of 2 (e.g. `16384` or `65536`). |
