# Verification & Test Specification (`openai_token_quota`)

Automated unit and integration tests for `openai_token_quota` under `crates/velda-plugin/src/post_response/openai_token_quota/` and [openai_token_quota_test.rs](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/background_plugin_development/crates/velda-plugin/tests/openai_token_quota_test.rs).

## Test Matrix

| Test Name | Scope | Expected Behavior | Status |
| :--- | :--- | :--- | :--- |
| `test_valid_config_normalizes_headers_and_slots` | `validation.rs` | Lowercases/trims `consumer_id_headers` and rounds `ledger_slots` to next power of 2. | **PASS** |
| `test_rejects_missing_or_empty_consumer_id_headers` | `validation.rs` | Rejects missing/empty `consumer_id_headers` and `ledger_slots < 64`. | **PASS** |
| `test_scan_unary_and_sse_single_pass` | `scanner.rs` | Extracts unary OpenAI JSON and SSE tail `"usage"` tokens in a single pass. | **PASS** |
| `test_openai_token_quota_emits_4_token_headers_without_cost` | `plugin.rs` | Emits 4 token headers (`x-ai-cached-tokens: "0"` when 0), `x-ai-metering-status: success`, no cost header. | **PASS** |
| `test_hierarchical_consumer_id_accumulates_in_shared_arena` | `plugin.rs` | Combines `x-tenant-id: abc` + `x-user-id: 123` into `abc.123` and accumulates tokens in `SharedUsageArena`. | **PASS** |
| `test_scanner_openai_unary_json` | Integration | Parses OpenAI unary JSON with `prompt_tokens_details.cached_tokens`. | **PASS** |
| `test_scanner_openai_sse_streaming` | Integration | Parses SSE tail event before `data: [DONE]`. | **PASS** |
| `test_plugin_on_response_hierarchical_shared_arena` | Integration | Full `run_post_response_plugins` chain updates `SharedUsageArena` for `abc.123`. | **PASS** |
| `test_plugin_zero_cached_tokens_emits_zero_unconditionally` | Integration | Emits `x-ai-cached-tokens: "0"` unconditionally when cached tokens = 0. | **PASS** |
| `test_plugin_no_usage_in_body` | Integration | Sets `x-ai-metering-status: no_usage_found` and omits token headers when no usage object exists. | **PASS** |
| `test_distributed_mode_continues_local_counting_when_redis_disconnected` | Integration | Proves Frontline Worker continues deducting local tokens without blocking when Redis is unreachable. | **PASS** |

## Execution Commands

```bash
cargo test -p velda-plugin -- openai_token_quota
cargo test -p velda-plugin --test openai_token_quota_test
```
