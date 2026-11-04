//! Comprehensive Unit Test Suite for AI Token Metering Plugin (`ai_token_metering`).

use http::header::HeaderName;
use http::{HeaderValue, StatusCode};
use std::sync::Arc;
use velda_core::{HeaderWorkspace, L7Response};
use velda_plugin::post_response::ai_token_metering::{
    AiTokenMeteringConfig, AiTokenMeteringPlugin, ModelPricing, scan_model, scan_token_usage,
};
use velda_plugin::post_response::{PostResponsePlugin, run_post_response_plugins};

#[test]
fn test_scanner_openai_unary_json() {
    let json_body = br#"{
        "id": "chatcmpl-8vXYZ123",
        "object": "chat.completion",
        "created": 1708890000,
        "model": "deepseek-chat",
        "system_fingerprint": "fp_abc123",
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Here is the optimized function in Rust:\n```rust\npub fn solve() {}\n```"
                },
                "logprobs": null,
                "finish_reason": "stop"
            }
        ],
        "usage": {
            "prompt_tokens": 1540,
            "completion_tokens": 320,
            "total_tokens": 1860,
            "prompt_tokens_details": {
                "cached_tokens": 1024
            }
        }
    }"#;

    let model = scan_model(json_body);
    assert_eq!(model, Some("deepseek-chat"));

    let usage = scan_token_usage(json_body).expect("Token usage must be found");
    assert_eq!(usage.prompt_tokens, 1540);
    assert_eq!(usage.completion_tokens, 320);
    assert_eq!(usage.total_tokens, 1860);
    assert_eq!(usage.cached_prompt_tokens, 1024);
}

#[test]
fn test_scanner_openai_sse_streaming() {
    let sse_body = br#"data: {"id":"chatcmpl-chunk-1","object":"chat.completion.chunk","created":1708890000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}

data: {"id":"chatcmpl-chunk-2","object":"chat.completion.chunk","created":1708890000,"model":"gpt-4o","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}

data: {"id":"chatcmpl-chunk-3","object":"chat.completion.chunk","created":1708890000,"model":"gpt-4o","choices":[],"usage":{"prompt_tokens":850,"completion_tokens":150,"total_tokens":1000,"prompt_tokens_details":{"cached_tokens":400}}}

data: [DONE]

"#;

    let model = scan_model(sse_body);
    assert_eq!(model, Some("gpt-4o"));

    let usage = scan_token_usage(sse_body).expect("Token usage must be found in SSE chunks");
    assert_eq!(usage.prompt_tokens, 850);
    assert_eq!(usage.completion_tokens, 150);
    assert_eq!(usage.total_tokens, 1000);
    assert_eq!(usage.cached_prompt_tokens, 400);
}

#[test]
fn test_pricing_calculation() {
    // deepseek-chat pricing:
    // Prompt: $0.14 / 1M ($140,000 micro-USD)
    // Cached: $0.014 / 1M ($14,000 micro-USD)
    // Completion: $0.28 / 1M ($280,000 micro-USD)
    let pricing = ModelPricing {
        prompt_micro_usd_per_million: 140_000,
        cached_prompt_micro_usd_per_million: 14_000,
        completion_micro_usd_per_million: 280_000,
    };

    // 1540 prompt (1024 cached, 516 uncached), 320 completion
    // uncached: 516 * 140_000 / 1_000_000 = 72
    // cached: 1024 * 14_000 / 1_000_000 = 14
    // comp: 320 * 280_000 / 1_000_000 = 89
    // total: 72 + 14 + 89 = 175 micro-USD
    let cost = pricing.compute_cost_micro_usd(1540, 320, 1024);
    assert_eq!(cost, 175);
}

#[test]
fn test_plugin_on_response_success_metering() {
    let mut config = AiTokenMeteringConfig::default();
    config.model_pricing.insert(
        "deepseek-chat".to_string(),
        ModelPricing {
            prompt_micro_usd_per_million: 140_000,
            cached_prompt_micro_usd_per_million: 14_000,
            completion_micro_usd_per_million: 280_000,
        },
    );

    let plugin = Arc::new(AiTokenMeteringPlugin::new("ai-meter-1", config));
    let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin.clone()];

    let response_json = br#"{
        "id": "chatcmpl-test-1",
        "object": "chat.completion",
        "created": 1708890000,
        "model": "deepseek-chat",
        "choices": [],
        "usage": {
            "prompt_tokens": 1540,
            "completion_tokens": 320,
            "total_tokens": 1860,
            "prompt_tokens_details": {
                "cached_tokens": 1024
            }
        }
    }"#;

    let mut resp = L7Response::from_bytes(StatusCode::OK, &response_json[..]);
    let mut req_ws = HeaderWorkspace::empty();
    req_ws.insert(
        HeaderName::from_static("x-consumer-id"),
        HeaderValue::from_static("tenant-corp-alpha"),
    );

    run_post_response_plugins(&chain, &req_ws, &mut resp);

    // Verify injected telemetry headers
    assert_eq!(resp.headers.get("x-ai-metering-status").unwrap(), "success");
    assert_eq!(resp.headers.get("x-ai-model").unwrap(), "deepseek-chat");
    assert_eq!(resp.headers.get("x-ai-prompt-tokens").unwrap(), "1540");
    assert_eq!(resp.headers.get("x-ai-completion-tokens").unwrap(), "320");
    assert_eq!(resp.headers.get("x-ai-total-tokens").unwrap(), "1860");
    assert_eq!(resp.headers.get("x-ai-cached-tokens").unwrap(), "1024");
    assert_eq!(resp.headers.get("x-ai-cost-micro-usd").unwrap(), "175");

    // Verify recorded ledger state
    let usage = plugin
        .get_consumer_usage("tenant-corp-alpha")
        .expect("Consumer usage must be recorded");
    assert_eq!(usage.total_prompt_tokens, 1540);
    assert_eq!(usage.total_completion_tokens, 320);
    assert_eq!(usage.total_cached_tokens, 1024);
    assert_eq!(usage.total_cost_micro_usd, 175);
}

#[test]
fn test_plugin_unpriced_model_strict_no_fallback() {
    let config = AiTokenMeteringConfig::default();
    let plugin = Arc::new(AiTokenMeteringPlugin::new("ai-meter-1", config));
    let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin.clone()];

    // Response from an unpriced model e.g. "qwen-2.5-coder-32b"
    let response_json = br#"{
        "id": "chatcmpl-unpriced-1",
        "object": "chat.completion",
        "created": 1708890000,
        "model": "qwen-2.5-coder-32b",
        "choices": [],
        "usage": {
            "prompt_tokens": 2000,
            "completion_tokens": 500,
            "total_tokens": 2500,
            "prompt_tokens_details": {
                "cached_tokens": 0
            }
        }
    }"#;

    let mut resp = L7Response::from_bytes(StatusCode::OK, &response_json[..]);
    let mut req_ws = HeaderWorkspace::empty();
    req_ws.insert(
        HeaderName::from_static("x-consumer-id"),
        HeaderValue::from_static("tenant-corp-beta"),
    );

    run_post_response_plugins(&chain, &req_ws, &mut resp);

    // STRICT INVARIANT:
    // Status must be unpriced_model, cost must be 0, model must be qwen-2.5-coder-32b (NO FALLBACK)
    assert_eq!(
        resp.headers.get("x-ai-metering-status").unwrap(),
        "unpriced_model"
    );
    assert_eq!(
        resp.headers.get("x-ai-model").unwrap(),
        "qwen-2.5-coder-32b"
    );
    assert_eq!(resp.headers.get("x-ai-prompt-tokens").unwrap(), "2000");
    assert_eq!(resp.headers.get("x-ai-completion-tokens").unwrap(), "500");
    assert_eq!(resp.headers.get("x-ai-total-tokens").unwrap(), "2500");
    assert_eq!(resp.headers.get("x-ai-cost-micro-usd").unwrap(), "0");

    // Token audit must be recorded with 0 cost
    let usage = plugin
        .get_consumer_usage("tenant-corp-beta")
        .expect("Consumer usage must be recorded");
    assert_eq!(usage.total_prompt_tokens, 2000);
    assert_eq!(usage.total_completion_tokens, 500);
    assert_eq!(usage.total_cost_micro_usd, 0);

    // Unpriced metric counter must be incremented
    assert_eq!(
        plugin
            .unpriced_requests_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        plugin
            .unpriced_tokens_total
            .load(std::sync::atomic::Ordering::Relaxed),
        2500
    );
}

#[test]
fn test_plugin_no_usage_in_body() {
    let config = AiTokenMeteringConfig::default();
    let plugin = Arc::new(AiTokenMeteringPlugin::new("ai-meter-1", config));
    let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin.clone()];

    let response_json = br#"{"error": {"message": "Rate limit reached", "type": "tokens"}}"#;
    let mut resp = L7Response::from_bytes(StatusCode::TOO_MANY_REQUESTS, &response_json[..]);
    let req_ws = HeaderWorkspace::empty();

    run_post_response_plugins(&chain, &req_ws, &mut resp);

    assert_eq!(
        resp.headers.get("x-ai-metering-status").unwrap(),
        "no_usage_found"
    );
    assert!(resp.headers.get("x-ai-prompt-tokens").is_none());
}
