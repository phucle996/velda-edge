//! Integration & Unit Test Suite for OpenAI Token Quota Plugin (`openai_token_quota`).

use http::header::HeaderName;
use http::{HeaderValue, StatusCode};
use std::sync::Arc;
use velda_core::{HeaderWorkspace, L7Response};
use velda_plugin::PluginConfig;
use velda_plugin::post_response::openai_token_quota::{OpenAiTokenQuotaPlugin, scan_token_usage};
use velda_plugin::post_response::{PostResponsePlugin, run_post_response_plugins};

#[test]
fn test_scanner_openai_unary_json() {
    let json_body = br#"{
        "id": "chatcmpl-8vXYZ123",
        "object": "chat.completion",
        "created": 1708890000,
        "model": "deepseek-chat",
        "choices": [
            {"index": 0, "message": {"role": "assistant", "content": "Hello!"}}
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

    let usage = scan_token_usage(json_body).expect("Usage must be parsed");
    assert_eq!(usage.prompt_tokens, 1540);
    assert_eq!(usage.completion_tokens, 320);
    assert_eq!(usage.total_tokens, 1860);
    assert_eq!(usage.cached_prompt_tokens, 1024);
}

#[test]
fn test_scanner_openai_sse_streaming() {
    let sse_stream = b"\
data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n\
data: {\"id\":\"1\",\"choices\":[],\"usage\":{\"prompt_tokens\":45,\"completion_tokens\":12,\"total_tokens\":57}}\n\n\
data: [DONE]\n\n";

    let usage = scan_token_usage(sse_stream).expect("SSE tail usage must be parsed");
    assert_eq!(usage.prompt_tokens, 45);
    assert_eq!(usage.completion_tokens, 12);
    assert_eq!(usage.total_tokens, 57);
    assert_eq!(usage.cached_prompt_tokens, 0);
}

#[test]
fn test_plugin_on_response_hierarchical_shared_arena() {
    velda_core::clock::init_clock();
    let cfg = PluginConfig {
        id: "openai_token_quota".into(),
        phase: "post_response".into(),
        config: serde_json::json!({
            "consumer_id_headers": ["x-tenant-id", "x-user-id"],
            "ledger_slots": 4096
        }),
    };

    let plugin = Arc::new(OpenAiTokenQuotaPlugin::from_config(&cfg).unwrap());
    let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin.clone()];

    let response_json = br#"{
        "id": "chatcmpl-test-1",
        "object": "chat.completion",
        "model": "deepseek-chat",
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
    let mut req_ws = HeaderWorkspace::default();
    req_ws.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("abc"),
    );
    req_ws.insert(
        HeaderName::from_static("x-user-id"),
        HeaderValue::from_static("123"),
    );

    run_post_response_plugins(&chain, &req_ws, &mut resp);

    assert_eq!(resp.headers.get("x-ai-metering-status").unwrap(), "success");
    assert_eq!(resp.headers.get("x-ai-prompt-tokens").unwrap(), "1540");
    assert_eq!(resp.headers.get("x-ai-completion-tokens").unwrap(), "320");
    assert_eq!(resp.headers.get("x-ai-total-tokens").unwrap(), "1860");
    assert_eq!(resp.headers.get("x-ai-cached-tokens").unwrap(), "1024");
    assert!(resp.headers.get("x-ai-cost-micro-usd").is_none());

    let (prompt, completion, total, cached, _rem) = plugin
        .ledger
        .read_consumer("abc.123")
        .expect("Hierarchical consumer usage must be recorded");
    assert_eq!((prompt, completion, total, cached), (1540, 320, 1860, 1024));
}

#[test]
fn test_distributed_mode_continues_local_counting_when_redis_disconnected() {
    velda_core::clock::init_clock();
    let cfg = PluginConfig {
        id: "openai_token_quota".into(),
        phase: "post_response".into(),
        config: serde_json::json!({
            "mode": "distributed",
            "consumer_id_headers": ["x-tenant-id", "x-user-id"],
            "redis_url": "redis://127.0.0.1:6399",
            "micro_quota_batch": 5000,
            "refill_watermark": 1000
        }),
    };
    let plugin = Arc::new(OpenAiTokenQuotaPlugin::from_config(&cfg).unwrap());
    let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin.clone()];

    let response_json =
        br#"{"usage":{"prompt_tokens":100,"completion_tokens":50,"total_tokens":150}}"#;
    let mut resp = L7Response::from_bytes(StatusCode::OK, &response_json[..]);
    let mut req_ws = HeaderWorkspace::default();
    req_ws.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("abc"),
    );
    req_ws.insert(
        HeaderName::from_static("x-user-id"),
        HeaderValue::from_static("123"),
    );

    run_post_response_plugins(&chain, &req_ws, &mut resp);

    assert_eq!(resp.headers.get("x-ai-total-tokens").unwrap(), "150");
    assert_eq!(resp.headers.get("x-ai-cached-tokens").unwrap(), "0");
    let (_p, _c, total, _ca, rem) = plugin.ledger.read_consumer("abc.123").unwrap();
    assert_eq!(total, 150);
    assert_eq!(rem, -150);
}

#[test]
fn test_plugin_zero_cached_tokens_emits_zero_unconditionally() {
    velda_core::clock::init_clock();
    let cfg = PluginConfig {
        id: "openai_token_quota".into(),
        phase: "post_response".into(),
        config: serde_json::json!({
            "consumer_id_headers": ["x-consumer-id"]
        }),
    };
    let plugin = Arc::new(OpenAiTokenQuotaPlugin::from_config(&cfg).unwrap());
    let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin.clone()];

    let response_json = br#"{
        "id": "chatcmpl-2",
        "model": "qwen-2.5-coder-32b",
        "usage": {
            "prompt_tokens": 2000,
            "completion_tokens": 500,
            "total_tokens": 2500
        }
    }"#;

    let mut resp = L7Response::from_bytes(StatusCode::OK, &response_json[..]);
    let mut req_ws = HeaderWorkspace::default();
    req_ws.insert(
        HeaderName::from_static("x-consumer-id"),
        HeaderValue::from_static("tenant-corp-beta"),
    );

    run_post_response_plugins(&chain, &req_ws, &mut resp);

    assert_eq!(resp.headers.get("x-ai-metering-status").unwrap(), "success");
    assert_eq!(resp.headers.get("x-ai-prompt-tokens").unwrap(), "2000");
    assert_eq!(resp.headers.get("x-ai-completion-tokens").unwrap(), "500");
    assert_eq!(resp.headers.get("x-ai-total-tokens").unwrap(), "2500");
    assert_eq!(resp.headers.get("x-ai-cached-tokens").unwrap(), "0");
    assert!(resp.headers.get("x-ai-cost-micro-usd").is_none());
}

#[test]
fn test_plugin_no_usage_in_body() {
    velda_core::clock::init_clock();
    let cfg = PluginConfig {
        id: "openai_token_quota".into(),
        phase: "post_response".into(),
        config: serde_json::json!({
            "consumer_id_headers": ["x-consumer-id"]
        }),
    };
    let plugin = Arc::new(OpenAiTokenQuotaPlugin::from_config(&cfg).unwrap());
    let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin.clone()];

    let response_json = br#"{"error": {"message": "Rate limit reached", "type": "tokens"}}"#;
    let mut resp = L7Response::from_bytes(StatusCode::TOO_MANY_REQUESTS, &response_json[..]);
    let req_ws = HeaderWorkspace::default();

    run_post_response_plugins(&chain, &req_ws, &mut resp);

    assert_eq!(
        resp.headers.get("x-ai-metering-status").unwrap(),
        "no_usage_found"
    );
    assert!(resp.headers.get("x-ai-prompt-tokens").is_none());
}

#[tokio::test]
async fn test_pbww_and_run_background_shutdown() {
    velda_core::clock::init_clock();

    let local_cfg = PluginConfig {
        id: "openai_token_quota".into(),
        phase: "post_response".into(),
        config: serde_json::json!({
            "mode": "local",
            "consumer_id_headers": ["x-consumer-id"]
        }),
    };
    let local_plugin = Arc::new(OpenAiTokenQuotaPlugin::from_config(&local_cfg).unwrap());
    assert_eq!(local_plugin.pbww(), 0);
    let (tx_local, rx_local) = tokio::sync::watch::channel(false);
    assert!(local_plugin.run_background(rx_local).is_none());
    drop(tx_local);

    let dist_cfg = PluginConfig {
        id: "openai_token_quota".into(),
        phase: "post_response".into(),
        config: serde_json::json!({
            "mode": "distributed",
            "consumer_id_headers": ["x-consumer-id"],
            "redis_url": "redis://127.0.0.1:6399",
            "shadow_weight": 35,
            "sync_interval_ms": 50
        }),
    };
    let dist_plugin = Arc::new(OpenAiTokenQuotaPlugin::from_config(&dist_cfg).unwrap());
    assert_eq!(dist_plugin.pbww(), 35);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let fut = dist_plugin
        .run_background(shutdown_rx)
        .expect("distributed mode must expose a background future");
    let handle = tokio::spawn(fut);

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let _ = shutdown_tx.send(true);
    let res = handle.await.expect("background task must join cleanly");
    assert!(res.is_ok());
}
