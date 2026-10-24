//! End-to-End Test Suite for gRPC Fault Injection Plugin (`pre_upstream: grpc_fault_injection`).
//!
//! Verifies complete chaos engineering lifecycles for gRPC over HTTP/2:
//! - Canonical Trailers-Only abort responses (`HTTP 200 OK`, `application/grpc`, `grpc-status`, `grpc-message`)
//! - Synthetic latency injection (`Action::Delay`)
//! - Combined latency and error abort
//! - Clean pass-through when disabled
//! - Pipeline integration with `build_route_plugin_plan` and `run_pre_upstream_plugins`.

use std::sync::Arc;
use std::time::Duration;

use http::{HeaderMap, Method, StatusCode, Uri, Version};
use rustc_hash::FxHashMap;
use serde_json::json;
use velda_core::{Action, Body, L7Request, ProtocolFamily, StreamingMode};
use velda_plugin::factory::{LoadedPlugin, build_route_plugin_plan};
use velda_plugin::pre_upstream::{
    GrpcAbortConfig, GrpcDelayConfig, GrpcFaultInjectionPlugin, PreUpstreamPlugin,
    run_pre_upstream_plugins,
};
use velda_sync::PluginConfig;

fn build_grpc_request(path: &str) -> L7Request {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/grpc"),
    );
    headers.insert(http::header::TE, http::HeaderValue::from_static("trailers"));

    L7Request::new(
        Method::POST,
        path.parse::<Uri>().unwrap(),
        Version::HTTP_2,
        headers,
        Body::Empty,
    )
}

#[test]
fn test_suite_grpc_fault_delay_only() {
    let delay = GrpcDelayConfig::new(100, 300).unwrap();
    let plugin: Arc<dyn PreUpstreamPlugin> = Arc::new(GrpcFaultInjectionPlugin::new(
        "grpc_fault",
        Some(delay),
        None,
    ));
    let chain = vec![plugin];

    let mut req = build_grpc_request("/order.v1.OrderService/CreateOrder");
    let mut upstream: Arc<str> = Arc::from("order_upstream");

    let action = run_pre_upstream_plugins(&chain, &mut req, &mut upstream);
    match action {
        Action::Delay { duration, and_then } => {
            assert_eq!(duration, Duration::from_millis(300));
            assert!(matches!(*and_then, Action::Continue));
        }
        _ => panic!("Expected Action::Delay"),
    }
}

#[test]
fn test_suite_grpc_fault_abort_trailers_only() {
    let abort = GrpcAbortConfig::new(100, 14, Some("upstream overloaded".into())).unwrap();
    let plugin: Arc<dyn PreUpstreamPlugin> = Arc::new(GrpcFaultInjectionPlugin::new(
        "grpc_fault",
        None,
        Some(abort),
    ));
    let chain = vec![plugin];

    let mut req = build_grpc_request("/payment.v1.PaymentService/ProcessPayment");
    let mut upstream: Arc<str> = Arc::from("payment_upstream");

    let action = run_pre_upstream_plugins(&chain, &mut req, &mut upstream);
    match action {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::OK);
            assert_eq!(
                resp.headers.get("content-type").unwrap(),
                "application/grpc"
            );
            assert_eq!(resp.headers.get("grpc-status").unwrap(), "14");
            assert_eq!(
                resp.headers.get("grpc-message").unwrap(),
                "upstream overloaded"
            );
            assert!(!resp.has_body());
        }
        _ => panic!("Expected Action::Respond"),
    }
}

#[test]
fn test_suite_grpc_fault_delay_and_abort() {
    let delay = GrpcDelayConfig::new(100, 150).unwrap();
    let abort = GrpcAbortConfig::new(100, 4, Some("deadline exceeded in gateway".into())).unwrap();
    let plugin: Arc<dyn PreUpstreamPlugin> = Arc::new(GrpcFaultInjectionPlugin::new(
        "grpc_fault",
        Some(delay),
        Some(abort),
    ));
    let chain = vec![plugin];

    let mut req = build_grpc_request("/inventory.v1.InventoryService/CheckStock");
    let mut upstream: Arc<str> = Arc::from("inventory_upstream");

    let action = run_pre_upstream_plugins(&chain, &mut req, &mut upstream);
    match action {
        Action::Delay { duration, and_then } => {
            assert_eq!(duration, Duration::from_millis(150));
            match *and_then {
                Action::Respond(resp) => {
                    assert_eq!(resp.status, StatusCode::OK);
                    assert_eq!(resp.headers.get("grpc-status").unwrap(), "4");
                    assert_eq!(
                        resp.headers.get("grpc-message").unwrap(),
                        "deadline exceeded in gateway"
                    );
                }
                _ => panic!("Expected Action::Respond inside Delay"),
            }
        }
        _ => panic!("Expected Action::Delay"),
    }
}

#[test]
fn test_suite_grpc_fault_pass_through_when_disabled() {
    let delay = GrpcDelayConfig::new(0, 100).unwrap();
    let abort = GrpcAbortConfig::new(0, 14, None).unwrap();
    let plugin: Arc<dyn PreUpstreamPlugin> = Arc::new(GrpcFaultInjectionPlugin::new(
        "grpc_fault",
        Some(delay),
        Some(abort),
    ));
    let chain = vec![plugin];

    let mut req = build_grpc_request("/order.v1.OrderService/GetOrder");
    let mut upstream: Arc<str> = Arc::from("order_upstream");

    let action = run_pre_upstream_plugins(&chain, &mut req, &mut upstream);
    assert!(matches!(action, Action::Continue));
}

#[test]
fn test_suite_grpc_fault_plan_compilation() {
    let json_cfg = json!({
        "delay": {
            "enabled": true,
            "fixed_delay_ms": 100,
            "percentage": 10
        },
        "abort": {
            "enabled": true,
            "grpc_status": 14,
            "grpc_message": "cluster saturated",
            "percentage": 5
        }
    });

    let config = PluginConfig {
        id: "grpc_fault_injection".to_string(),
        phase: "pre_upstream".to_string(),
        config: json_cfg,
    };

    let plugin = GrpcFaultInjectionPlugin::from_config(&config).unwrap();
    let mut pool = FxHashMap::default();
    pool.insert(
        "grpc_fault_injection".to_string(),
        LoadedPlugin::PreUpstream(Arc::new(plugin)),
    );

    let plan = build_route_plugin_plan(
        "grpc-route-1",
        &["grpc_fault_injection".to_string()],
        &pool,
        ProtocolFamily::Grpc,
        StreamingMode::disabled(),
    )
    .unwrap();

    assert!(!plan.is_empty());
    assert!(plan.pre_upstream_hooks.is_some());
    let hooks = plan.pre_upstream_hooks.unwrap();
    assert_eq!(hooks.len(), 1);
    assert_eq!(hooks[0].id(), "grpc_fault_injection");
}
