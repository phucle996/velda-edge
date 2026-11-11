//! Comprehensive integration test suite for Consumer Quota Plugin (`pre_upstream: consumer_quota`).
//!
//! # Verifications
//! - Rolling sliding window enforcement (e.g. AI 5-hour quota model).
//! - Multi-member fair-use isolation within shared tenant spaces.
//! - RFC 6585 Bit-1 bypass header injection on rejection (`Retry-After`, `x-quota-remaining: 0`).
//! - Automatic fallback to `x-forwarded-for` when explicit consumer headers are absent.

use std::sync::Arc;

use http::header::RETRY_AFTER;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Uri, Version};
use velda_core::{Action, Body, L7Request, RejectReason};
use velda_plugin::pre_upstream::PreUpstreamPlugin;
use velda_plugin::pre_upstream::consumer_quota::{
    ConsumerQuotaConfig, ConsumerQuotaPlugin, QuotaAlgorithm, QuotaWindowConfig,
};

fn build_test_request(headers: HeaderMap) -> L7Request {
    L7Request::new(
        Method::POST,
        Uri::from_static("/v1/chat/completions"),
        Version::HTTP_11,
        headers,
        Body::Empty,
    )
}

#[test]
fn test_consumer_quota_sliding_window_burst_and_exhaustion() {
    let config = ConsumerQuotaConfig {
        key_headers: vec![HeaderName::from_static("x-user-id")],
        windows: vec![QuotaWindowConfig {
            name: "burst_limit".to_string(),
            window_seconds: 60,
            limit: 3,
            algorithm: QuotaAlgorithm::SlidingWindow,
        }],
        distributed: None,
    };
    let plugin = Arc::new(ConsumerQuotaPlugin::new("quota-test-1", config));
    let mut target = velda_core::UpstreamId::from("upstream-llm");

    // Request 1: Allowed, remaining = 2
    let mut h1 = HeaderMap::new();
    h1.insert(
        HeaderName::from_static("x-user-id"),
        HeaderValue::from_static("user_alice"),
    );
    let mut r1 = build_test_request(h1);
    let act1 = plugin.on_upstream_request(&mut r1, &mut target);
    assert!(matches!(act1, Action::Continue));
    assert_eq!(r1.headers.get_u64("x-quota-remaining"), Some(2));
    r1.headers.render();
    assert_eq!(r1.headers.get("x-quota-remaining").unwrap(), "2");
    assert_eq!(r1.headers.get("x-quota-window").unwrap(), "burst_limit");

    // Request 2: Allowed, remaining = 1
    let mut h2 = HeaderMap::new();
    h2.insert(
        HeaderName::from_static("x-user-id"),
        HeaderValue::from_static("user_alice"),
    );
    let mut r2 = build_test_request(h2);
    let act2 = plugin.on_upstream_request(&mut r2, &mut target);
    assert!(matches!(act2, Action::Continue));
    assert_eq!(r2.headers.get_u64("x-quota-remaining"), Some(1));
    r2.headers.render();
    assert_eq!(r2.headers.get("x-quota-remaining").unwrap(), "1");

    // Request 3: Allowed, remaining = 0
    let mut h3 = HeaderMap::new();
    h3.insert(
        HeaderName::from_static("x-user-id"),
        HeaderValue::from_static("user_alice"),
    );
    let mut r3 = build_test_request(h3);
    let act3 = plugin.on_upstream_request(&mut r3, &mut target);
    assert!(matches!(act3, Action::Continue));
    assert_eq!(r3.headers.get_u64("x-quota-remaining"), Some(0));
    r3.headers.render();
    assert_eq!(r3.headers.get("x-quota-remaining").unwrap(), "0");

    // Request 4: Exhausted -> Action::Reject(TooManyRequests) with Bit-1 bypass headers
    let mut h4 = HeaderMap::new();
    h4.insert(
        HeaderName::from_static("x-user-id"),
        HeaderValue::from_static("user_alice"),
    );
    let mut r4 = build_test_request(h4);
    let act4 = plugin.on_upstream_request(&mut r4, &mut target);
    match act4 {
        Action::Reject(reason) => {
            assert_eq!(reason, RejectReason::TooManyRequests);
            assert_eq!(r4.headers.get_u64("x-quota-remaining"), Some(0));
            assert!(r4.headers.contains_key(RETRY_AFTER));
            r4.headers.render();
            assert!(r4.headers.bypass_headers().count() >= 2);
            assert_eq!(r4.headers.get("x-quota-remaining").unwrap(), "0");
            assert_eq!(r4.headers.get("x-quota-window").unwrap(), "burst_limit");
        }
        other => panic!("Expected Action::Reject(TooManyRequests), got {other:?}"),
    }
}

#[test]
fn test_consumer_quota_multi_tenant_member_isolation() {
    let config = ConsumerQuotaConfig {
        key_headers: vec![HeaderName::from_static("x-user-id")],
        windows: vec![QuotaWindowConfig {
            name: "member_window".to_string(),
            window_seconds: 60,
            limit: 2,
            algorithm: QuotaAlgorithm::SlidingWindow,
        }],
        distributed: None,
    };
    let plugin = Arc::new(ConsumerQuotaPlugin::new("quota-test-2", config));
    let mut target = velda_core::UpstreamId::from("upstream-llm");

    // Alice consumes 2 requests (exhausts her quota)
    for _ in 0..2 {
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static("x-user-id"),
            HeaderValue::from_static("alice"),
        );
        let mut req = build_test_request(h);
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));
    }

    // Alice 3rd request is blocked
    let mut h_alice = HeaderMap::new();
    h_alice.insert(
        HeaderName::from_static("x-user-id"),
        HeaderValue::from_static("alice"),
    );
    let mut req_alice = build_test_request(h_alice);
    assert!(matches!(
        plugin.on_upstream_request(&mut req_alice, &mut target),
        Action::Reject(RejectReason::TooManyRequests)
    ));

    // Bob still has full quota available
    let mut h_bob = HeaderMap::new();
    h_bob.insert(
        HeaderName::from_static("x-user-id"),
        HeaderValue::from_static("bob"),
    );
    let mut req_bob = build_test_request(h_bob);
    let act_bob = plugin.on_upstream_request(&mut req_bob, &mut target);
    assert!(matches!(act_bob, Action::Continue));
    assert_eq!(req_bob.headers.get_u64("x-quota-remaining"), Some(1));
    req_bob.headers.render();
    assert_eq!(req_bob.headers.get("x-quota-remaining").unwrap(), "1");
}

#[test]
fn test_consumer_quota_fallback_to_client_ip() {
    let config = ConsumerQuotaConfig {
        key_headers: vec![HeaderName::from_static("x-user-id")],
        windows: vec![QuotaWindowConfig {
            name: "ip_window".to_string(),
            window_seconds: 60,
            limit: 1,
            algorithm: QuotaAlgorithm::FixedWindow,
        }],
        distributed: None,
    };
    let plugin = Arc::new(ConsumerQuotaPlugin::new("quota-test-3", config));
    let mut target = velda_core::UpstreamId::from("upstream-llm");

    // No x-user-id header, should fallback to x-forwarded-for
    let mut h1 = HeaderMap::new();
    h1.insert(
        HeaderName::from_static("x-forwarded-for"),
        HeaderValue::from_static("198.51.100.42"),
    );
    let mut r1 = build_test_request(h1);
    assert!(matches!(
        plugin.on_upstream_request(&mut r1, &mut target),
        Action::Continue
    ));

    // Second request from same IP is blocked
    let mut h2 = HeaderMap::new();
    h2.insert(
        HeaderName::from_static("x-forwarded-for"),
        HeaderValue::from_static("198.51.100.42"),
    );
    let mut r2 = build_test_request(h2);
    assert!(matches!(
        plugin.on_upstream_request(&mut r2, &mut target),
        Action::Reject(RejectReason::TooManyRequests)
    ));

    // Different IP is permitted
    let mut h3 = HeaderMap::new();
    h3.insert(
        HeaderName::from_static("x-forwarded-for"),
        HeaderValue::from_static("203.0.113.88"),
    );
    let mut r3 = build_test_request(h3);
    assert!(matches!(
        plugin.on_upstream_request(&mut r3, &mut target),
        Action::Continue
    ));
}
