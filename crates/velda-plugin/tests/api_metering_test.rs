//! Comprehensive integration test suite for API Metering Plugin (`pre_upstream: api_metering`).
//!
//! # Verifications
//! - Multi-tier routing & enforcement (`free` -> 402, `pro` -> 429, `enterprise` -> overage passthrough).
//! - Prepaid micro-credit balance tracking and atomic cost deduction ($10^{-6}$).
//! - Authoritative 402 Payment Required short-circuiting with structured JSON diagnostic body.
//! - Pay-as-you-go debt recording (`x-metering-overage: true`).

use std::sync::Arc;

use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, Version};
use rustc_hash::FxHashMap;
use velda_core::{Action, Body, L7Request, RejectReason};
use velda_plugin::pre_upstream::PreUpstreamPlugin;
use velda_plugin::pre_upstream::api_metering::{
    ApiMeteringConfig, ApiMeteringPlugin, ExhaustedAction, TierConfig,
};
use velda_plugin::pre_upstream::consumer_quota::{QuotaAlgorithm, QuotaWindowConfig};

fn build_test_request(headers: HeaderMap) -> L7Request {
    L7Request::new(
        Method::POST,
        Uri::from_static("/v1/models/generate"),
        Version::HTTP_11,
        headers,
        Body::Empty,
    )
}

#[test]
fn test_api_metering_free_tier_rejection_402() {
    let mut tiers = FxHashMap::default();
    tiers.insert(
        "free".to_string(),
        TierConfig {
            name: "free".to_string(),
            windows: vec![QuotaWindowConfig {
                name: "monthly_allowance".to_string(),
                window_seconds: 30 * 86_400,
                limit: 2,
                algorithm: QuotaAlgorithm::FixedWindow,
            }],
            cost_per_req_micro_credits: 0,
            exhausted_action: ExhaustedAction::Reject402,
        },
    );

    let config = ApiMeteringConfig {
        account_headers: vec![HeaderName::from_static("x-tenant-id")],
        tier_header: HeaderName::from_static("x-plan-tier"),
        default_tier: "free".to_string(),
        initial_balance_micro_credits: 0,
        tiers,
        distributed: None,
    };
    let plugin = Arc::new(ApiMeteringPlugin::new("metering-test-1", config));
    let mut target = velda_core::UpstreamId::from("upstream-ai");

    // Request 1: Allowed, remaining = 1
    let mut h1 = HeaderMap::new();
    h1.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("tenant_startup_inc"),
    );
    let mut r1 = build_test_request(h1);
    let act1 = plugin.on_upstream_request(&mut r1, &mut target);
    assert!(matches!(act1, Action::Continue));
    assert_eq!(r1.headers.get_u64("x-metering-remaining"), Some(1));
    r1.headers.render();
    assert_eq!(r1.headers.get("x-metering-tier").unwrap(), "free");
    assert_eq!(r1.headers.get("x-metering-remaining").unwrap(), "1");

    // Request 2: Allowed, remaining = 0
    let mut h2 = HeaderMap::new();
    h2.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("tenant_startup_inc"),
    );
    let mut r2 = build_test_request(h2);
    let act2 = plugin.on_upstream_request(&mut r2, &mut target);
    assert!(matches!(act2, Action::Continue));
    assert_eq!(r2.headers.get_u64("x-metering-remaining"), Some(0));
    r2.headers.render();
    assert_eq!(r2.headers.get("x-metering-remaining").unwrap(), "0");

    // Request 3: Quota exhausted -> Action::Respond(402 Payment Required)
    let mut h3 = HeaderMap::new();
    h3.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("tenant_startup_inc"),
    );
    let mut r3 = build_test_request(h3);
    let act3 = plugin.on_upstream_request(&mut r3, &mut target);
    match act3 {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::PAYMENT_REQUIRED);
            assert_eq!(
                resp.headers.get(CONTENT_TYPE).unwrap(),
                "application/json; charset=utf-8"
            );
            assert_eq!(resp.headers.get("x-metering-tier").unwrap(), "free");
            assert_eq!(
                resp.headers.get("x-metering-exhausted-window").unwrap(),
                "monthly_allowance"
            );

            // Verify informative JSON body
            let body_bytes = match resp.body {
                Body::Bytes(b) => b,
                _ => panic!("Expected Body::Bytes"),
            };
            let body_str = std::str::from_utf8(&body_bytes).unwrap();
            assert!(body_str.contains(r#""error":"payment_required""#));
            assert!(body_str.contains(r#""tier":"free""#));
        }
        other => panic!("Expected Action::Respond(402), got {other:?}"),
    }
}

#[test]
fn test_api_metering_pro_tier_rejection_429() {
    let mut tiers = FxHashMap::default();
    tiers.insert(
        "pro".to_string(),
        TierConfig {
            name: "pro".to_string(),
            windows: vec![QuotaWindowConfig {
                name: "burst_limit".to_string(),
                window_seconds: 60,
                limit: 1,
                algorithm: QuotaAlgorithm::SlidingWindow,
            }],
            cost_per_req_micro_credits: 0,
            exhausted_action: ExhaustedAction::Reject429,
        },
    );

    let config = ApiMeteringConfig {
        account_headers: vec![HeaderName::from_static("x-tenant-id")],
        tier_header: HeaderName::from_static("x-plan-tier"),
        default_tier: "free".to_string(),
        initial_balance_micro_credits: 0,
        tiers,
        distributed: None,
    };
    let plugin = Arc::new(ApiMeteringPlugin::new("metering-test-2", config));
    let mut target = velda_core::UpstreamId::from("upstream-ai");

    // Request 1: Allowed
    let mut h1 = HeaderMap::new();
    h1.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("pro_corp"),
    );
    h1.insert(
        HeaderName::from_static("x-plan-tier"),
        HeaderValue::from_static("pro"),
    );
    let mut r1 = build_test_request(h1);
    assert!(matches!(
        plugin.on_upstream_request(&mut r1, &mut target),
        Action::Continue
    ));

    // Request 2: Rejected with 429 and Bit-1 bypass
    let mut h2 = HeaderMap::new();
    h2.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("pro_corp"),
    );
    h2.insert(
        HeaderName::from_static("x-plan-tier"),
        HeaderValue::from_static("pro"),
    );
    let mut r2 = build_test_request(h2);
    let act2 = plugin.on_upstream_request(&mut r2, &mut target);
    match act2 {
        Action::Reject(reason) => {
            assert_eq!(reason, RejectReason::TooManyRequests);
            r2.headers.render();
            assert!(r2.headers.bypass_headers().count() >= 2);
            assert_eq!(r2.headers.get("x-metering-tier").unwrap(), "pro");
            assert_eq!(r2.headers.get("x-metering-exhausted").unwrap(), "true");
        }
        other => panic!("Expected Action::Reject(TooManyRequests), got {other:?}"),
    }
}

#[test]
fn test_api_metering_enterprise_overage_passthrough() {
    let mut tiers = FxHashMap::default();
    tiers.insert(
        "enterprise".to_string(),
        TierConfig {
            name: "enterprise".to_string(),
            windows: vec![QuotaWindowConfig {
                name: "base_cap".to_string(),
                window_seconds: 60,
                limit: 1,
                algorithm: QuotaAlgorithm::FixedWindow,
            }],
            cost_per_req_micro_credits: 100_000, // 0.1 credit per req
            exhausted_action: ExhaustedAction::Passthrough,
        },
    );

    let config = ApiMeteringConfig {
        account_headers: vec![HeaderName::from_static("x-tenant-id")],
        tier_header: HeaderName::from_static("x-plan-tier"),
        default_tier: "enterprise".to_string(),
        initial_balance_micro_credits: 100_000, // 1 request credit funded
        tiers,
        distributed: None,
    };
    let plugin = Arc::new(ApiMeteringPlugin::new("metering-test-3", config));
    let mut target = velda_core::UpstreamId::from("upstream-ai");

    // Request 1: Consumes the 100_000 credit -> balance = 0
    let mut h1 = HeaderMap::new();
    h1.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("mega_corp"),
    );
    let mut r1 = build_test_request(h1);
    let act1 = plugin.on_upstream_request(&mut r1, &mut target);
    assert!(matches!(act1, Action::Continue));
    assert_eq!(r1.headers.get_u64("x-metering-balance"), Some(0));
    r1.headers.render();
    assert_eq!(r1.headers.get("x-metering-balance").unwrap(), "0");

    // Request 2: Balance 0 < 100_000 cost, but Passthrough policy kicks in!
    // Balance drops to -100_000 (debt), request passes through with x-metering-overage
    let mut h2 = HeaderMap::new();
    h2.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("mega_corp"),
    );
    let mut r2 = build_test_request(h2);
    let act2 = plugin.on_upstream_request(&mut r2, &mut target);
    assert!(matches!(act2, Action::Continue));
    assert_eq!(r2.headers.get("x-metering-overage").unwrap(), "true");
    assert_eq!(r2.headers.get("x-metering-balance").unwrap(), "-100000");
}

#[test]
fn test_api_metering_prepaid_micro_credits_deduction() {
    let mut tiers = FxHashMap::default();
    tiers.insert(
        "payg".to_string(),
        TierConfig {
            name: "payg".to_string(),
            windows: vec![], // No request count windows, bounded solely by credit wallet
            cost_per_req_micro_credits: 500_000, // 0.5 credits per inference call
            exhausted_action: ExhaustedAction::Reject402,
        },
    );

    let config = ApiMeteringConfig {
        account_headers: vec![HeaderName::from_static("x-tenant-id")],
        tier_header: HeaderName::from_static("x-plan-tier"),
        default_tier: "payg".to_string(),
        initial_balance_micro_credits: 1_000_000, // 1.0 credit funded
        tiers,
        distributed: None,
    };
    let plugin = Arc::new(ApiMeteringPlugin::new("metering-credits-test", config));
    let mut target = velda_core::UpstreamId::from("upstream-ai");

    // Request 1: Consumes 500_000, remaining balance = 500_000
    let mut h1 = HeaderMap::new();
    h1.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("ai_startup"),
    );
    let mut r1 = build_test_request(h1);
    let act1 = plugin.on_upstream_request(&mut r1, &mut target);
    assert!(matches!(act1, Action::Continue));
    assert_eq!(r1.headers.get_u64("x-metering-balance"), Some(500_000));
    r1.headers.render();
    assert_eq!(r1.headers.get("x-metering-balance").unwrap(), "500000");

    // Request 2: Consumes 500_000, remaining balance = 0
    let mut h2 = HeaderMap::new();
    h2.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("ai_startup"),
    );
    let mut r2 = build_test_request(h2);
    let act2 = plugin.on_upstream_request(&mut r2, &mut target);
    assert!(matches!(act2, Action::Continue));
    assert_eq!(r2.headers.get_u64("x-metering-balance"), Some(0));
    r2.headers.render();
    assert_eq!(r2.headers.get("x-metering-balance").unwrap(), "0");

    // Request 3: Insufficient credits (0 < 500_000) -> 402 Payment Required
    let mut h3 = HeaderMap::new();
    h3.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("ai_startup"),
    );
    let mut r3 = build_test_request(h3);
    let act3 = plugin.on_upstream_request(&mut r3, &mut target);
    match act3 {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::PAYMENT_REQUIRED);
            assert_eq!(
                resp.headers.get("x-metering-exhausted-window").unwrap(),
                "credit_balance"
            );
        }
        other => panic!("Expected Action::Respond(402), got {other:?}"),
    }
}
