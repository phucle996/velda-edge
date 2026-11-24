//! Comprehensive integration test suite for API Metering Plugin (`pre_upstream: api_metering`).
//!
//! # Verifications
//! - Multi-tier routing & enforcement (`free` -> 402, `pro` -> 429, `enterprise` -> overage passthrough).
//! - Prepaid micro-credit balance tracking and atomic cost deduction ($10^{-6}$).
//! - Authoritative 402 Payment Required short-circuiting with structured JSON diagnostic body.
//! - Pay-as-you-go debt recording (`x-metering-overage: true`).

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use http::header::CONTENT_TYPE;
use http::{HeaderName, HeaderValue, Method, StatusCode, Uri, Version};
use rustc_hash::FxHashMap;
use serde_json::json;
use velda_core::{Action, Body, HeaderWorkspace, L7Request, RejectReason};
use velda_etp::registry::build_registry;
use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use velda_plugin::PluginCapability;
use velda_plugin::capability::AccessLevel;
use velda_plugin::config::PluginConfig;
use velda_plugin::factory::{LoadedPlugin, wire_plugin_etps};
use velda_plugin::pre_upstream::PreUpstreamPlugin;
use velda_plugin::pre_upstream::api_metering::{
    ApiMeteringConfig, ApiMeteringPlugin, ExhaustedAction, MAX_ACCOUNTS_PER_SHARD,
    ShardedMeteringState, TierConfig,
};
use velda_plugin::pre_upstream::consumer_quota::{QuotaAlgorithm, QuotaWindowConfig};

fn build_test_request(headers: HeaderWorkspace) -> L7Request {
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
            grace_enabled: false,
            grace_requests: None,
        },
    );

    let config = ApiMeteringConfig {
        account_headers: vec![HeaderName::from_static("x-tenant-id")],
        tier_header: HeaderName::from_static("x-plan-tier"),
        default_tier: "free".to_string(),
        initial_balance_micro_credits: 0,
        tiers,
        etp_id: None,
        distributed: None,
    };
    let plugin = Arc::new(ApiMeteringPlugin::new("metering-test-1", config));
    let mut target = 1;

    // Request 1: Allowed, remaining = 1
    let mut h1 = HeaderWorkspace::empty(4);
    h1.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("tenant_startup_inc"),
    );
    let mut r1 = build_test_request(h1);
    let act1 = plugin.on_upstream_request(&mut r1, &mut target);
    assert!(matches!(act1, Action::Continue));
    assert_eq!(r1.headers.get_u64("x-metering-remaining"), Some(1));
    assert_eq!(r1.headers.get("x-metering-tier").unwrap(), "free");
    assert_eq!(r1.headers.get("x-metering-remaining").unwrap(), "1");

    // Request 2: Allowed, remaining = 0
    let mut h2 = HeaderWorkspace::empty(4);
    h2.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("tenant_startup_inc"),
    );
    let mut r2 = build_test_request(h2);
    let act2 = plugin.on_upstream_request(&mut r2, &mut target);
    assert!(matches!(act2, Action::Continue));
    assert_eq!(r2.headers.get_u64("x-metering-remaining"), Some(0));
    assert_eq!(r2.headers.get("x-metering-remaining").unwrap(), "0");

    // Request 3: Quota exhausted -> Action::Respond(402 Payment Required)
    let mut h3 = HeaderWorkspace::empty(4);
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
            grace_enabled: false,
            grace_requests: None,
        },
    );

    let config = ApiMeteringConfig {
        account_headers: vec![HeaderName::from_static("x-tenant-id")],
        tier_header: HeaderName::from_static("x-plan-tier"),
        default_tier: "free".to_string(),
        initial_balance_micro_credits: 0,
        tiers,
        etp_id: None,
        distributed: None,
    };
    let plugin = Arc::new(ApiMeteringPlugin::new("metering-test-2", config));
    let mut target = 1;

    // Request 1: Allowed
    let mut h1 = HeaderWorkspace::empty(4);
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
    let mut h2 = HeaderWorkspace::empty(4);
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
            grace_enabled: false,
            grace_requests: None,
        },
    );

    let config = ApiMeteringConfig {
        account_headers: vec![HeaderName::from_static("x-tenant-id")],
        tier_header: HeaderName::from_static("x-plan-tier"),
        default_tier: "enterprise".to_string(),
        initial_balance_micro_credits: 100_000, // 1 request credit funded
        tiers,
        etp_id: None,
        distributed: None,
    };
    let plugin = Arc::new(ApiMeteringPlugin::new("metering-test-3", config));
    let mut target = 1;

    // Request 1: Consumes the 100_000 credit -> balance = 0
    let mut h1 = HeaderWorkspace::empty(4);
    h1.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("mega_corp"),
    );
    let mut r1 = build_test_request(h1);
    let act1 = plugin.on_upstream_request(&mut r1, &mut target);
    assert!(matches!(act1, Action::Continue));
    assert_eq!(r1.headers.get_u64("x-metering-balance"), Some(0));
    assert_eq!(r1.headers.get("x-metering-balance").unwrap(), "0");

    // Request 2: Balance 0 < 100_000 cost, but Passthrough policy kicks in!
    // Balance drops to -100_000 (debt), request passes through with x-metering-overage
    let mut h2 = HeaderWorkspace::empty(4);
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
            grace_enabled: false,
            grace_requests: None,
        },
    );

    let config = ApiMeteringConfig {
        account_headers: vec![HeaderName::from_static("x-tenant-id")],
        tier_header: HeaderName::from_static("x-plan-tier"),
        default_tier: "payg".to_string(),
        initial_balance_micro_credits: 1_000_000, // 1.0 credit funded
        tiers,
        etp_id: None,
        distributed: None,
    };
    let plugin = Arc::new(ApiMeteringPlugin::new("metering-credits-test", config));
    let mut target = 1;

    // Request 1: Consumes 500_000, remaining balance = 500_000
    let mut h1 = HeaderWorkspace::empty(4);
    h1.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("ai_startup"),
    );
    let mut r1 = build_test_request(h1);
    let act1 = plugin.on_upstream_request(&mut r1, &mut target);
    assert!(matches!(act1, Action::Continue));
    assert_eq!(r1.headers.get_u64("x-metering-balance"), Some(500_000));
    assert_eq!(r1.headers.get("x-metering-balance").unwrap(), "500000");

    // Request 2: Consumes 500_000, remaining balance = 0
    let mut h2 = HeaderWorkspace::empty(4);
    h2.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("ai_startup"),
    );
    let mut r2 = build_test_request(h2);
    let act2 = plugin.on_upstream_request(&mut r2, &mut target);
    assert!(matches!(act2, Action::Continue));
    assert_eq!(r2.headers.get_u64("x-metering-balance"), Some(0));
    assert_eq!(r2.headers.get("x-metering-balance").unwrap(), "0");

    // Request 3: Insufficient credits (0 < 500_000) -> 402 Payment Required
    let mut h3 = HeaderWorkspace::empty(4);
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

#[test]
fn test_api_metering_payg_empty_windows_omits_bogus_window_headers() {
    let mut tiers = FxHashMap::default();
    tiers.insert(
        "payg".to_string(),
        TierConfig {
            name: "payg".to_string(),
            windows: vec![],
            cost_per_req_micro_credits: 1_000,
            exhausted_action: ExhaustedAction::Reject402,
            grace_enabled: false,
            grace_requests: None,
        },
    );
    let plugin = ApiMeteringPlugin::new(
        "metering-payg",
        ApiMeteringConfig {
            default_tier: "payg".to_string(),
            initial_balance_micro_credits: 10_000,
            tiers,
            ..Default::default()
        },
    );
    assert_eq!(
        plugin.capability(),
        PluginCapability::header_only(AccessLevel::Write)
    );

    let mut h = HeaderWorkspace::empty(4);
    h.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("payg_tenant"),
    );
    let mut req = build_test_request(h);
    let mut target = 0;
    assert!(matches!(
        plugin.on_upstream_request(&mut req, &mut target),
        Action::Continue
    ));
    assert_eq!(req.headers.get("x-metering-tier").unwrap(), "payg");
    assert_eq!(req.headers.get_u64("x-metering-balance"), Some(9_000));
    assert!(req.headers.get("x-metering-remaining").is_none());
    assert!(req.headers.get("x-metering-window").is_none());
}

#[test]
fn test_api_metering_tier_header_resolution_and_dynamic_tier_switch() {
    let mut tiers = FxHashMap::default();
    tiers.insert(
        "free".to_string(),
        TierConfig {
            name: "free".to_string(),
            windows: vec![QuotaWindowConfig {
                name: "free_cap".to_string(),
                window_seconds: 60,
                limit: 5,
                algorithm: QuotaAlgorithm::FixedWindow,
            }],
            cost_per_req_micro_credits: 0,
            exhausted_action: ExhaustedAction::Reject402,
            grace_enabled: false,
            grace_requests: None,
        },
    );
    tiers.insert(
        "pro".to_string(),
        TierConfig {
            name: "pro".to_string(),
            windows: vec![QuotaWindowConfig {
                name: "pro_cap".to_string(),
                window_seconds: 60,
                limit: 100,
                algorithm: QuotaAlgorithm::SlidingWindow,
            }],
            cost_per_req_micro_credits: 0,
            exhausted_action: ExhaustedAction::Reject429,
            grace_enabled: false,
            grace_requests: None,
        },
    );

    let plugin = ApiMeteringPlugin::new(
        "metering-switch",
        ApiMeteringConfig {
            default_tier: "free".to_string(),
            tiers,
            ..Default::default()
        },
    );
    let mut target = 0;

    // 1. No tier header -> defaults to "free", remaining = 4
    let mut h1 = HeaderWorkspace::empty(4);
    h1.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("switch_org"),
    );
    let mut r1 = build_test_request(h1);
    assert!(matches!(
        plugin.on_upstream_request(&mut r1, &mut target),
        Action::Continue
    ));
    assert_eq!(r1.headers.get("x-metering-tier").unwrap(), "free");
    assert_eq!(r1.headers.get_u64("x-metering-remaining"), Some(4));

    // 2. Switch dynamically to "  PRO  " (same window count = 1!) -> state re-initialized to pro_cap limit 100
    let mut h2 = HeaderWorkspace::empty(4);
    h2.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("switch_org"),
    );
    h2.insert(
        HeaderName::from_static("x-plan-tier"),
        HeaderValue::from_static("  PRO  "),
    );
    let mut r2 = build_test_request(h2);
    assert!(matches!(
        plugin.on_upstream_request(&mut r2, &mut target),
        Action::Continue
    ));
    assert_eq!(r2.headers.get("x-metering-tier").unwrap(), "pro");
    assert_eq!(r2.headers.get("x-metering-window").unwrap(), "pro_cap");
    assert_eq!(r2.headers.get_u64("x-metering-remaining"), Some(99));

    // 3. Unknown tier falls back to default_tier ("free")
    let mut h3 = HeaderWorkspace::empty(4);
    h3.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("other_org"),
    );
    h3.insert(
        HeaderName::from_static("x-plan-tier"),
        HeaderValue::from_static("nonexistent"),
    );
    let mut r3 = build_test_request(h3);
    assert!(matches!(
        plugin.on_upstream_request(&mut r3, &mut target),
        Action::Continue
    ));
    assert_eq!(r3.headers.get("x-metering-tier").unwrap(), "free");
}

#[test]
fn test_api_metering_dry_run_isolation_multi_window() {
    let mut tiers = FxHashMap::default();
    tiers.insert(
        "multi".to_string(),
        TierConfig {
            name: "multi".to_string(),
            windows: vec![
                QuotaWindowConfig {
                    name: "daily".to_string(),
                    window_seconds: 86_400,
                    limit: 5,
                    algorithm: QuotaAlgorithm::FixedWindow,
                },
                QuotaWindowConfig {
                    name: "burst".to_string(),
                    window_seconds: 60,
                    limit: 1,
                    algorithm: QuotaAlgorithm::FixedWindow,
                },
            ],
            cost_per_req_micro_credits: 10_000,
            exhausted_action: ExhaustedAction::Reject402,
            grace_enabled: false,
            grace_requests: None,
        },
    );
    let plugin = ApiMeteringPlugin::new(
        "metering-multi",
        ApiMeteringConfig {
            default_tier: "multi".to_string(),
            initial_balance_micro_credits: 50_000,
            tiers,
            ..Default::default()
        },
    );
    let mut target = 0;

    // Request 1 passes both windows: daily remaining = 4, burst remaining = 0 -> min_remaining = 0 ("burst")
    let mut h1 = HeaderWorkspace::empty(4);
    h1.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("dry_tenant"),
    );
    let mut r1 = build_test_request(h1);
    assert!(matches!(
        plugin.on_upstream_request(&mut r1, &mut target),
        Action::Continue
    ));
    assert_eq!(r1.headers.get_u64("x-metering-remaining"), Some(0));
    assert_eq!(r1.headers.get("x-metering-window").unwrap(), "burst");
    assert_eq!(r1.headers.get_u64("x-metering-balance"), Some(40_000));

    // Request 2 fails burst (window 2). Must NOT deduct wallet credits or advance daily counter.
    let mut h2 = HeaderWorkspace::empty(4);
    h2.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("dry_tenant"),
    );
    let mut r2 = build_test_request(h2);
    assert!(matches!(
        plugin.on_upstream_request(&mut r2, &mut target),
        Action::Respond(resp) if resp.status == StatusCode::PAYMENT_REQUIRED
    ));

    // Switching tier only resets windows, wallet balance must still be 40_000 (not 30_000!).
    let deltas = plugin.state.collect_deltas();
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].1, 10_000);
}

#[test]
fn test_api_metering_overage_passthrough_still_advances_windows() {
    let mut tiers = FxHashMap::default();
    tiers.insert(
        "ent".to_string(),
        TierConfig {
            name: "ent".to_string(),
            windows: vec![QuotaWindowConfig {
                name: "w1".to_string(),
                window_seconds: 60,
                limit: 10,
                algorithm: QuotaAlgorithm::FixedWindow,
            }],
            cost_per_req_micro_credits: 100,
            exhausted_action: ExhaustedAction::Passthrough,
            grace_enabled: false,
            grace_requests: None,
        },
    );
    let plugin = ApiMeteringPlugin::new(
        "metering-overage-win",
        ApiMeteringConfig {
            default_tier: "ent".to_string(),
            initial_balance_micro_credits: 0, // Immediate overage
            tiers,
            ..Default::default()
        },
    );
    let mut target = 0;

    for _ in 0..3 {
        let mut h = HeaderWorkspace::empty(4);
        h.insert(
            HeaderName::from_static("x-tenant-id"),
            HeaderValue::from_static("debt_org"),
        );
        let mut req = build_test_request(h);
        assert!(matches!(
            plugin.on_upstream_request(&mut req, &mut target),
            Action::Continue
        ));
        assert_eq!(req.headers.get("x-metering-overage").unwrap(), "true");
    }

    // Fund wallet via Redis reconcile -> window counter must have tracked the 3 requests (10 - 3 - 1 = 6)
    let deltas = plugin.state.collect_deltas();
    assert_eq!(deltas[0].1, 300);
    plugin.state.reconcile_balances(&[(deltas[0].0, 10_000)]);

    let mut h4 = HeaderWorkspace::empty(4);
    h4.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("debt_org"),
    );
    let mut r4 = build_test_request(h4);
    assert!(matches!(
        plugin.on_upstream_request(&mut r4, &mut target),
        Action::Continue
    ));
    assert_eq!(r4.headers.get_u64("x-metering-remaining"), Some(6));
}

#[test]
fn test_api_metering_in_flight_delta_revert_and_concurrent_reconcile() {
    let state = ShardedMeteringState::new();
    let tier = TierConfig {
        name: "paid".to_string(),
        windows: vec![],
        cost_per_req_micro_credits: 1_000,
        exhausted_action: ExhaustedAction::Reject402,
        grace_enabled: false,
        grace_requests: None,
    };
    let now = std::time::Instant::now();

    // Spend 1_000 (balance = 9_000, delta = 1_000)
    state.evaluate(42, &tier, 10_000, 1000, now);
    let d1 = state.collect_deltas();
    assert_eq!(d1, vec![(42, 1_000)]);

    // Simulate Redis error -> revert in-flight delta, then next collect recovers it
    state.revert_in_flight_deltas(&d1);
    let d2 = state.collect_deltas();
    assert_eq!(d2, vec![(42, 1_000)]);

    // Simulate concurrent traffic arriving BEFORE Redis RPC finishes (+2_000 local consumption)
    state.evaluate(42, &tier, 10_000, 1001, now);
    state.evaluate(42, &tier, 10_000, 1002, now);

    // Redis ACK arrives with balance after d2 (9_000); reconcile must preserve the 2_000 local delta -> 7_000
    state.reconcile_balances(&[(42, 9_000)]);
    let d3 = state.collect_deltas();
    assert_eq!(d3, vec![(42, 2_000)]);
}

#[test]
fn test_api_metering_from_config_json_escape_and_window_limit_guard() {
    // Reject > MAX_WINDOWS_PER_TIER (4) cleanly
    let bad_cfg = PluginConfig {
        id: "api_metering".to_string(),
        config: json!({
            "tiers": {
                "bad": {
                    "windows": [
                        { "name": "w1", "window_seconds": 10, "limit": 1 },
                        { "name": "w2", "window_seconds": 20, "limit": 1 },
                        { "name": "w3", "window_seconds": 30, "limit": 1 },
                        { "name": "w4", "window_seconds": 40, "limit": 1 },
                        { "name": "w5", "window_seconds": 50, "limit": 1 }
                    ]
                }
            }
        }),
    };
    assert!(ApiMeteringPlugin::from_config(&bad_cfg).is_err());

    // Valid config with quotes in tier name verifies 402 JSON remains valid JSON
    let ok_cfg = PluginConfig {
        id: "api_metering".to_string(),
        config: json!({
            "default_tier": "q\"uote",
            "tiers": {
                "q\"uote": {
                    "exhausted_action": "402",
                    "windows": [{ "name": "win\\1", "window_seconds": 60, "limit": 1 }]
                }
            }
        }),
    };
    let plugin = ApiMeteringPlugin::from_config(&ok_cfg).unwrap();
    let mut target = 0;

    let mut h1 = HeaderWorkspace::empty(4);
    h1.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("t1"),
    );
    assert!(matches!(
        plugin.on_upstream_request(&mut build_test_request(h1), &mut target),
        Action::Continue
    ));

    let mut h2 = HeaderWorkspace::empty(4);
    h2.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("t1"),
    );
    match plugin.on_upstream_request(&mut build_test_request(h2), &mut target) {
        Action::Respond(resp) => {
            let Body::Bytes(b) = resp.body else {
                panic!("Expected Body::Bytes");
            };
            let parsed: serde_json::Value = serde_json::from_slice(&b).unwrap();
            assert_eq!(parsed["error"], "payment_required");
            assert_eq!(parsed["tier"], "q\"uote");
            assert_eq!(parsed["window"], "win\\1");
        }
        other => panic!("Expected 402 Respond, got {other:?}"),
    }
}

#[test]
fn test_api_metering_shard_growth_and_clock_eviction_bounded() {
    let state = ShardedMeteringState::new();
    let tier = TierConfig {
        name: "free".to_string(),
        windows: vec![QuotaWindowConfig {
            name: "cap".to_string(),
            window_seconds: 60,
            limit: 100,
            algorithm: QuotaAlgorithm::FixedWindow,
        }],
        cost_per_req_micro_credits: 0,
        exhausted_action: ExhaustedAction::Reject402,
        grace_enabled: false,
        grace_requests: None,
    };
    let now = std::time::Instant::now();

    // Insert 40,000 keys targeting strictly shard 0 ((i << 6) | 0) > MAX_ACCOUNTS_PER_SHARD (32_768)
    // to verify seamless arena expansion and bounded CLOCK eviction with zero hangs or panics.
    for i in 1u64..=40_000 {
        let key = i << 6;
        let dec = state.evaluate(key, &tier, 0, 1000 + (i / 100), now);
        assert!(matches!(
            dec,
            velda_plugin::pre_upstream::api_metering::MeteringDecision::Allowed { .. }
        ));
    }
    // Key 1 was evicted earlier due to bounded capacity; evaluating it again succeeds cleanly.
    let re_dec = state.evaluate(1 << 6, &tier, 0, 2000, now);
    assert!(matches!(
        re_dec,
        velda_plugin::pre_upstream::api_metering::MeteringDecision::Allowed {
            min_remaining: 99,
            ..
        }
    ));
    const {
        assert!(MAX_ACCOUNTS_PER_SHARD == 32_768);
    }
}

#[test]
fn test_api_metering_missing_account_header_rejects_400() {
    let plugin = ApiMeteringPlugin::new("metering-400", ApiMeteringConfig::default());
    let mut target = 0;

    // 1. Completely missing tenant headers (even with x-forwarded-for present) -> 400 Bad Request
    let mut h1 = HeaderWorkspace::empty(4);
    h1.insert(
        HeaderName::from_static("x-forwarded-for"),
        HeaderValue::from_static("203.0.113.10"),
    );
    match plugin.on_upstream_request(&mut build_test_request(h1), &mut target) {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::BAD_REQUEST);
            let Body::Bytes(b) = resp.body else {
                panic!("Expected Body::Bytes");
            };
            let parsed: serde_json::Value = serde_json::from_slice(&b).unwrap();
            assert_eq!(parsed["error"], "bad_request");
        }
        other => panic!("Expected Action::Respond(400), got {other:?}"),
    }

    // 2. Blank/whitespace tenant header -> 400 Bad Request
    let mut h2 = HeaderWorkspace::empty(4);
    h2.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("   "),
    );
    assert!(matches!(
        plugin.on_upstream_request(&mut build_test_request(h2), &mut target),
        Action::Respond(resp) if resp.status == StatusCode::BAD_REQUEST
    ));
}

#[tokio::test]
async fn test_api_metering_etp_wiring_and_shadow_sync() {
    // 1. Validation: distributed.enabled=true without etp_id must fail fast.
    let missing_etp_cfg = PluginConfig {
        id: "api_metering".to_string(),
        config: json!({
            "distributed": {
                "enabled": true,
                "sync_interval_ms": 50
            }
        }),
    };
    assert!(ApiMeteringPlugin::from_config(&missing_etp_cfg).is_err());

    // 2. Start lightweight mock RESP2 server for RedisEtp.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 2048];
        let n = sock.read(&mut buf).await.unwrap();
        let req_str = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(req_str.contains("HINCRBY"));
        assert!(req_str.contains("velda:test:metering:consumed"));
        sock.write_all(b":250000\r\n").await.unwrap();
    });

    let plugin = Arc::new(
        ApiMeteringPlugin::from_config(&PluginConfig {
            id: "api_metering".to_string(),
            config: json!({
                "etp_id": "redis-metering",
                "initial_balance_micro_credits": 1_000_000,
                "default_tier": "payg",
                "tiers": {
                    "payg": {
                        "cost_per_req_micro_credits": 250_000,
                        "exhausted_action": "402",
                        "windows": []
                    }
                },
                "distributed": {
                    "enabled": true,
                    "key_prefix": "velda:test",
                    "sync_interval_ms": 20,
                    "pbww": 25
                }
            }),
        })
        .unwrap(),
    );

    let entry = EtpEntryConfig {
        id: "redis-metering".to_string(),
        kind: EtpKind::Redis,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings: json!({
            "host": "127.0.0.1",
            "port": addr.port(),
            "db": 0
        }),
    };
    let registry = build_registry(std::slice::from_ref(&entry)).unwrap();
    let mut map = FxHashMap::default();
    map.insert(
        "api_metering".to_string(),
        LoadedPlugin::PreUpstream(Arc::clone(&plugin) as Arc<dyn PreUpstreamPlugin>),
    );
    wire_plugin_etps(&map, &registry).unwrap();

    // Spend 250_000 locally -> background loop flushes to mock RedisEtp -> balance reconciled to 750_000.
    let mut h = HeaderWorkspace::empty(4);
    h.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("etp_tenant"),
    );
    let mut target = 0;
    assert!(matches!(
        plugin.on_upstream_request(&mut build_test_request(h), &mut target),
        Action::Continue
    ));

    let (tx, rx) = tokio::sync::watch::channel(false);
    let bg = Arc::clone(&plugin).run_background(rx).unwrap();
    let bg_handle = tokio::spawn(bg);

    server_task.await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let _ = tx.send(true);
    let _ = bg_handle.await;

    // Next request observes reconciled balance (750_000 - 250_000 = 500_000).
    let mut h2 = HeaderWorkspace::empty(4);
    h2.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("etp_tenant"),
    );
    let mut r2 = build_test_request(h2);
    assert!(matches!(
        plugin.on_upstream_request(&mut r2, &mut target),
        Action::Continue
    ));
    assert_eq!(r2.headers.get_u64("x-metering-balance"), Some(500_000));
}

#[test]
fn test_api_metering_hierarchical_3_level_2phase_commit() {
    let mut tiers = FxHashMap::default();
    tiers.insert(
        "hier".to_string(),
        TierConfig {
            name: "hier".to_string(),
            windows: vec![QuotaWindowConfig {
                name: "cap".to_string(),
                window_seconds: 60,
                limit: 2,
                algorithm: QuotaAlgorithm::FixedWindow,
            }],
            cost_per_req_micro_credits: 100,
            exhausted_action: ExhaustedAction::Reject402,
            grace_enabled: false,
            grace_requests: None,
        },
    );
    let plugin = ApiMeteringPlugin::new(
        "metering-hier",
        ApiMeteringConfig {
            account_headers: vec![
                HeaderName::from_static("x-tenant-id"),
                HeaderName::from_static("x-team-id"),
                HeaderName::from_static("x-user-id"),
            ],
            default_tier: "hier".to_string(),
            initial_balance_micro_credits: 1_000,
            tiers,
            ..Default::default()
        },
    );
    let mut target = 0;

    let make_req = |user: &'static str| {
        let mut h = HeaderWorkspace::empty(6);
        h.insert(
            HeaderName::from_static("x-tenant-id"),
            HeaderValue::from_static("acme"),
        );
        h.insert(
            HeaderName::from_static("x-team-id"),
            HeaderValue::from_static("core"),
        );
        h.insert(
            HeaderName::from_static("x-user-id"),
            HeaderValue::from_static(user),
        );
        build_test_request(h)
    };

    // Level 0 (acme), Level 1 (acme:core), Level 2 (acme:core:u1) all pass twice (limit = 2).
    assert!(matches!(
        plugin.on_upstream_request(&mut make_req("u1"), &mut target),
        Action::Continue
    ));
    assert!(matches!(
        plugin.on_upstream_request(&mut make_req("u1"), &mut target),
        Action::Continue
    ));
    // 3rd request from acme:core:u1 hits Level 0 (acme) limit=2 -> 402
    assert!(matches!(
        plugin.on_upstream_request(&mut make_req("u1"), &mut target),
        Action::Respond(resp) if resp.status == StatusCode::PAYMENT_REQUIRED
    ));

    // Distinct tenant "beta" has independent Level 0..2 arenas -> passes cleanly
    let mut hb = HeaderWorkspace::empty(6);
    hb.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("beta"),
    );
    hb.insert(
        HeaderName::from_static("x-user-id"),
        HeaderValue::from_static("u1"),
    );
    assert!(matches!(
        plugin.on_upstream_request(&mut build_test_request(hb), &mut target),
        Action::Continue
    ));
}

#[test]
fn test_api_metering_node_grace_overdraft_bounded() {
    let mut tiers = FxHashMap::default();
    tiers.insert(
        "grace".to_string(),
        TierConfig {
            name: "grace".to_string(),
            windows: vec![QuotaWindowConfig {
                name: "w".to_string(),
                window_seconds: 60,
                limit: 200,
                algorithm: QuotaAlgorithm::FixedWindow,
            }],
            cost_per_req_micro_credits: 0,
            exhausted_action: ExhaustedAction::Reject402,
            grace_enabled: true, // clamp(200 / 200, 2, 1000) == 2
            grace_requests: None,
        },
    );
    let plugin = ApiMeteringPlugin::new(
        "metering-grace",
        ApiMeteringConfig {
            default_tier: "grace".to_string(),
            tiers,
            ..Default::default()
        },
    );
    let mut target = 0;

    // 200 normal + 2 grace = 202 allowed
    for _ in 0..202 {
        let mut h = HeaderWorkspace::empty(4);
        h.insert(
            HeaderName::from_static("x-tenant-id"),
            HeaderValue::from_static("grace_org"),
        );
        assert!(matches!(
            plugin.on_upstream_request(&mut build_test_request(h), &mut target),
            Action::Continue
        ));
    }

    // 203rd request exhausts grace -> 402
    let mut h_over = HeaderWorkspace::empty(4);
    h_over.insert(
        HeaderName::from_static("x-tenant-id"),
        HeaderValue::from_static("grace_org"),
    );
    assert!(matches!(
        plugin.on_upstream_request(&mut build_test_request(h_over), &mut target),
        Action::Respond(resp) if resp.status == StatusCode::PAYMENT_REQUIRED
    ));
}

#[test]
fn test_api_metering_multi_core_hot_org_concurrent_leases() {
    let mut tiers = FxHashMap::default();
    tiers.insert(
        "hot".to_string(),
        TierConfig {
            name: "hot".to_string(),
            windows: vec![QuotaWindowConfig {
                name: "w".to_string(),
                window_seconds: 3600,
                limit: 1_000,
                algorithm: QuotaAlgorithm::FixedWindow,
            }],
            cost_per_req_micro_credits: 0,
            exhausted_action: ExhaustedAction::Reject429,
            grace_enabled: true, // node_grace = clamp(1000 / 200, 2, 1000) = 5
            grace_requests: None,
        },
    );
    let plugin = Arc::new(ApiMeteringPlugin::new(
        "metering-multicore",
        ApiMeteringConfig {
            default_tier: "hot".to_string(),
            tiers,
            ..Default::default()
        },
    ));

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let p = Arc::clone(&plugin);
            std::thread::spawn(move || {
                let mut ok = 0u64;
                let mut target = 0;
                for _ in 0..200 {
                    let mut h = HeaderWorkspace::empty(4);
                    h.insert(
                        HeaderName::from_static("x-tenant-id"),
                        HeaderValue::from_static("hot_org"),
                    );
                    if matches!(
                        p.on_upstream_request(&mut build_test_request(h), &mut target),
                        Action::Continue
                    ) {
                        ok += 1;
                    }
                }
                ok
            })
        })
        .collect();

    let total_ok: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    // Multi-core lease + node grace guarantees >= 1000 (no false 429s) and bounded <= 1005
    assert!(
        (1_000..=1_005).contains(&total_ok),
        "Expected total_ok in 1000..=1005, got {total_ok}"
    );
}
