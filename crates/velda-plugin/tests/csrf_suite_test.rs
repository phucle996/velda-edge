//! End-to-End Test Suite for CSRF Protection Plugin (`pre_upstream: csrf`).
//!
//! Verifies complete OWASP / RFC 6454 / W3C Fetch Metadata protection lifecycles,
//! Simple Request form traps, Subdomain Cookie Tossing defenses (HMAC-SHA256 signed stateless tokens),
//! and multi-phase pipeline interaction with `cors_preflight`.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use http::header::{CONTENT_TYPE, COOKIE, HOST, ORIGIN};
use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, Version};
use ring::hmac;
use rustc_hash::FxHashMap;
use velda_core::{Action, Body, L7Request, ProtocolFamily, StreamingMode};
use velda_plugin::PreUpstreamPlugin;
use velda_plugin::factory::{LoadedPlugin, build_route_plugin_plan};
use velda_plugin::pre_route::{CorsPreflightPlugin, OriginPattern};
use velda_plugin::pre_upstream::{CsrfConfig, CsrfPlugin};

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn build_test_request(method: Method, uri_str: &str, headers: HeaderMap) -> L7Request {
    L7Request::new(
        method,
        uri_str.parse::<Uri>().unwrap(),
        Version::HTTP_11,
        headers,
        Body::Empty,
    )
}

fn extract_body_str(action: &Action) -> String {
    match action {
        Action::Respond(resp) => match &resp.body {
            Body::Bytes(b) => String::from_utf8_lossy(b).to_string(),
            Body::Empty => String::new(),
        },
        _ => String::new(),
    }
}

#[test]
fn test_csrf_neutralizes_w3c_simple_request_trap() {
    // ─────────────────────────────────────────────────────────────────────────
    // Multi-phase integration test:
    // Route configured with both `cors_preflight` (pre_route) and `csrf` (pre_upstream).
    // An attacker uses an HTML <form> (Simple Request) to bypass CORS preflight.
    // The `csrf` plugin at pre_upstream catches and terminates it with 403 Forbidden.
    // ─────────────────────────────────────────────────────────────────────────
    let preflight_plugin = Arc::new(CorsPreflightPlugin::new(
        "cors-gate",
        vec![OriginPattern::Exact("https://app.example.com".to_string())],
        HeaderValue::from_static("GET, POST, OPTIONS"),
        Some(HeaderValue::from_static("Content-Type, X-CSRF-Token")),
        false,
        true,
        Some(HeaderValue::from_static("86400")),
    ));

    let csrf_plugin = Arc::new(
        CsrfPlugin::new(
            "csrf-gate",
            CsrfConfig {
                cookie_name: "__Host-velda_csrf".to_string(),
                header_name: "X-CSRF-Token".to_string(),
                secret_key: Some("sec_k89f2a019482bca881726c0981273412".to_string()),
                token_ttl_secs: 7200,
                safe_methods: vec!["GET".to_string(), "HEAD".to_string(), "OPTIONS".to_string()],
                trusted_origins: vec!["https://app.example.com".to_string()],
                enforce_fetch_metadata: true,
                enforce_origin: true,
            },
        )
        .unwrap(),
    );

    let mut available_plugins = FxHashMap::default();
    available_plugins.insert(
        "cors-gate".to_string(),
        LoadedPlugin::PreRoute(preflight_plugin),
    );
    available_plugins.insert(
        "csrf-gate".to_string(),
        LoadedPlugin::PreUpstream(csrf_plugin),
    );

    let plan = build_route_plugin_plan(
        "route_transfer",
        &["cors-gate".to_string(), "csrf-gate".to_string()],
        &available_plugins,
        ProtocolFamily::Http,
        StreamingMode::DISABLED,
    )
    .unwrap();

    // 1. Attacker sends HTML <form> POST (Simple Request): No Preflight, has victim's cookie, NO custom header
    let mut attacker_headers = HeaderMap::new();
    attacker_headers.insert(HOST, HeaderValue::from_static("api.example.com"));
    attacker_headers.insert(
        ORIGIN,
        HeaderValue::from_static("https://evil-attacker.com"),
    );
    attacker_headers.insert(
        COOKIE,
        HeaderValue::from_static("__Host-velda_csrf=some_token"),
    );
    attacker_headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    attacker_headers.insert("sec-fetch-site", HeaderValue::from_static("cross-site"));

    let mut attacker_req = build_test_request(Method::POST, "/api/transfer", attacker_headers);
    let mut target: Arc<str> = Arc::from("core_fintech_upstream");

    // Phase 1: Pre-route plugins evaluate. Simple request is POST (not OPTIONS), so cors_preflight continues!
    let pre_route_hooks = plan.pre_route_hooks.as_deref().unwrap_or(&[]);
    let pre_route_action =
        velda_plugin::pre_route::run_pre_route_plugins(pre_route_hooks, &mut attacker_req);
    assert!(
        matches!(pre_route_action, Action::Continue),
        "CORS preflight passes Simple Request!"
    );

    // Phase 2: Pre-upstream plugins evaluate. CSRF plugin intercepts and terminates the malicious request!
    let pre_upstream_hooks = plan.pre_upstream_hooks.as_deref().unwrap_or(&[]);
    let pre_upstream_action = velda_plugin::pre_upstream::run_pre_upstream_plugins(
        pre_upstream_hooks,
        &mut attacker_req,
        &mut target,
    );
    let Action::Respond(resp) = pre_upstream_action else {
        panic!("Expected CSRF plugin to block Simple Request form bypass!");
    };

    assert_eq!(resp.status, StatusCode::FORBIDDEN);
    let body_str = String::from_utf8_lossy(match &resp.body {
        Body::Bytes(b) => b,
        Body::Empty => b"",
    });
    assert!(
        body_str.contains("CSRF"),
        "Body must state CSRF verification failed: {body_str}"
    );

    // 2. Legitimate SPA request: Sends Origin, Cookie, and matching valid X-CSRF-Token
    let secret = "sec_k89f2a019482bca881726c0981273412";
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    let nonce = [0x55u8; 16];
    let now = current_epoch_secs();
    let valid_token = CsrfPlugin::mint_token(&key, &nonce, now);

    let mut legit_headers = HeaderMap::new();
    legit_headers.insert(HOST, HeaderValue::from_static("api.example.com"));
    legit_headers.insert(ORIGIN, HeaderValue::from_static("https://app.example.com"));
    legit_headers.insert(
        COOKIE,
        HeaderValue::from_str(&format!("__Host-velda_csrf={valid_token}")).unwrap(),
    );
    legit_headers.insert("X-CSRF-Token", HeaderValue::from_str(&valid_token).unwrap());
    legit_headers.insert("sec-fetch-site", HeaderValue::from_static("same-site"));

    let mut legit_req = build_test_request(Method::POST, "/api/transfer", legit_headers);
    let pre_route_action =
        velda_plugin::pre_route::run_pre_route_plugins(pre_route_hooks, &mut legit_req);
    assert!(matches!(pre_route_action, Action::Continue));

    let pre_upstream_action = velda_plugin::pre_upstream::run_pre_upstream_plugins(
        pre_upstream_hooks,
        &mut legit_req,
        &mut target,
    );
    assert!(
        matches!(pre_upstream_action, Action::Continue),
        "Legitimate request with valid signed CSRF token must pass!"
    );
}

#[test]
fn test_subdomain_cookie_tossing_defense() {
    let secret = "sec_k89f2a019482bca881726c0981273412";
    let plugin = CsrfPlugin::new(
        "csrf-test",
        CsrfConfig {
            cookie_name: "__Host-csrf".to_string(),
            header_name: "X-CSRF-Token".to_string(),
            secret_key: Some(secret.to_string()),
            token_ttl_secs: 3600,
            safe_methods: vec!["GET".to_string()],
            trusted_origins: vec!["https://example.com".to_string()],
            enforce_fetch_metadata: false,
            enforce_origin: true,
        },
    )
    .unwrap();

    let mut target: Arc<str> = Arc::from("upstream");

    // Attacker on sub.example.com tossed a cookie with a fabricated token
    let forged_token = "000000006718a999.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    let mut headers = HeaderMap::new();
    headers.insert(HOST, HeaderValue::from_static("example.com"));
    headers.insert(ORIGIN, HeaderValue::from_static("https://example.com"));
    headers.insert(
        COOKIE,
        HeaderValue::from_str(&format!("__Host-csrf={forged_token}")).unwrap(),
    );
    headers.insert("X-CSRF-Token", HeaderValue::from_str(forged_token).unwrap());

    let mut req = build_test_request(Method::POST, "/account/delete", headers);
    let action = plugin.on_upstream_request(&mut req, &mut target);

    let Action::Respond(resp) = &action else {
        panic!("Expected 403 Forbidden for forged HMAC signature");
    };
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
    assert!(extract_body_str(&action).contains("signed token invalid"));
}

#[test]
fn test_fetch_metadata_and_origin_enforcement() {
    let plugin = CsrfPlugin::new(
        "csrf-test",
        CsrfConfig {
            cookie_name: "csrf_cookie".to_string(),
            header_name: "X-CSRF-Token".to_string(),
            secret_key: None, // Pure Double-Submit mode
            token_ttl_secs: 3600,
            safe_methods: vec!["GET".to_string(), "HEAD".to_string()],
            trusted_origins: vec!["https://trusted.example.com".to_string()],
            enforce_fetch_metadata: true,
            enforce_origin: true,
        },
    )
    .unwrap();

    let mut target: Arc<str> = Arc::from("upstream");

    // Case 1: Origin mismatch against Host and trusted_origins
    let mut headers_mismatch = HeaderMap::new();
    headers_mismatch.insert(HOST, HeaderValue::from_static("api.example.com"));
    headers_mismatch.insert(ORIGIN, HeaderValue::from_static("https://untrusted.com"));
    headers_mismatch.insert(COOKIE, HeaderValue::from_static("csrf_cookie=token123"));
    headers_mismatch.insert("X-CSRF-Token", HeaderValue::from_static("token123"));

    let mut req_mismatch = build_test_request(Method::POST, "/data", headers_mismatch);
    let action_mismatch = plugin.on_upstream_request(&mut req_mismatch, &mut target);
    assert!(matches!(action_mismatch, Action::Respond(_)));

    // Case 2: Origin matching Host header -> Passes origin gate
    let mut headers_valid = HeaderMap::new();
    headers_valid.insert(HOST, HeaderValue::from_static("api.example.com"));
    headers_valid.insert(ORIGIN, HeaderValue::from_static("https://api.example.com"));
    headers_valid.insert(COOKIE, HeaderValue::from_static("csrf_cookie=token123"));
    headers_valid.insert("X-CSRF-Token", HeaderValue::from_static("token123"));

    let mut req_valid = build_test_request(Method::POST, "/data", headers_valid);
    let action_valid = plugin.on_upstream_request(&mut req_valid, &mut target);
    assert!(matches!(action_valid, Action::Continue));
}
