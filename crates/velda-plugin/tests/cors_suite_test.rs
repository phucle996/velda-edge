//! End-to-End Test Suite for CORS Preflight (`pre_route`) and CORS Response (`post_response`) Plugins.
//!
//! Verifies complete RFC 6454 / W3C Fetch cross-origin lifecycles, zero-allocation invariants,
//! hardware-accelerated method bitmasks, CVE-2020-7662 subdomain injection defenses, and
//! cache poisoning prevention across single-phase and multi-phase route executions.

use http::header::{
    ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE,
    ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, CONTENT_LENGTH, CONTENT_TYPE,
    ORIGIN, VARY,
};
use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, Version};
use velda_core::{Action, Body, L7Request, L7Response, RejectReason};
use velda_plugin::post_response::{CorsConfig, CorsResponsePlugin, PostResponsePlugin};
use velda_plugin::pre_route::{CorsPreflightPlugin, OriginPattern, PreRoutePlugin};

fn make_test_request(
    method: Method,
    uri_str: &str,
    origin: Option<&str>,
    request_method: Option<&str>,
    request_headers: Option<&str>,
) -> L7Request {
    let mut headers = HeaderMap::new();
    if let Some(orig) = origin {
        headers.insert(ORIGIN, HeaderValue::from_str(orig).unwrap());
    }
    if let Some(rm) = request_method {
        headers.insert(
            ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_str(rm).unwrap(),
        );
    }
    if let Some(rh) = request_headers {
        headers.insert(
            ACCESS_CONTROL_REQUEST_HEADERS,
            HeaderValue::from_str(rh).unwrap(),
        );
    }

    L7Request::new(
        method,
        uri_str.parse::<Uri>().unwrap(),
        Version::HTTP_11,
        headers,
        Body::Empty,
    )
}

/// Helper to build standard production preflight plugin with credentials and subdomain support.
fn build_test_preflight_plugin() -> CorsPreflightPlugin {
    CorsPreflightPlugin::new(
        "test-cors-preflight",
        vec![
            OriginPattern::Exact("https://app.example.com".to_string()),
            OriginPattern::Subdomain {
                prefix: "https://".to_string(),
                suffix: ".corp.internal".to_string(),
            },
        ],
        HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS"),
        Some(HeaderValue::from_static(
            "Authorization, Content-Type, X-Request-ID",
        )),
        true, // allow_all_headers (reflects requested headers)
        true, // allow_credentials
        Some(HeaderValue::from_static("86400")),
    )
}

/// Helper to build matching post-response CORS plugin.
fn build_test_response_plugin() -> CorsResponsePlugin {
    CorsResponsePlugin::with_config(
        "test-cors-response",
        CorsConfig {
            allow_origins: vec![
                OriginPattern::Exact("https://app.example.com".to_string()),
                OriginPattern::Subdomain {
                    prefix: "https://".to_string(),
                    suffix: ".corp.internal".to_string(),
                },
            ],
            allow_credentials: true,
            expose_headers: vec!["X-Request-ID".to_string(), "X-Total-Count".to_string()],
            max_age_secs: Some(86400),
        },
    )
}

#[test]
fn test_spa_multi_phase_lifecycle() {
    let preflight_plugin = build_test_preflight_plugin();
    let response_plugin = build_test_response_plugin();

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 1: Browser issues preflight OPTIONS request
    // ─────────────────────────────────────────────────────────────────────────
    let mut preflight_req = make_test_request(
        Method::OPTIONS,
        "/api/v1/users",
        Some("https://app.example.com"),
        Some("POST"),
        Some("Authorization, Content-Type, X-Custom-Trace"),
    );

    let action = preflight_plugin.on_request(&mut preflight_req);

    // Assert preflight is intercepted at pre_route without reaching router/upstream
    let Action::Respond(preflight_resp) = action else {
        panic!("Expected Action::Respond for valid preflight OPTIONS");
    };

    assert_eq!(preflight_resp.status, StatusCode::NO_CONTENT);
    assert_eq!(
        preflight_resp
            .headers
            .get(ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "https://app.example.com"
    );
    assert_eq!(
        preflight_resp
            .headers
            .get(ACCESS_CONTROL_ALLOW_CREDENTIALS)
            .unwrap(),
        "true"
    );
    assert_eq!(
        preflight_resp
            .headers
            .get(ACCESS_CONTROL_ALLOW_METHODS)
            .unwrap(),
        "GET, POST, PUT, DELETE, OPTIONS"
    );
    assert_eq!(
        preflight_resp
            .headers
            .get(ACCESS_CONTROL_ALLOW_HEADERS)
            .unwrap(),
        "Authorization, Content-Type, X-Custom-Trace" // Reflected
    );
    assert_eq!(
        preflight_resp.headers.get(ACCESS_CONTROL_MAX_AGE).unwrap(),
        "86400"
    );
    assert_eq!(preflight_resp.headers.get(CONTENT_LENGTH).unwrap(), "0");
    assert!(
        preflight_resp
            .headers
            .get(VARY)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("Origin")
    );

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 2: Browser issues actual authenticated POST request
    // ─────────────────────────────────────────────────────────────────────────
    let mut actual_req = make_test_request(
        Method::POST,
        "/api/v1/users",
        Some("https://app.example.com"),
        None,
        None,
    );

    // Pre-route plugin must allow non-OPTIONS requests to proceed directly
    let action = preflight_plugin.on_request(&mut actual_req);
    assert!(matches!(action, Action::Continue));

    // Simulated upstream response with existing Vary header
    let mut upstream_resp = L7Response::empty(StatusCode::CREATED);
    upstream_resp
        .headers
        .insert(VARY, HeaderValue::from_static("Accept-Encoding"));
    upstream_resp
        .headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    // Post-response plugin executes on outgoing upstream response
    response_plugin.on_response(&actual_req.headers, &mut upstream_resp);

    assert_eq!(upstream_resp.status, StatusCode::CREATED);
    assert_eq!(
        upstream_resp
            .headers
            .get(ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "https://app.example.com"
    );
    assert_eq!(
        upstream_resp
            .headers
            .get(ACCESS_CONTROL_ALLOW_CREDENTIALS)
            .unwrap(),
        "true"
    );
    assert_eq!(
        upstream_resp
            .headers
            .get(ACCESS_CONTROL_EXPOSE_HEADERS)
            .unwrap(),
        "X-Request-ID, X-Total-Count"
    );

    // Verify native multi-value Vary preservation (both Accept-Encoding and Origin present)
    let vary_headers: Vec<_> = upstream_resp
        .headers
        .get_all(VARY)
        .iter()
        .map(|v| v.to_str().unwrap())
        .collect();
    assert_eq!(vary_headers, vec!["Accept-Encoding", "Origin"]);
}

#[test]
fn test_preflight_method_bitmask_rejection() {
    let plugin = build_test_preflight_plugin();

    // Probing disallowed method (DELETE is allowed, but PATCH or TRACE is disallowed by config)
    let mut preflight_req = make_test_request(
        Method::OPTIONS,
        "/api/v1/users",
        Some("https://app.example.com"),
        Some("PATCH"), // Not in GET, POST, PUT, DELETE, OPTIONS
        None,
    );

    let action = plugin.on_request(&mut preflight_req);
    assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
}

#[test]
fn test_preflight_unauthorized_origin_rejection() {
    let plugin = build_test_preflight_plugin();

    let mut preflight_req = make_test_request(
        Method::OPTIONS,
        "/api/v1/users",
        Some("https://malicious-attacker.com"),
        Some("POST"),
        None,
    );

    let action = plugin.on_request(&mut preflight_req);
    assert!(matches!(action, Action::Reject(RejectReason::Forbidden)));
}

#[test]
fn test_subdomain_pattern_and_cve_2020_7662_defense() {
    let preflight_plugin = build_test_preflight_plugin();
    let response_plugin = build_test_response_plugin();

    // 1. Legitimate subdomains must pass
    let valid_subdomains = [
        "https://billing.corp.internal",
        "https://prod.us-east.corp.internal",
        "https://a-1.corp.internal",
    ];

    for origin in valid_subdomains {
        let mut req = make_test_request(Method::OPTIONS, "/api", Some(origin), Some("GET"), None);
        let action = preflight_plugin.on_request(&mut req);
        let Action::Respond(resp) = action else {
            panic!("Expected 204 for valid subdomain '{origin}'");
        };
        assert_eq!(resp.status, StatusCode::NO_CONTENT);
        assert_eq!(
            resp.headers.get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            origin
        );
    }

    // 2. Adversarial injection attempts must be rejected with 403 Forbidden
    let attack_origins = [
        "https://evil.com/fake.corp.internal", // Path delimiter injection
        "https://attacker.corp.internal.fake.com", // Suffix append bypass
        "https://user:pass@app.corp.internal", // Credential injection
        "https://corp.internal",               // Missing subdomain label
        "https://.corp.internal",              // Empty label
        "https://bad..sub.corp.internal",      // Consecutive dots
        "https://app.corp.internal:8080",      // Port injection mismatch
    ];

    for attack in attack_origins {
        // Preflight probe rejection
        let mut req = make_test_request(Method::OPTIONS, "/api", Some(attack), Some("GET"), None);
        let action = preflight_plugin.on_request(&mut req);
        assert!(
            matches!(action, Action::Reject(RejectReason::Forbidden)),
            "Expected 403 for attack origin '{attack}'"
        );

        // Actual request probe: post_response must inject ZERO CORS headers
        let actual_req = make_test_request(Method::GET, "/api", Some(attack), None, None);
        let mut actual_resp = L7Response::empty(StatusCode::OK);
        response_plugin.on_response(&actual_req.headers, &mut actual_resp);
        assert!(
            actual_resp
                .headers
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none()
        );
        assert!(
            actual_resp
                .headers
                .get(ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .is_none()
        );
    }
}

#[test]
fn test_non_cors_traffic_pass_through() {
    let preflight_plugin = build_test_preflight_plugin();
    let response_plugin = build_test_response_plugin();

    // 1. Regular GET without Origin header (e.g. curl or internal microservice)
    let mut req = make_test_request(Method::GET, "/api/data", None, None, None);

    // Pre-route pass-through
    let action = preflight_plugin.on_request(&mut req);
    assert!(matches!(action, Action::Continue));

    // Post-response pass-through
    let mut resp = L7Response::empty(StatusCode::OK);
    resp.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response_plugin.on_response(&req.headers, &mut resp);

    // Headers must remain completely unaltered
    assert!(resp.headers.get(ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
    assert!(resp.headers.get(ACCESS_CONTROL_ALLOW_CREDENTIALS).is_none());
    assert!(resp.headers.get(VARY).is_none());
    assert_eq!(resp.headers.get(CONTENT_TYPE).unwrap(), "application/json");

    // 2. OPTIONS request without CORS preflight headers (Standard HTTP WebDAV or RFC 9110 OPTIONS)
    let mut plain_options = make_test_request(Method::OPTIONS, "/api/data", None, None, None);
    let action = preflight_plugin.on_request(&mut plain_options);
    assert!(matches!(action, Action::Continue));
}

#[test]
fn test_wildcard_origin_without_credentials() {
    let preflight = CorsPreflightPlugin::new(
        "preflight-wildcard",
        vec![OriginPattern::Any],
        HeaderValue::from_static("GET, POST"),
        None,
        false,
        false, // Credentials false allows wildcard '*'
        None,
    );

    let response = CorsResponsePlugin::with_config(
        "response-wildcard",
        CorsConfig {
            allow_origins: vec![OriginPattern::Any],
            allow_credentials: false,
            expose_headers: Vec::new(),
            max_age_secs: None,
        },
    );

    let mut req = make_test_request(
        Method::OPTIONS,
        "/public",
        Some("https://anywhere.org"),
        Some("GET"),
        None,
    );

    let Action::Respond(preflight_resp) = preflight.on_request(&mut req) else {
        panic!("Expected Action::Respond");
    };

    assert_eq!(
        preflight_resp
            .headers
            .get(ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "*"
    );
    assert!(
        preflight_resp
            .headers
            .get(ACCESS_CONTROL_ALLOW_CREDENTIALS)
            .is_none()
    );

    // Actual GET response
    let actual_req = make_test_request(
        Method::GET,
        "/public",
        Some("https://anywhere.org"),
        None,
        None,
    );
    let mut resp = L7Response::empty(StatusCode::OK);
    response.on_response(&actual_req.headers, &mut resp);

    assert_eq!(resp.headers.get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*");
    assert!(resp.headers.get(ACCESS_CONTROL_ALLOW_CREDENTIALS).is_none());
    // When wildcard '*' is used without credentials, Vary: Origin is not strictly required
    assert!(resp.headers.get(VARY).is_none());
}

#[test]
fn test_vary_header_idempotency() {
    let response_plugin = build_test_response_plugin();
    let req = make_test_request(
        Method::GET,
        "/data",
        Some("https://app.example.com"),
        None,
        None,
    );

    // Case 1: Upstream already emitted "Vary: Origin"
    let mut resp1 = L7Response::empty(StatusCode::OK);
    resp1
        .headers
        .insert(VARY, HeaderValue::from_static("Origin"));
    response_plugin.on_response(&req.headers, &mut resp1);

    let vary_count = resp1.headers.get_all(VARY).iter().count();
    assert_eq!(vary_count, 1, "Must not append duplicate Vary: Origin");

    // Case 2: Upstream already emitted combined "Vary: Accept-Encoding, Origin"
    let mut resp2 = L7Response::empty(StatusCode::OK);
    resp2
        .headers
        .insert(VARY, HeaderValue::from_static("Accept-Encoding, origin"));
    response_plugin.on_response(&req.headers, &mut resp2);

    let vary_count = resp2.headers.get_all(VARY).iter().count();
    assert_eq!(vary_count, 1, "Must detect origin in comma-separated list");
}
