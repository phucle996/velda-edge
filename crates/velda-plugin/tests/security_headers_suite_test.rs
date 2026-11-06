//! Comprehensive Integration Test Suite for Security Headers Post-Response Plugin (`security_headers`).
//!
//! Verifies:
//! - Dual-representation execution (<= 16 bytes in hardware registers, > 16 bytes as pointers)
//! - Complete OWASP & W3C defensive response headers injection
//! - Modern origin isolation (COOP, COEP, CORP) and cross-domain policy directives
//! - Zero-allocation contiguous flat array behavior
//! - 64-byte L1 cacheline alignment and ALU bitmask dispatch
//! - Declarative validation and configuration lifecycle from `PluginConfig`
//! - Selective feature disabling and explicit sentinels

use http::StatusCode;
use serde_json::json;
use velda_core::{HeaderWorkspace, L7Response};
use velda_plugin::post_response::PostResponsePlugin;
use velda_plugin::post_response::security_headers::{
    CompiledHeader, FLAG_COEP, FLAG_COOP, FLAG_CORP, FLAG_CROSS_DOMAIN, FLAG_CSP,
    FLAG_FRAME_OPTIONS, FLAG_HSTS, FLAG_NOSNIFF, FLAG_PERMISSIONS_POLICY, FLAG_REFERRER_POLICY,
    FLAG_XSS_PROTECTION, HstsConfig, SecurityHeadersConfig, SecurityHeadersPlugin,
};
use velda_sync::post_sync::plugin::PluginConfig;

#[test]
fn test_suite_default_owasp_hardening() {
    let plugin = SecurityHeadersPlugin::new("sec-default-suite");
    let mut resp = L7Response::from_bytes(StatusCode::OK, "{\"status\":\"healthy\"}");
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

    // Verify dual-representation: short headers <= 16 bytes are in numeric integer registers
    assert!(
        resp.headers.numeric_len > 0,
        "Short headers must be written directly to HeaderWorkspace numeric registers"
    );

    resp.headers.render();

    assert_eq!(
        resp.headers.get("strict-transport-security").unwrap(),
        "max-age=31536000; includeSubDomains"
    );
    assert_eq!(
        resp.headers.get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(resp.headers.get("x-frame-options").unwrap(), "DENY");
    assert_eq!(
        resp.headers.get("referrer-policy").unwrap(),
        "strict-origin-when-cross-origin"
    );
    assert!(!resp.headers.contains_key("content-security-policy"));
    assert!(!resp.headers.contains_key("permissions-policy"));
    assert!(!resp.headers.contains_key("cross-origin-opener-policy"));
    assert!(!resp.headers.contains_key("cross-origin-embedder-policy"));
    assert!(!resp.headers.contains_key("cross-origin-resource-policy"));

    // Verify bitmask matches default features
    let expected_mask = FLAG_HSTS | FLAG_NOSNIFF | FLAG_FRAME_OPTIONS | FLAG_REFERRER_POLICY;
    assert_eq!(plugin.op_mask, expected_mask);
}

#[test]
fn test_suite_modern_browser_isolation_full_stack() {
    let config = SecurityHeadersConfig {
        hsts: HstsConfig {
            enable: true,
            max_age_secs: 63_072_000,
            include_subdomains: true,
            preload: true,
        },
        frame_options: Some("SAMEORIGIN".to_string()),
        content_type_nosniff: true,
        referrer_policy: Some("no-referrer".to_string()),
        csp: Some("default-src 'self'; frame-ancestors 'self'".to_string()),
        permissions_policy: Some("geolocation=(), microphone=(), camera=()".to_string()),
        cross_origin_opener_policy: Some("same-origin".to_string()),
        cross_origin_embedder_policy: Some("require-corp".to_string()),
        cross_origin_resource_policy: Some("same-origin".to_string()),
        cross_domain_policies: Some("none".to_string()),
        xss_protection: Some("0".to_string()),
    };

    let plugin = SecurityHeadersPlugin::with_config("sec-full-isolation", config);
    let mut resp = L7Response::from_bytes(StatusCode::OK, "{\"status\":\"isolated\"}");
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

    // Verify dual representation partitioning in compiled headers:
    let compiled = plugin.compiled_headers();
    let register_count = compiled
        .iter()
        .filter(|h| matches!(h, CompiledHeader::InlineRegister { .. }))
        .count();
    let pointer_count = compiled
        .iter()
        .filter(|h| matches!(h, CompiledHeader::StandardPointer { .. }))
        .count();

    // nosniff (7B), SAMEORIGIN (10B), no-referrer (11B), same-origin (11B), require-corp (12B), same-origin (11B), none (4B), 0 (1B) -> 8 inline registers!
    assert_eq!(register_count, 8);
    // HSTS (36B), CSP (>16B), Permissions (>16B) -> 3 standard pointers!
    assert_eq!(pointer_count, 3);

    resp.headers.render();

    assert_eq!(
        resp.headers.get("strict-transport-security").unwrap(),
        "max-age=63072000; includeSubDomains; preload"
    );
    assert_eq!(resp.headers.get("x-frame-options").unwrap(), "SAMEORIGIN");
    assert_eq!(
        resp.headers.get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(resp.headers.get("referrer-policy").unwrap(), "no-referrer");
    assert_eq!(
        resp.headers.get("content-security-policy").unwrap(),
        "default-src 'self'; frame-ancestors 'self'"
    );
    assert_eq!(
        resp.headers.get("permissions-policy").unwrap(),
        "geolocation=(), microphone=(), camera=()"
    );
    assert_eq!(
        resp.headers.get("cross-origin-opener-policy").unwrap(),
        "same-origin"
    );
    assert_eq!(
        resp.headers.get("cross-origin-embedder-policy").unwrap(),
        "require-corp"
    );
    assert_eq!(
        resp.headers.get("cross-origin-resource-policy").unwrap(),
        "same-origin"
    );
    assert_eq!(
        resp.headers
            .get("x-permitted-cross-domain-policies")
            .unwrap(),
        "none"
    );
    assert_eq!(resp.headers.get("x-xss-protection").unwrap(), "0");

    let all_flags = FLAG_HSTS
        | FLAG_NOSNIFF
        | FLAG_FRAME_OPTIONS
        | FLAG_REFERRER_POLICY
        | FLAG_CSP
        | FLAG_PERMISSIONS_POLICY
        | FLAG_COOP
        | FLAG_COEP
        | FLAG_CORP
        | FLAG_CROSS_DOMAIN
        | FLAG_XSS_PROTECTION;
    assert_eq!(plugin.op_mask, all_flags);
    assert_eq!(plugin.compiled_headers().len(), 11);
}

#[test]
fn test_suite_from_config_full_manifest_roundtrip() {
    let manifest_json = json!({
        "hsts": {
            "enable": true,
            "max_age_secs": 31536000,
            "include_subdomains": true,
            "preload": false
        },
        "frame_options": "DENY",
        "content_type_nosniff": true,
        "referrer_policy": "strict-origin-when-cross-origin",
        "content_security_policy": "script-src 'self' https://trustedscripts.example.com",
        "permissions_policy": null,
        "cross_origin_opener_policy": "same-origin",
        "cross_origin_embedder_policy": "credentialless",
        "cross_origin_resource_policy": "cross-origin",
        "cross_domain_policies": "master-only",
        "xss_protection": "0"
    });

    let plugin_cfg = PluginConfig {
        id: "edge_sec_enterprise".to_string(),
        phase: "post_response".to_string(),
        config: manifest_json,
    };

    let plugin = SecurityHeadersPlugin::from_config(&plugin_cfg).expect("valid configuration");
    assert_eq!(plugin.id(), "edge_sec_enterprise");

    let mut resp = L7Response::from_bytes(StatusCode::OK, "{\"ok\":true}");
    let req_headers = HeaderWorkspace::default();
    plugin.on_response(&req_headers, &mut resp);
    resp.headers.render();

    assert_eq!(
        resp.headers.get("cross-origin-opener-policy").unwrap(),
        "same-origin"
    );
    assert_eq!(
        resp.headers.get("cross-origin-embedder-policy").unwrap(),
        "credentialless"
    );
    assert_eq!(
        resp.headers.get("cross-origin-resource-policy").unwrap(),
        "cross-origin"
    );
    assert_eq!(
        resp.headers
            .get("x-permitted-cross-domain-policies")
            .unwrap(),
        "master-only"
    );
    assert_eq!(resp.headers.get("x-xss-protection").unwrap(), "0");
    assert_eq!(
        resp.headers.get("content-security-policy").unwrap(),
        "script-src 'self' https://trustedscripts.example.com"
    );
}

#[test]
fn test_suite_hardware_alignment_and_zero_indirection() {
    assert_eq!(
        std::mem::align_of::<SecurityHeadersPlugin>(),
        64,
        "Plugin struct must align to 64-byte L1 CPU cacheline boundary"
    );

    let plugin = SecurityHeadersPlugin::new("align-test");
    let headers = plugin.compiled_headers();
    assert_eq!(headers.len(), 4);

    // Ensure memory address of headers points within the struct boundary (no heap pointer indirection)
    let plugin_addr = &plugin as *const _ as usize;
    let headers_slice_addr = headers.as_ptr() as usize;
    let struct_size = std::mem::size_of::<SecurityHeadersPlugin>();
    assert!(
        headers_slice_addr >= plugin_addr && headers_slice_addr < plugin_addr + struct_size,
        "Headers array must be stored contiguously inline within the struct, not on the heap"
    );
}

#[test]
fn test_suite_selective_disabled_state() {
    let empty_cfg = SecurityHeadersConfig {
        hsts: HstsConfig {
            enable: false,
            max_age_secs: 0,
            include_subdomains: false,
            preload: false,
        },
        frame_options: None,
        content_type_nosniff: false,
        referrer_policy: None,
        csp: None,
        permissions_policy: None,
        cross_origin_opener_policy: None,
        cross_origin_embedder_policy: None,
        cross_origin_resource_policy: None,
        cross_domain_policies: None,
        xss_protection: None,
    };

    let plugin = SecurityHeadersPlugin::with_config("disabled-sec", empty_cfg);
    assert_eq!(plugin.op_mask, 0);
    assert_eq!(plugin.compiled_headers().len(), 0);

    let mut resp = L7Response::from_bytes(StatusCode::OK, "");
    let req_headers = HeaderWorkspace::default();
    plugin.on_response(&req_headers, &mut resp);
    resp.headers.render();

    assert!(!resp.headers.contains_key("strict-transport-security"));
    assert!(!resp.headers.contains_key("x-content-type-options"));
    assert!(!resp.headers.contains_key("x-frame-options"));
    assert!(!resp.headers.contains_key("referrer-policy"));
    assert!(!resp.headers.contains_key("content-security-policy"));
    assert!(!resp.headers.contains_key("permissions-policy"));
    assert!(!resp.headers.contains_key("cross-origin-opener-policy"));
    assert!(!resp.headers.contains_key("cross-origin-embedder-policy"));
    assert!(!resp.headers.contains_key("cross-origin-resource-policy"));
    assert!(
        !resp
            .headers
            .contains_key("x-permitted-cross-domain-policies")
    );
    assert!(!resp.headers.contains_key("x-xss-protection"));
}

#[test]
fn test_suite_extreme_heavyweight_headers_and_partitioning() {
    let heavy_csp = "default-src 'self' https://api.velda.io https://cdn.velda.io https://assets.velda.io; script-src 'self' 'unsafe-inline' https://analytics.google.com; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; connect-src https://*.velda.io wss://*.velda.io".to_string();
    let heavy_permissions = "camera=(), microphone=(), geolocation=(self \"https://maps.velda.io\"), payment=(self \"https://pay.stripe.com\"), display-capture=(), usb=()".to_string();

    let extreme_cfg = SecurityHeadersConfig {
        hsts: HstsConfig {
            enable: true,
            max_age_secs: 63_072_000,
            include_subdomains: true,
            preload: true,
        },
        frame_options: Some("SAMEORIGIN".to_string()), // 10 bytes <= 16 -> InlineRegister
        content_type_nosniff: true,                    // 7 bytes <= 16 -> InlineRegister
        referrer_policy: Some("strict-origin-when-cross-origin".to_string()), // 32 bytes > 16 -> StandardPointer
        csp: Some(heavy_csp.clone()), // 258 bytes > 16 -> StandardPointer
        permissions_policy: Some(heavy_permissions.clone()), // 156 bytes > 16 -> StandardPointer
        cross_origin_opener_policy: Some("same-origin-allow-popups".to_string()), // 24 bytes > 16 -> StandardPointer
        cross_origin_embedder_policy: Some("credentialless".to_string()), // 14 bytes <= 16 -> InlineRegister
        cross_origin_resource_policy: Some("cross-origin".to_string()), // 12 bytes <= 16 -> InlineRegister
        cross_domain_policies: Some("by-content-type".to_string()), // 15 bytes <= 16 -> InlineRegister
        xss_protection: Some("1; mode=block".to_string()), // 13 bytes <= 16 -> InlineRegister
    };

    let plugin = SecurityHeadersPlugin::with_config("sec-extreme-suite", extreme_cfg);
    let compiled = plugin.compiled_headers();
    assert_eq!(compiled.len(), 11);

    // Exact verification of dual representation partitioning:
    // <= 16 bytes: nosniff (7B), SAMEORIGIN (10B), credentialless (14B), cross-origin (12B), by-content-type (15B), 1; mode=block (13B) => 6
    // > 16 bytes: HSTS (47B), referrer (32B), CSP (258B), permissions (156B), COOP (24B) => 5
    let register_count = compiled
        .iter()
        .filter(|h| matches!(h, CompiledHeader::InlineRegister { .. }))
        .count();
    let pointer_count = compiled
        .iter()
        .filter(|h| matches!(h, CompiledHeader::StandardPointer { .. }))
        .count();

    assert_eq!(register_count, 6);
    assert_eq!(pointer_count, 5);

    let mut resp = L7Response::from_bytes(StatusCode::OK, "{\"status\":\"extreme\"}");
    let req_headers = HeaderWorkspace::default();
    plugin.on_response(&req_headers, &mut resp);
    resp.headers.render();

    assert_eq!(
        resp.headers.get("content-security-policy").unwrap(),
        heavy_csp.as_str()
    );
    assert_eq!(
        resp.headers.get("permissions-policy").unwrap(),
        heavy_permissions.as_str()
    );
    assert_eq!(resp.headers.get("x-frame-options").unwrap(), "SAMEORIGIN");
    assert_eq!(
        resp.headers.get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(
        resp.headers.get("cross-origin-embedder-policy").unwrap(),
        "credentialless"
    );
    assert_eq!(
        resp.headers.get("cross-origin-resource-policy").unwrap(),
        "cross-origin"
    );
    assert_eq!(
        resp.headers
            .get("x-permitted-cross-domain-policies")
            .unwrap(),
        "by-content-type"
    );
    assert_eq!(
        resp.headers.get("x-xss-protection").unwrap(),
        "1; mode=block"
    );
}
