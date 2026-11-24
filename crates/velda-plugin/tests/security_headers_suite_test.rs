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
use velda_plugin::PluginConfig;
use velda_plugin::post_response::PostResponsePlugin;
use velda_plugin::post_response::security_headers::validation::validate_security_headers_config;
use velda_plugin::post_response::security_headers::{
    CompiledHeader, FLAG_COEP, FLAG_COOP, FLAG_CORP, FLAG_CROSS_DOMAIN, FLAG_CSP,
    FLAG_FRAME_OPTIONS, FLAG_HSTS, FLAG_NOSNIFF, FLAG_PERMISSIONS_POLICY, FLAG_REFERRER_POLICY,
    FLAG_XSS_PROTECTION, HstsConfig, SecurityHeadersConfig, SecurityHeadersPlugin,
};

#[test]
fn test_suite_default_owasp_hardening() {
    let plugin = SecurityHeadersPlugin::new("sec-default-suite");
    let mut resp = L7Response::from_bytes(StatusCode::OK, "{\"status\":\"healthy\"}");
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

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
        config: manifest_json,
    };

    let plugin = SecurityHeadersPlugin::from_config(&plugin_cfg).expect("valid configuration");
    assert_eq!(plugin.id(), "edge_sec_enterprise");

    let mut resp = L7Response::from_bytes(StatusCode::OK, "{\"ok\":true}");
    let req_headers = HeaderWorkspace::default();
    plugin.on_response(&req_headers, &mut resp);

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

#[test]
fn test_default_security_headers_injection() {
    let plugin = SecurityHeadersPlugin::new("sec-default");
    let mut resp = L7Response::from_bytes(StatusCode::OK, "{\"status\":\"ok\"}");
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

    assert!(resp.headers.contains_key("x-content-type-options"));
    assert!(resp.headers.contains_key("x-frame-options"));

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
}

#[test]
fn test_cacheline_alignment_and_bitmask() {
    assert_eq!(
        std::mem::align_of::<SecurityHeadersPlugin>(),
        64,
        "SecurityHeadersPlugin must be 64-byte L1 cacheline aligned"
    );

    let plugin = SecurityHeadersPlugin::new("sec-align");
    assert!(plugin.op_mask & FLAG_HSTS != 0);
    assert!(plugin.op_mask & FLAG_NOSNIFF != 0);
    assert!(plugin.op_mask & FLAG_FRAME_OPTIONS != 0);
    assert!(plugin.op_mask & FLAG_REFERRER_POLICY != 0);
}

#[test]
fn test_custom_hsts_preload_and_csp() {
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
        csp: Some("default-src 'self'; script-src 'self' cdn.example.com".to_string()),
        permissions_policy: Some("camera=(), microphone=()".to_string()),
        cross_origin_opener_policy: None,
        cross_origin_embedder_policy: None,
        cross_origin_resource_policy: None,
        cross_domain_policies: None,
        xss_protection: None,
    };

    let plugin = SecurityHeadersPlugin::with_config("sec-custom", config);
    let mut resp = L7Response::from_bytes(StatusCode::OK, "");
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

    assert_eq!(
        resp.headers.get("strict-transport-security").unwrap(),
        "max-age=63072000; includeSubDomains; preload"
    );
    assert_eq!(resp.headers.get("x-frame-options").unwrap(), "SAMEORIGIN");
    assert_eq!(resp.headers.get("referrer-policy").unwrap(), "no-referrer");
    assert_eq!(
        resp.headers.get("content-security-policy").unwrap(),
        "default-src 'self'; script-src 'self' cdn.example.com"
    );
    assert_eq!(
        resp.headers.get("permissions-policy").unwrap(),
        "camera=(), microphone=()"
    );
}

#[test]
fn test_modern_coop_coep_corp_injection() {
    let config = SecurityHeadersConfig {
        hsts: HstsConfig::default(),
        frame_options: Some("DENY".to_string()),
        content_type_nosniff: true,
        referrer_policy: Some("strict-origin-when-cross-origin".to_string()),
        csp: None,
        permissions_policy: None,
        cross_origin_opener_policy: Some("same-origin".to_string()),
        cross_origin_embedder_policy: Some("require-corp".to_string()),
        cross_origin_resource_policy: Some("same-site".to_string()),
        cross_domain_policies: Some("none".to_string()),
        xss_protection: Some("0".to_string()),
    };

    let plugin = SecurityHeadersPlugin::with_config("sec-modern", config);
    let mut resp = L7Response::from_bytes(StatusCode::OK, "");
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

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
        "same-site"
    );
    assert_eq!(
        resp.headers
            .get("x-permitted-cross-domain-policies")
            .unwrap(),
        "none"
    );
    assert_eq!(resp.headers.get("x-xss-protection").unwrap(), "0");
}

#[test]
fn test_selective_disabling() {
    let config = SecurityHeadersConfig {
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

    let plugin = SecurityHeadersPlugin::with_config("sec-disabled", config);
    let mut resp = L7Response::from_bytes(StatusCode::OK, "");
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

    assert_eq!(plugin.compiled_headers().len(), 0);
    assert_eq!(plugin.op_mask, 0);
    assert!(!resp.headers.contains_key("strict-transport-security"));
    assert!(!resp.headers.contains_key("x-content-type-options"));
    assert!(!resp.headers.contains_key("x-frame-options"));
    assert!(!resp.headers.contains_key("referrer-policy"));
}

#[test]
fn test_explicit_none_values_are_omitted() {
    let config = SecurityHeadersConfig {
        hsts: HstsConfig::default(),
        frame_options: Some("none".to_string()),
        content_type_nosniff: true,
        referrer_policy: Some("none".to_string()),
        csp: Some("none".to_string()),
        permissions_policy: Some("none".to_string()),
        cross_origin_opener_policy: Some("none".to_string()),
        cross_origin_embedder_policy: Some("none".to_string()),
        cross_origin_resource_policy: Some("none".to_string()),
        cross_domain_policies: None,
        xss_protection: Some("none".to_string()),
    };

    let plugin = SecurityHeadersPlugin::with_config("sec-none", config);
    let mut resp = L7Response::from_bytes(StatusCode::OK, "");
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

    assert!(resp.headers.contains_key("strict-transport-security"));
    assert!(resp.headers.contains_key("x-content-type-options"));
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
fn test_from_config_json_deserialization() {
    let raw_json = json!({
        "hsts": {
            "enable": true,
            "max_age_secs": 15768000,
            "include_subdomains": false,
            "preload": false
        },
        "frame_options": "DENY",
        "content_type_nosniff": true,
        "referrer_policy": "origin",
        "content_security_policy": "default-src https:",
        "permissions_policy": "geolocation=()",
        "cross_origin_opener_policy": "same-origin",
        "cross_origin_embedder_policy": "credentialless",
        "cross_origin_resource_policy": "cross-origin",
        "cross_domain_policies": "none",
        "xss_protection": "0"
    });

    let p_cfg = PluginConfig {
        id: "my-sec-plugin".to_string(),
        config: raw_json,
    };

    let plugin =
        SecurityHeadersPlugin::from_config(&p_cfg).expect("deserialization should succeed");
    assert_eq!(plugin.id, "my-sec-plugin");

    let mut resp = L7Response::from_bytes(StatusCode::OK, "");
    let req_headers = HeaderWorkspace::default();
    plugin.on_response(&req_headers, &mut resp);

    assert_eq!(
        resp.headers.get("strict-transport-security").unwrap(),
        "max-age=15768000"
    );
    assert_eq!(resp.headers.get("x-frame-options").unwrap(), "DENY");
    assert_eq!(
        resp.headers.get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(resp.headers.get("referrer-policy").unwrap(), "origin");
    assert_eq!(
        resp.headers.get("content-security-policy").unwrap(),
        "default-src https:"
    );
    assert_eq!(
        resp.headers.get("permissions-policy").unwrap(),
        "geolocation=()"
    );
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
        "none"
    );
    assert_eq!(resp.headers.get("x-xss-protection").unwrap(), "0");
}

#[test]
fn test_valid_explicit_security_headers() {
    let val = json!({
        "hsts": {
            "enable": true,
            "max_age_secs": 31536000,
            "include_subdomains": true,
            "preload": false
        },
        "frame_options": "DENY",
        "content_type_nosniff": true,
        "referrer_policy": "strict-origin-when-cross-origin",
        "content_security_policy": null,
        "permissions_policy": null,
        "cross_origin_opener_policy": "same-origin",
        "cross_origin_embedder_policy": "require-corp",
        "cross_origin_resource_policy": "same-origin",
        "cross_domain_policies": "none",
        "xss_protection": "0"
    });
    let res = validate_security_headers_config("sec_1", &val);
    assert!(res.is_ok());
}

#[test]
fn test_missing_modern_header_key_rejected() {
    let val = json!({
        "hsts": {
            "enable": true,
            "max_age_secs": 31536000,
            "include_subdomains": true,
            "preload": false
        },
        "frame_options": "DENY",
        "content_type_nosniff": true,
        "referrer_policy": "strict-origin-when-cross-origin",
        "content_security_policy": null,
        "permissions_policy": null,
        "cross_origin_embedder_policy": "require-corp",
        "cross_origin_resource_policy": "same-origin",
        "cross_domain_policies": "none",
        "xss_protection": "0"
    });
    let res = validate_security_headers_config("sec_missing_coop", &val);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("missing required explicit configuration key 'cross_origin_opener_policy'")
    );
}

#[test]
fn test_hsts_enabled_with_zero_max_age_rejected() {
    let val = json!({
        "hsts": {
            "enable": true,
            "max_age_secs": 0,
            "include_subdomains": false,
            "preload": false
        },
        "frame_options": "DENY",
        "content_type_nosniff": true,
        "referrer_policy": "no-referrer",
        "content_security_policy": null,
        "permissions_policy": null,
        "cross_origin_opener_policy": null,
        "cross_origin_embedder_policy": null,
        "cross_origin_resource_policy": null,
        "cross_domain_policies": null,
        "xss_protection": null
    });
    let res = validate_security_headers_config("sec_bad_hsts", &val);
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("max_age_secs is 0"));
}

#[test]
fn test_invalid_frame_options_rejected() {
    let val = json!({
        "hsts": {
            "enable": false,
            "max_age_secs": 0,
            "include_subdomains": false,
            "preload": false
        },
        "frame_options": "INVALID_FRAME_VALUE",
        "content_type_nosniff": true,
        "referrer_policy": "no-referrer",
        "content_security_policy": null,
        "permissions_policy": null,
        "cross_origin_opener_policy": null,
        "cross_origin_embedder_policy": null,
        "cross_origin_resource_policy": null,
        "cross_domain_policies": null,
        "xss_protection": null
    });
    let res = validate_security_headers_config("sec_bad_frame", &val);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("invalid frame_options")
    );
}

#[test]
fn test_invalid_coop_rejected() {
    let val = json!({
        "hsts": {
            "enable": false,
            "max_age_secs": 0,
            "include_subdomains": false,
            "preload": false
        },
        "frame_options": "DENY",
        "content_type_nosniff": true,
        "referrer_policy": "no-referrer",
        "content_security_policy": null,
        "permissions_policy": null,
        "cross_origin_opener_policy": "invalid-coop",
        "cross_origin_embedder_policy": null,
        "cross_origin_resource_policy": null,
        "cross_domain_policies": null,
        "xss_protection": null
    });
    let res = validate_security_headers_config("sec_bad_coop", &val);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("invalid cross_origin_opener_policy")
    );
}

#[test]
fn test_missing_required_fields_rejected() {
    let val = json!({
        "frame_options": "DENY"
    });
    let res = validate_security_headers_config("sec_missing", &val);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("missing required explicit configuration key")
    );
}
