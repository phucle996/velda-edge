//! Integration Tests for `response_header_mutation` Post-Response Plugin.

use http::header::HeaderName;
use http::{HeaderValue, StatusCode};
use velda_core::{HeaderWorkspace, L7Response};
use velda_plugin::config::PluginConfig;
use velda_plugin::post_response::PostResponsePlugin;
use velda_plugin::post_response::response_header_mutation::{
    ResponseHeaderMutationConfig, ResponseHeaderMutationPlugin, ResponseHeaderValueSpec,
};

#[test]
fn test_remove_sensitive_headers() {
    let mut config = ResponseHeaderMutationConfig::default();
    config.remove.push(HeaderName::from_static("x-powered-by"));
    config.remove.push(HeaderName::from_static("server"));

    let plugin = ResponseHeaderMutationPlugin::new("mut-1", config);

    let mut headers = HeaderWorkspace::empty(4);
    headers.insert("x-powered-by", HeaderValue::from_static("Express"));
    headers.insert("server", HeaderValue::from_static("Apache/2.4"));
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    let mut resp = L7Response::from_bytes(StatusCode::OK, "{}").with_headers(headers);
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

    assert!(!resp.headers.contains_key("x-powered-by"));
    assert!(!resp.headers.contains_key("server"));
    assert!(resp.headers.contains_key("content-type"));
}

#[test]
fn test_remove_with_simd_probe_optimization_and_over_32_fallback() {
    let mut config = ResponseHeaderMutationConfig::default();
    config.remove.push(HeaderName::from_static("x-backend-1"));
    config.remove.push(HeaderName::from_static("x-backend-2"));
    config.remove.push(HeaderName::from_static("x-backend-3"));
    config.remove.push(HeaderName::from_static("x-backend-4"));
    config.remove.push(HeaderName::from_static("x-backend-5"));
    config.remove.push(HeaderName::from_static("server"));

    let plugin = ResponseHeaderMutationPlugin::new("mut-simd-remove", config);

    let mut headers_a = HeaderWorkspace::new(4);
    headers_a.insert("content-type", HeaderValue::from_static("text/plain"));
    headers_a.insert("content-length", HeaderValue::from_static("12"));

    let mut resp_a = L7Response::from_bytes(StatusCode::OK, "hello").with_headers(headers_a);
    let req_headers = HeaderWorkspace::default();
    plugin.on_response(&req_headers, &mut resp_a);
    assert!(resp_a.headers.contains_key("content-type"));
    assert_eq!(resp_a.headers.len(), 3);

    let mut headers_b = HeaderWorkspace::empty(4);
    headers_b.insert("server", HeaderValue::from_static("nginx"));
    let mut resp_b = L7Response::from_bytes(StatusCode::OK, "hello").with_headers(headers_b);
    plugin.on_response(&req_headers, &mut resp_b);
    assert!(!resp_b.headers.contains_key("server"));

    // When >32 removal headers are configured, the 33rd+ header must still be removed
    let mut big_cfg = ResponseHeaderMutationConfig::default();
    for i in 0..33 {
        let name = HeaderName::from_bytes(format!("x-strip-hdr-{i:02}").as_bytes()).unwrap();
        big_cfg.remove.push(name);
    }
    let big_plugin = ResponseHeaderMutationPlugin::new("mut-over-32", big_cfg);
    let mut headers_c = HeaderWorkspace::empty(4);
    headers_c.insert("x-strip-hdr-32", HeaderValue::from_static("secret"));
    let mut resp_c = L7Response::from_bytes(StatusCode::OK, "ok").with_headers(headers_c);
    big_plugin.on_response(&req_headers, &mut resp_c);
    assert!(!resp_c.headers.contains_key("x-strip-hdr-32"));
}

#[test]
fn test_set_and_dynamic_variables() {
    let mut config = ResponseHeaderMutationConfig::default();
    config.set.push((
        HeaderName::from_static("server"),
        ResponseHeaderValueSpec::parse("velda-edge").unwrap(),
    ));
    config.set.push((
        HeaderName::from_static("x-request-id"),
        ResponseHeaderValueSpec::parse("$request_id").unwrap(),
    ));
    config.set.push((
        HeaderName::from_static("x-status-reflected"),
        ResponseHeaderValueSpec::parse("$status").unwrap(),
    ));
    config.set.push((
        HeaderName::from_static("x-timestamp"),
        ResponseHeaderValueSpec::parse("$timestamp_epoch").unwrap(),
    ));

    let plugin = ResponseHeaderMutationPlugin::new("mut-set", config);

    let headers = HeaderWorkspace::empty(4);
    let mut resp = L7Response::from_bytes(StatusCode::CREATED, "{}").with_headers(headers);
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

    assert_eq!(
        resp.headers.get_hex128("x-request-id"),
        req_headers.get_hex128("x-request-id")
    );
    assert_eq!(resp.headers.get_u64("x-status-reflected"), Some(201));
    assert_eq!(
        resp.headers.get_u64("x-timestamp"),
        Some(req_headers.ingress_time_sec())
    );
    assert_eq!(resp.headers.get("server").unwrap(), "velda-edge");
    assert_eq!(
        resp.headers.get("x-request-id").unwrap(),
        req_headers.request_id()
    );
    assert_eq!(resp.headers.get("x-status-reflected").unwrap(), "201");
    assert!(resp.headers.contains_key("x-timestamp"));
}

#[test]
fn test_set_if_missing() {
    let mut config = ResponseHeaderMutationConfig::default();
    config.set_if_missing.push((
        HeaderName::from_static("x-content-type-options"),
        ResponseHeaderValueSpec::parse("nosniff").unwrap(),
    ));
    config.set_if_missing.push((
        HeaderName::from_static("x-existing"),
        ResponseHeaderValueSpec::parse("new-val").unwrap(),
    ));

    let plugin = ResponseHeaderMutationPlugin::new("mut-sim", config);

    let mut headers = HeaderWorkspace::empty(4);
    headers.insert("x-existing", HeaderValue::from_static("original-val"));

    let mut resp = L7Response::from_bytes(StatusCode::OK, "{}").with_headers(headers);
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

    assert_eq!(
        resp.headers.get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(resp.headers.get("x-existing").unwrap(), "original-val");
}

#[test]
fn test_add_multi_value_headers() {
    let mut config = ResponseHeaderMutationConfig::default();
    config.add.push((
        HeaderName::from_static("vary"),
        ResponseHeaderValueSpec::parse("User-Agent").unwrap(),
    ));

    let plugin = ResponseHeaderMutationPlugin::new("mut-add", config);

    let mut headers = HeaderWorkspace::empty(4);
    headers.insert("vary", HeaderValue::from_static("Accept-Encoding"));

    let mut resp = L7Response::from_bytes(StatusCode::OK, "{}").with_headers(headers);
    let req_headers = HeaderWorkspace::default();

    plugin.on_response(&req_headers, &mut resp);

    let vary_values: Vec<&str> = resp
        .headers
        .get_all("vary")
        .iter()
        .map(|v| v.to_str().unwrap())
        .collect();

    assert_eq!(vary_values, vec!["Accept-Encoding", "User-Agent"]);
}

#[test]
fn test_from_config_strict_validation() {
    let valid_json = serde_json::json!({
        "remove_headers": ["server"],
        "set_headers": {
            "server": "velda-edge",
            "x-served-by": "edge-node"
        },
        "append_headers": {
            "vary": "accept-encoding"
        },
        "set_if_missing": {
            "x-content-type-options": "nosniff"
        }
    });

    let p_cfg = PluginConfig {
        id: "res_mut".into(),
        config: valid_json,
    };

    let plugin = ResponseHeaderMutationPlugin::from_config(&p_cfg).unwrap();
    assert_eq!(plugin.config.remove.len(), 1);
    assert_eq!(plugin.config.set.len(), 2);
    assert_eq!(plugin.config.add.len(), 1);
    assert_eq!(plugin.config.set_if_missing.len(), 1);

    let invalid_json = serde_json::json!({
        "remove_headers": ["server"]
    });
    let bad_cfg = PluginConfig {
        id: "res_mut_bad".into(),
        config: invalid_json,
    };
    assert!(ResponseHeaderMutationPlugin::from_config(&bad_cfg).is_err());
}
