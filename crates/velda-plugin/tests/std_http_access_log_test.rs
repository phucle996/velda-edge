//! Comprehensive Integration Test Suite for Standard Stream HTTP Access Log Plugin (`std_http_access_log`).

use std::sync::Arc;

use http::{HeaderMap, HeaderValue, StatusCode, Version};
use serde_json::json;

use velda_core::{HeaderWorkspace, L7Response};
use velda_plugin::post_response::std_http_access_log::{
    LogFormat, PackedLogRecord, SamplingTable, StdHttpAccessLogConfig, StdHttpAccessLogPlugin,
    TargetStream, copy_record,
};
use velda_plugin::post_response::{PostResponsePlugin, run_post_response_plugins};
use velda_sync::post_sync::plugin::PluginConfig;

#[test]
fn test_status_aware_sampling_exact_codes_only() {
    let mut sampling = SamplingTable::empty();
    sampling.set_rate(200, 0.05); // 5% sample for 200
    sampling.set_rate(429, 1.0); // 100% sample for rate limit
    sampling.set_rate(502, 1.0); // 100% sample for bad gateway

    // Unconfigured status codes MUST be 0.0 (never sampled)
    assert!(!sampling.should_sample(201));
    assert!(!sampling.should_sample(404));
    assert!(!sampling.should_sample(500));
    assert!(!sampling.should_sample(503));

    // 100% status codes must always sample
    for _ in 0..100 {
        assert!(sampling.should_sample(429));
        assert!(sampling.should_sample(502));
    }

    // 5% sampling test over 10,000 trials
    let mut hits_200 = 0;
    for _ in 0..10_000 {
        if sampling.should_sample(200) {
            hits_200 += 1;
        }
    }
    // 5% of 10,000 is ~500 hits (+/- 150 tolerance)
    assert!(
        (350..=650).contains(&hits_200),
        "Expected ~500 hits for 5% sampling on 200, got {hits_200}"
    );
}

#[test]
fn test_simd_record_register_harmony() {
    assert_eq!(std::mem::size_of::<PackedLogRecord>(), 256);
    assert_eq!(std::mem::align_of::<PackedLogRecord>(), 64);

    let mut record = PackedLogRecord::empty();
    record.timestamp_ms = 1710000000000;
    record.status = 200;
    record.content_length = 8192;
    record.duration_us = 15200;
    record.version = 3; // HTTP/2
    record.method = 1; // GET
    record.set_request_id(b"req-uuid-99999");
    record.set_route_id(b"checkout");
    record.set_upstream(b"10.0.2.15:8443");

    // Real-world Chrome 124 User-Agent string (~140 bytes)
    let chrome_ua = b"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
    record.set_user_agent(chrome_ua);

    let mut copied = PackedLogRecord::empty();
    copy_record(&mut copied, &record);

    assert_eq!(copied, record);
    assert_eq!(copied.request_id(), "req-uuid-99999");
    assert_eq!(copied.route_id(), "checkout");
    assert_eq!(copied.upstream(), "10.0.2.15:8443");
    assert_eq!(copied.user_agent(), std::str::from_utf8(chrome_ua).unwrap());
    assert_eq!(copied.latency_ms(), 15.2);
}

#[test]
fn test_long_browser_user_agent_support() {
    let mut record = PackedLogRecord::empty();

    // Extensive macOS Safari 17 User-Agent string (154 bytes)
    let safari_ua = b"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15 Edg/123.0.2420.81";
    record.set_user_agent(safari_ua);

    assert_eq!(record.user_agent_len as usize, safari_ua.len());
    assert_eq!(record.user_agent(), std::str::from_utf8(safari_ua).unwrap());

    let mut buf = Vec::with_capacity(512);
    record.write_json(&mut buf);

    let json = std::str::from_utf8(&buf).unwrap();
    assert!(json.contains("Edg/123.0.2420.81"));
}

#[test]
fn test_post_response_pipeline_execution() {
    let mut sampling = SamplingTable::empty();
    sampling.set_rate(200, 1.0); // 100% sample for 200

    let config = StdHttpAccessLogConfig {
        target: TargetStream::Stdout,
        format: LogFormat::Json,
        include_user_agent: true,
        include_request_id: true,
        include_route_id: true,
        include_upstream: true,
        include_latency: true,
        buffer_capacity: 64,
        sampling,
    };

    let plugin = Arc::new(StdHttpAccessLogPlugin::new_for_test(
        "std_http_access_log",
        config,
    ));
    let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin.clone()];

    let mut headers = HeaderMap::new();
    headers.insert("x-route-id", HeaderValue::from_static("api-v1"));
    headers.insert(
        "x-upstream-addr",
        HeaderValue::from_static("10.0.1.50:8080"),
    );
    headers.insert("x-response-time", HeaderValue::from_static("8.75ms"));
    headers.insert("content-length", HeaderValue::from_static("1024"));

    let mut resp = L7Response::new(
        StatusCode::OK,
        Version::HTTP_2,
        headers,
        velda_core::Body::Bytes(bytes::Bytes::from_static(b"OK")),
    );

    let mut req_headers = HeaderWorkspace::default();
    req_headers.insert("x-request-id", HeaderValue::from_static("trace-001"));
    req_headers.insert("user-agent", HeaderValue::from_static("curl/8.6.0"));
    req_headers.insert("x-forwarded-method", HeaderValue::from_static("GET"));

    run_post_response_plugins(&chain, &req_headers, &mut resp);

    assert_eq!(plugin.logged_count(), 1);
    assert_eq!(plugin.dropped_count(), 0);

    let mut out = Vec::new();
    let drained = plugin.drain_to_buffer(&mut out);
    assert_eq!(drained, 1);

    let log_line = std::str::from_utf8(&out).unwrap();
    assert!(log_line.contains("\"status\":200"));
    assert!(log_line.contains("\"method\":\"GET\""));
    assert!(log_line.contains("\"latency_us\":8750"));
    assert!(log_line.contains("\"request_id\":\"trace-001\""));
    assert!(log_line.contains("\"route_id\":\"api-v1\""));
    assert!(log_line.contains("\"upstream\":\"10.0.1.50:8080\""));
    assert!(log_line.contains("\"user_agent\":\"curl/8.6.0\""));
}

#[test]
fn test_unsampled_status_invisible_on_hot_path() {
    let mut sampling = SamplingTable::empty();
    sampling.set_rate(500, 1.0); // Only 500 is sampled

    let config = StdHttpAccessLogConfig {
        sampling,
        buffer_capacity: 64,
        ..Default::default()
    };

    let plugin = Arc::new(StdHttpAccessLogPlugin::new_for_test(
        "std_http_access_log",
        config,
    ));
    let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin.clone()];

    let mut resp = L7Response::empty(StatusCode::OK); // 200 is unconfigured
    let req_headers = HeaderWorkspace::default();

    run_post_response_plugins(&chain, &req_headers, &mut resp);

    // Completely unlogged, 0 allocations, 0 ring buffer interactions
    assert_eq!(plugin.logged_count(), 0);
    assert_eq!(plugin.dropped_count(), 0);

    let mut out = Vec::new();
    assert_eq!(plugin.drain_to_buffer(&mut out), 0);
}

#[test]
fn test_ring_buffer_saturation_drops_fail_open() {
    let mut sampling = SamplingTable::empty();
    sampling.set_rate(200, 1.0);

    let config = StdHttpAccessLogConfig {
        sampling,
        buffer_capacity: 64,
        ..Default::default()
    };

    let plugin = StdHttpAccessLogPlugin::new_for_test("std_http_access_log", config);
    let mut resp = L7Response::empty(StatusCode::OK);
    let req_headers = HeaderWorkspace::default();

    // Push 200 records to saturate the 64-capacity per-shard ring buffer
    for _ in 0..200 {
        plugin.on_response(&req_headers, &mut resp);
    }

    assert!(plugin.logged_count() > 0);
    assert!(plugin.dropped_count() > 0);
    assert_eq!(plugin.logged_count() + plugin.dropped_count(), 200);
}

#[test]
fn test_plugin_instantiate_from_config_success() {
    let valid_json = json!({
        "target": "stdout",
        "format": "clf",
        "include_user_agent": true,
        "include_request_id": true,
        "include_route_id": true,
        "include_upstream": true,
        "include_latency": true,
        "buffer_capacity": 4096,
        "sampling": {
            "200": 0.1,
            "404": 0.5,
            "502": 1.0
        }
    });

    let p_cfg = PluginConfig {
        id: "std_http_access_log".into(),
        phase: "post_response".into(),
        config: valid_json,
    };

    let plugin = velda_plugin::post_response::instantiate(&p_cfg)
        .expect("Must instantiate valid std_http_access_log");
    assert_eq!(plugin.id(), "std_http_access_log");
}

#[test]
fn test_plugin_instantiate_rejects_file_target() {
    let bad_json = json!({
        "target": "file", // Forbidden: direct file logging is rejected
        "format": "json",
        "buffer_capacity": 1024,
        "include_request_id": true,
        "include_route_id": true,
        "include_upstream": true,
        "include_latency": true
    });

    let p_cfg = PluginConfig {
        id: "std_http_access_log".into(),
        phase: "post_response".into(),
        config: bad_json,
    };

    assert!(velda_plugin::post_response::instantiate(&p_cfg).is_err());
}

#[test]
fn test_plugin_instantiate_rejects_wildcard_status() {
    let bad_json = json!({
        "target": "stdout",
        "format": "json",
        "buffer_capacity": 1024,
        "include_request_id": true,
        "include_route_id": true,
        "include_upstream": true,
        "include_latency": true,
        "sampling": {
            "2xx": 0.1 // Wildcard rejected
        }
    });

    let p_cfg = PluginConfig {
        id: "std_http_access_log".into(),
        phase: "post_response".into(),
        config: bad_json,
    };

    assert!(velda_plugin::post_response::instantiate(&p_cfg).is_err());
}
