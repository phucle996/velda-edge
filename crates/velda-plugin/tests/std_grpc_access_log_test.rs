//! Comprehensive Integration Test Suite for Standard Stream gRPC Access Log Plugin (`std_grpc_access_log`).

use std::sync::Arc;

use http::{HeaderMap, HeaderValue, StatusCode, Version};
use serde_json::json;

use velda_core::{HeaderWorkspace, L7Response};
use velda_plugin::post_response::std_grpc_access_log::{
    GrpcLogFormat, GrpcSamplingTable, PackedGrpcRecord, StdGrpcAccessLogConfig,
    StdGrpcAccessLogPlugin, TargetStream, copy_record,
};
use velda_plugin::post_response::{PostResponsePlugin, run_post_response_plugins};
use velda_sync::post_sync::plugin::PluginConfig;

#[test]
fn test_grpc_status_aware_sampling_exact_codes_and_names() {
    let mut sampling = GrpcSamplingTable::empty();
    sampling.set_rate(0, 0.05); // 5% sample for OK (0)
    sampling.set_rate(14, 1.0); // 100% sample for UNAVAILABLE (14)
    sampling.set_rate(4, 1.0); // 100% sample for DEADLINE_EXCEEDED (4)

    // Unconfigured status codes MUST be 0.0 (never sampled)
    assert!(!sampling.should_sample(1)); // Cancelled
    assert!(!sampling.should_sample(3)); // InvalidArgument
    assert!(!sampling.should_sample(5)); // NotFound
    assert!(!sampling.should_sample(16)); // Unauthenticated

    // 100% status codes must always sample
    for _ in 0..100 {
        assert!(sampling.should_sample(14));
        assert!(sampling.should_sample(4));
    }

    // 5% sampling test over 10,000 trials
    let mut hits_0 = 0;
    for _ in 0..10_000 {
        if sampling.should_sample(0) {
            hits_0 += 1;
        }
    }
    assert!(
        (350..=650).contains(&hits_0),
        "Expected ~500 hits for 5% sampling on gRPC status 0, got {hits_0}"
    );
}

#[test]
fn test_grpc_simd_record_register_harmony() {
    assert_eq!(std::mem::size_of::<PackedGrpcRecord>(), 256);
    assert_eq!(std::mem::align_of::<PackedGrpcRecord>(), 64);

    let mut record = PackedGrpcRecord::empty();
    record.timestamp_ms = 1710000000000;
    record.duration_us = 3500;
    record.grpc_status = 0;
    record.transport = 1; // TCP
    record.messages_sent = 120;
    record.messages_received = 1;
    record.content_length = 32768;
    record.set_rpc_path("/order.OrderService/StreamOrders");
    record.set_request_id(b"req-grpc-harmony");
    record.set_route_id(b"rt-grpc");
    record.set_upstream(b"10.0.1.5:50051");
    record.set_peer_addr(b"127.0.0.1:45678");

    let mut dst = PackedGrpcRecord::empty();
    copy_record(&mut dst, &record);
    assert_eq!(record, dst);
}

#[test]
fn test_grpc_post_response_pipeline_execution() {
    let mut sampling = GrpcSamplingTable::empty();
    sampling.set_rate(0, 1.0); // 100% on OK

    let config = StdGrpcAccessLogConfig {
        target: TargetStream::Stdout,
        format: GrpcLogFormat::Json,
        include_request_id: true,
        include_route_id: true,
        include_upstream: true,
        include_peer_addr: true,
        include_latency: true,
        buffer_capacity: 64,
        sampling,
    };

    let plugin = Arc::new(StdGrpcAccessLogPlugin::new_for_test(
        "std_grpc_access_log",
        config,
    ));
    let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin.clone()];

    let mut headers = HeaderMap::new();
    headers.insert("grpc-status", HeaderValue::from_static("0"));
    headers.insert("x-route-id", HeaderValue::from_static("rpc-stream"));
    headers.insert(
        "x-upstream-addr",
        HeaderValue::from_static("10.0.2.10:50051"),
    );
    headers.insert("x-response-time", HeaderValue::from_static("12.4ms"));
    headers.insert("x-grpc-messages-sent", HeaderValue::from_static("42"));
    headers.insert("content-length", HeaderValue::from_static("8192"));

    let mut resp = L7Response::new(
        StatusCode::OK,
        Version::HTTP_2,
        headers,
        velda_core::Body::Bytes(bytes::Bytes::from_static(b"response-data")),
    );

    let mut req_headers = HeaderWorkspace::default();
    req_headers.insert("x-request-id", HeaderValue::from_static("trace-grpc-01"));
    req_headers.insert(
        "x-forwarded-path",
        HeaderValue::from_static("/telemetry.Metrics/StreamBatch"),
    );
    req_headers.insert("x-real-ip", HeaderValue::from_static("10.200.1.4"));
    req_headers.insert("x-grpc-messages-recv", HeaderValue::from_static("5"));

    run_post_response_plugins(&chain, &req_headers, &mut resp);

    assert_eq!(plugin.logged_count(), 1);
    assert_eq!(plugin.dropped_count(), 0);

    let mut out = Vec::new();
    let drained = plugin.drain_to_buffer(&mut out);
    assert_eq!(drained, 1);

    let log_line = std::str::from_utf8(&out).unwrap();
    assert!(log_line.contains("\"grpc_status\":0"));
    assert!(log_line.contains("\"grpc_status_name\":\"OK\""));
    assert!(log_line.contains("\"service\":\"telemetry.Metrics\""));
    assert!(log_line.contains("\"method\":\"StreamBatch\""));
    assert!(log_line.contains("\"messages_sent\":42"));
    assert!(log_line.contains("\"messages_received\":5"));
    assert!(log_line.contains("\"bytes\":8192"));
    assert!(
        log_line.contains("\"latency_us\":12400") || log_line.contains("\"duration_us\":12400")
    );
    assert!(log_line.contains("\"request_id\":\"trace-grpc-01\""));
    assert!(log_line.contains("\"route_id\":\"rpc-stream\""));
    assert!(log_line.contains("\"upstream\":\"10.0.2.10:50051\""));
    assert!(log_line.contains("\"peer\":\"10.200.1.4\""));
}

#[test]
fn test_grpc_unsampled_status_invisible_on_hot_path() {
    let mut sampling = GrpcSamplingTable::empty();
    sampling.set_rate(14, 1.0); // Only UNAVAILABLE is sampled

    let config = StdGrpcAccessLogConfig {
        sampling,
        buffer_capacity: 64,
        ..Default::default()
    };

    let plugin = Arc::new(StdGrpcAccessLogPlugin::new_for_test(
        "std_grpc_access_log",
        config,
    ));
    let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin.clone()];

    let mut resp = L7Response::empty(StatusCode::OK); // 0 (OK) is unconfigured
    let req_headers = HeaderWorkspace::default();

    run_post_response_plugins(&chain, &req_headers, &mut resp);

    assert_eq!(plugin.logged_count(), 0);
    assert_eq!(plugin.dropped_count(), 0);

    let mut out = Vec::new();
    assert_eq!(plugin.drain_to_buffer(&mut out), 0);
}

#[test]
fn test_grpc_ring_buffer_saturation_drops_fail_open() {
    let mut sampling = GrpcSamplingTable::empty();
    sampling.set_rate(0, 1.0);

    let config = StdGrpcAccessLogConfig {
        sampling,
        buffer_capacity: 64,
        ..Default::default()
    };

    let plugin = StdGrpcAccessLogPlugin::new_for_test("std_grpc_access_log", config);
    let mut resp = L7Response::empty(StatusCode::OK);
    let req_headers = HeaderWorkspace::default();

    for _ in 0..200 {
        plugin.on_response(&req_headers, &mut resp);
    }

    assert!(plugin.logged_count() > 0);
    assert!(plugin.dropped_count() > 0);
    assert_eq!(plugin.logged_count() + plugin.dropped_count(), 200);
}

#[test]
fn test_grpc_plugin_instantiate_from_config_success() {
    let valid_json = json!({
        "target": "stdout",
        "format": "json",
        "include_request_id": true,
        "include_route_id": true,
        "include_upstream": true,
        "include_peer_addr": true,
        "include_latency": true,
        "buffer_capacity": 4096,
        "sampling": {
            "0": 0.1,
            "14": 1.0,
            "UNAVAILABLE": 1.0
        }
    });

    let p_cfg = PluginConfig {
        id: "std_grpc_access_log".into(),
        phase: "post_response".into(),
        config: valid_json,
    };

    let plugin = velda_plugin::post_response::instantiate(&p_cfg)
        .expect("Must instantiate valid std_grpc_access_log");
    assert_eq!(plugin.id(), "std_grpc_access_log");
}

#[test]
fn test_grpc_plugin_instantiate_rejects_file_target() {
    let bad_json = json!({
        "target": "file", // Forbidden: direct file logging is rejected
        "format": "json",
        "buffer_capacity": 1024
    });

    let p_cfg = PluginConfig {
        id: "std_grpc_access_log".into(),
        phase: "post_response".into(),
        config: bad_json,
    };

    assert!(velda_plugin::post_response::instantiate(&p_cfg).is_err());
}

#[test]
fn test_grpc_plugin_instantiate_rejects_wildcard_status() {
    let bad_json = json!({
        "target": "stdout",
        "format": "json",
        "buffer_capacity": 1024,
        "sampling": {
            "*": 0.1 // Wildcard rejected
        }
    });

    let p_cfg = PluginConfig {
        id: "std_grpc_access_log".into(),
        phase: "post_response".into(),
        config: bad_json,
    };

    assert!(velda_plugin::post_response::instantiate(&p_cfg).is_err());
}
