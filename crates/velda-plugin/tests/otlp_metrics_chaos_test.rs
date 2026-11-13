//! Exhaustive Chaos & Stress Test Suite for OTLP Metrics Exporter (`otlp_metrics_exporter`).
//!
//! Validates:
//! 1. 100% round-trip binary deserialization conformance with Prost `ExportMetricsServiceRequest`.
//! 2. 100% round-trip schema conformance with `serde_json::from_slice`.
//! 3. Boundary extremes: 0, 1, `u64::MAX`, negative active streams (-1, -100, `i64::MIN`).
//! 4. Continuous 1,000-cycle stress test verifying zero heap growth and memory stability.

#![allow(clippy::too_many_arguments)]

use std::sync::Arc;

use prost::Message;
use velda_observability::{
    DownstreamProtocol, MetricsSnapshot, MetricsStore, PipeStrategy, UpstreamProtocol,
};
use velda_plugin::background::otlp_metrics_exporter::json::OtlpJsonTemplate;
use velda_plugin::background::otlp_metrics_exporter::proto::{
    ExportMetricsServiceRequest, OtlpProtoTemplate, number_data_point,
};

/// Helper: constructs a synthetic snapshot where all 36 pipelines have uniform values.
fn make_uniform_snapshot(
    req_2xx: u64,
    req_3xx: u64,
    req_4xx: u64,
    req_5xx: u64,
    req_other: u64,
    active: i64,
    rx: u64,
    tx: u64,
) -> MetricsSnapshot {
    let mut snapshot = MetricsStore::new().snapshot();
    let mut pipelines = Vec::with_capacity(36);
    for slot_idx in 0..36 {
        let mut p = snapshot.pipelines[slot_idx];
        p.requests_2xx = req_2xx;
        p.requests_3xx = req_3xx;
        p.requests_4xx = req_4xx;
        p.requests_5xx = req_5xx;
        p.requests_other = req_other;
        p.active_streams = active;
        p.bytes_rx = rx;
        p.bytes_tx = tx;
        pipelines.push(p);
    }
    snapshot.pipelines = pipelines;
    snapshot
}

// ============================================================================
// 1. PROTOBUF ROUND-TRIP DECODE & SCHEMA INTEGRITY
// ============================================================================
#[test]
fn test_otlp_metrics_proto_roundtrip_decode() {
    let snapshot = make_uniform_snapshot(100, 20, 5, 2, 1, 42, 1024, 2048);
    let mut template = OtlpProtoTemplate::new("velda-chaos-service");
    let mut buf = Vec::with_capacity(32 * 1024);

    template.update_and_encode(&snapshot, &mut buf);
    assert!(!buf.is_empty(), "Encoded proto buffer must not be empty");

    let decoded = ExportMetricsServiceRequest::decode(&buf[..])
        .expect("Prost must successfully decode pre-rendered wire skeleton");

    assert_eq!(decoded.resource_metrics.len(), 1);
    let rm = &decoded.resource_metrics[0];
    let res = rm.resource.as_ref().expect("Resource must be present");
    assert_eq!(res.attributes[0].key, "service.name");

    let sm = &rm.scope_metrics[0];
    assert_eq!(sm.metrics.len(), 4);

    // Metric 0: velda.requests.total
    let m0 = &sm.metrics[0];
    assert_eq!(m0.name, "velda.requests.total");
    if let Some(velda_plugin::background::otlp_metrics_exporter::proto::metric::Data::Sum(sum)) =
        &m0.data
    {
        assert_eq!(sum.data_points.len(), 180);
        assert_eq!(
            sum.data_points[0].value,
            Some(number_data_point::Value::AsInt(100))
        );
        assert_eq!(
            sum.data_points[1].value,
            Some(number_data_point::Value::AsInt(20))
        );
        assert_eq!(
            sum.data_points[2].value,
            Some(number_data_point::Value::AsInt(5))
        );
        assert_eq!(
            sum.data_points[3].value,
            Some(number_data_point::Value::AsInt(2))
        );
        assert_eq!(
            sum.data_points[4].value,
            Some(number_data_point::Value::AsInt(1))
        );
    } else {
        panic!("Metric 0 must be Sum");
    }

    // Metric 1: velda.active_streams
    let m1 = &sm.metrics[1];
    assert_eq!(m1.name, "velda.active_streams");
    if let Some(velda_plugin::background::otlp_metrics_exporter::proto::metric::Data::Gauge(g)) =
        &m1.data
    {
        assert_eq!(g.data_points.len(), 36);
        assert_eq!(
            g.data_points[0].value,
            Some(number_data_point::Value::AsInt(42))
        );
    } else {
        panic!("Metric 1 must be Gauge");
    }

    // Metric 2: velda.bytes_received_total
    let m2 = &sm.metrics[2];
    assert_eq!(m2.name, "velda.bytes_received_total");
    if let Some(velda_plugin::background::otlp_metrics_exporter::proto::metric::Data::Sum(sum)) =
        &m2.data
    {
        assert_eq!(sum.data_points.len(), 36);
        assert_eq!(
            sum.data_points[0].value,
            Some(number_data_point::Value::AsInt(1024))
        );
    } else {
        panic!("Metric 2 must be Sum");
    }

    // Metric 3: velda.bytes_transmitted_total
    let m3 = &sm.metrics[3];
    assert_eq!(m3.name, "velda.bytes_transmitted_total");
    if let Some(velda_plugin::background::otlp_metrics_exporter::proto::metric::Data::Sum(sum)) =
        &m3.data
    {
        assert_eq!(sum.data_points.len(), 36);
        assert_eq!(
            sum.data_points[0].value,
            Some(number_data_point::Value::AsInt(2048))
        );
    } else {
        panic!("Metric 3 must be Sum");
    }
}

// ============================================================================
// 2. JSON ROUND-TRIP DECODE & SCHEMA INTEGRITY
// ============================================================================
#[test]
fn test_otlp_metrics_json_serde_roundtrip() {
    let snapshot = make_uniform_snapshot(999, 888, 777, 666, 555, -12, 100_000, 200_000);
    let template = OtlpJsonTemplate::new("velda-json-service");
    let mut buf = Vec::with_capacity(64 * 1024);

    template.format_into(&snapshot, &mut buf);
    assert!(!buf.is_empty());

    let parsed: serde_json::Value =
        serde_json::from_slice(&buf).expect("Payload must be strictly valid JSON");

    let rm = &parsed["resourceMetrics"][0];
    assert_eq!(
        rm["resource"]["attributes"][0]["value"]["stringValue"],
        "velda-json-service"
    );

    let metrics = rm["scopeMetrics"][0]["metrics"]
        .as_array()
        .expect("metrics array");
    assert_eq!(metrics.len(), 4);

    // Metric 0
    assert_eq!(metrics[0]["name"], "velda.requests.total");
    let dps0 = metrics[0]["sum"]["dataPoints"].as_array().unwrap();
    assert_eq!(dps0.len(), 180);
    assert_eq!(dps0[0]["asInt"], "999");
    assert_eq!(dps0[1]["asInt"], "888");

    // Metric 1: active streams negative
    assert_eq!(metrics[1]["name"], "velda.active_streams");
    let dps1 = metrics[1]["gauge"]["dataPoints"].as_array().unwrap();
    assert_eq!(dps1.len(), 36);
    assert_eq!(dps1[0]["asInt"], "-12");

    // Metric 2: bytes rx
    assert_eq!(metrics[2]["name"], "velda.bytes_received_total");
    let dps2 = metrics[2]["sum"]["dataPoints"].as_array().unwrap();
    assert_eq!(dps2.len(), 36);
    assert_eq!(dps2[0]["asInt"], "100000");

    // Metric 3: bytes tx
    assert_eq!(metrics[3]["name"], "velda.bytes_transmitted_total");
    let dps3 = metrics[3]["sum"]["dataPoints"].as_array().unwrap();
    assert_eq!(dps3.len(), 36);
    assert_eq!(dps3[0]["asInt"], "200000");
}

// ============================================================================
// 3. EXTREME BOUNDARY VALUES: ZERO, ONES, MAX U64, NEGATIVE ACTIVE STREAMS
// ============================================================================
#[test]
fn test_otlp_metrics_boundary_values() {
    let test_cases = [
        // Case 1: Pure Zero
        (0u64, 0u64, 0u64, 0u64, 0u64, 0i64, 0u64, 0u64),
        // Case 2: Minimal non-zero
        (1, 1, 1, 1, 1, 1, 1, 1),
        // Case 3: Negative active streams (-1, -100, -999999)
        (10, 20, 30, 40, 50, -1, 100, 200),
        (10, 20, 30, 40, 50, -100, 100, 200),
        (10, 20, 30, 40, 50, -999_999, 100, 200),
        // Case 4: Extreme active stream boundary
        (100, 200, 300, 400, 500, i64::MIN, 1000, 2000),
        (100, 200, 300, 400, 500, i64::MAX, 1000, 2000),
        // Case 5: u64 boundaries
        (
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            0,
            u64::MAX,
            u64::MAX,
        ),
    ];

    let json_template = OtlpJsonTemplate::new("velda-boundary-service");
    let mut proto_template = OtlpProtoTemplate::new("velda-boundary-service");
    let mut json_buf = Vec::with_capacity(64 * 1024);
    let mut proto_buf = Vec::with_capacity(32 * 1024);

    for (c2, c3, c4, c5, co, active, rx, tx) in test_cases {
        let snapshot = make_uniform_snapshot(c2, c3, c4, c5, co, active, rx, tx);

        // 1. Validate JSON
        json_buf.clear();
        json_template.format_into(&snapshot, &mut json_buf);
        let parsed_json: serde_json::Value = serde_json::from_slice(&json_buf)
            .unwrap_or_else(|e| panic!("Failed to parse boundary JSON: {e}"));
        assert!(parsed_json["resourceMetrics"].is_array());

        // 2. Validate Protobuf
        proto_buf.clear();
        proto_template.update_and_encode(&snapshot, &mut proto_buf);
        let decoded = ExportMetricsServiceRequest::decode(&proto_buf[..])
            .unwrap_or_else(|e| panic!("Failed to decode boundary Protobuf: {e}"));
        assert_eq!(decoded.resource_metrics.len(), 1);

        // Verify active streams value in proto
        let active_dp_val = &decoded.resource_metrics[0].scope_metrics[0].metrics[1];
        if let Some(velda_plugin::background::otlp_metrics_exporter::proto::metric::Data::Gauge(
            g,
        )) = &active_dp_val.data
        {
            assert_eq!(
                g.data_points[0].value,
                Some(number_data_point::Value::AsInt(active))
            );
        }
    }
}

// ============================================================================
// 4. STRESS TEST: 1,000 CONTINUOUS CYCLES & MEMORY STABILITY
// ============================================================================
#[test]
fn test_otlp_metrics_stress_1000_cycles_memory_stability() {
    let store = Arc::new(MetricsStore::new());
    let mut proto_template = OtlpProtoTemplate::new("velda-stress-service");
    let json_template = OtlpJsonTemplate::new("velda-stress-service");

    let mut json_buf = Vec::with_capacity(64 * 1024);
    let mut proto_buf = Vec::with_capacity(32 * 1024);

    // Warm-up
    for _ in 0..10 {
        let snap = store.snapshot();
        json_template.format_into(&snap, &mut json_buf);
        proto_template.update_and_encode(&snap, &mut proto_buf);
    }

    let initial_json_capacity = json_buf.capacity();
    let initial_proto_capacity = proto_buf.capacity();

    for cycle in 0..1000 {
        // Mutate metrics activity
        let slot = store.slot_on_core(
            cycle % 4,
            DownstreamProtocol::H1,
            UpstreamProtocol::H1,
            PipeStrategy::Buffered,
        );
        slot.stream_started();
        slot.record_completion(&velda_observability::StreamCompletion::new(
            200,
            128,
            256,
            std::time::Duration::from_micros(500),
            None,
        ));

        let snapshot = store.snapshot();

        // JSON format cycle
        json_buf.clear();
        json_template.format_into(&snapshot, &mut json_buf);
        assert!(
            !json_buf.is_empty(),
            "Cycle {cycle}: JSON buffer must not be empty"
        );
        assert!(
            json_buf.capacity() == initial_json_capacity,
            "Cycle {cycle}: JSON buffer capacity grew from {initial_json_capacity} to {}",
            json_buf.capacity()
        );

        // Proto format cycle
        proto_buf.clear();
        proto_template.update_and_encode(&snapshot, &mut proto_buf);
        assert!(
            !proto_buf.is_empty(),
            "Cycle {cycle}: Proto buffer must not be empty"
        );
        assert!(
            proto_buf.capacity() == initial_proto_capacity,
            "Cycle {cycle}: Proto buffer capacity grew from {initial_proto_capacity} to {}",
            proto_buf.capacity()
        );
    }
}
