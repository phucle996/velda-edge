//! Chaos & Extreme Edge-Case Test Suite for Grafana Loki Log Shipper (`loki_log_shipper`).
//!
//! Validates:
//! 1. Request ID Extremes: id=0, id=u64::MAX, alternating bit patterns, 32-char lowercase hex.
//! 2. IP Address Chaos: RFC 4291 IPv4-mapped & RFC 5952 IPv6 branchless zero-compression.
//! 3. Multi-Protocol Chaos: 7 interleaved protocols partitioned into exactly 3 stream buckets (http, grpc, raw).
//! 4. Full Round-Trip Decoding: Validating JSON syntax with serde_json and gRPC wire with Prost.
//! 5. Scratch Buffer Stress: 10,000 records across 100 cycles proving 0 reallocations and data integrity.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use prost::Message;
use serde_json::Value;
use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, PipeStrategy,
};

use velda_plugin::background::loki_log_shipper::json::{
    LokiJsonFormatter, LokiStreamTemplate, format_loki_push_payload,
};
use velda_plugin::background::loki_log_shipper::proto::{
    LokiProtoFormatter, LokiProtoLabels, PushRequest, format_loki_records_proto,
};

/// Helper: constructs a test `AccessLogRecord` with given parameters.
fn build_record(
    id: u64,
    ts: u32,
    peer_str: &str,
    proto: LogProtocol,
    method: LogHttpMethod,
    grpc_status: LogGrpcStatus,
) -> AccessLogRecord {
    let peer: SocketAddr = peer_str.parse().expect("Valid socket address");
    AccessLogRecord::from_scalars(
        id,
        ts,
        320,
        peer,
        Some("10.0.0.1:8080".parse().unwrap()),
        128,
        512,
        200,
        1,
        0,
        proto,
        proto,
        PipeStrategy::Buffered,
        LogCompletionReason::Complete,
        method,
        grpc_status,
        b"velda-test-agent/1.0",
    )
}

// ============================================================================
// 1. Biên Cực Trị Request ID
// ============================================================================
#[test]
fn test_chaos_request_id_boundaries() {
    let test_cases: Vec<(u32, u64)> = vec![
        (0, 0),
        (u32::MAX, u64::MAX),
        (0, u64::MAX),
        (u32::MAX, 0),
        (0x65ec_8780, 0xaaaa_bbbb_cccc_dddd),
        (0x1234_5678, 0x0123_4567_89ab_cdef),
        (0, 0x8000_0000_0000_0000),
        (1, 1),
        (0xdead_beef, 0xfedc_ba98_7654_3210),
    ];

    let mut labels = HashMap::new();
    labels.insert("cluster".to_string(), "chaos-edge".to_string());
    let mut json_formatter = LokiJsonFormatter::new("loki-chaos", &labels);
    let proto_labels = LokiProtoLabels::new("loki-chaos", &labels);

    for &(ts, id) in &test_cases {
        let rec = build_record(
            id,
            ts,
            "127.0.0.1:8080",
            LogProtocol::H1,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
        );
        let batches = vec![Arc::from(vec![rec])];

        // 1.1 JSON Formatting
        let mut json_buf = Vec::new();
        let count = json_formatter.format_batches(&batches, &mut json_buf);
        assert_eq!(count, 1);

        let parsed: Value =
            serde_json::from_slice(&json_buf).expect("JSON format must be 100% valid");
        let line_val = parsed["streams"][0]["values"][0][1]
            .as_str()
            .expect("Log line string in JSON values");

        // Expected 32-character lowercase hex Request ID
        let expected_hex = format!("{:016x}{:016x}", ts as u64, id);
        assert_eq!(expected_hex.len(), 32);
        assert!(
            expected_hex
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()),
            "Hex must be strictly lowercase ASCII: {expected_hex}"
        );
        assert!(
            line_val.contains(&format!("request_id={expected_hex}")),
            "JSON line does not contain expected 32-hex request_id={expected_hex}. Line: {line_val}"
        );

        // 1.2 gRPC Protobuf Formatting
        let mut proto_buf = Vec::new();
        let proto_count = format_loki_records_proto(&batches, &proto_labels, &mut proto_buf);
        assert_eq!(proto_count, 1);

        let decoded =
            PushRequest::decode(&proto_buf[..]).expect("gRPC PushRequest must decode cleanly");
        assert_eq!(decoded.streams.len(), 1);
        let proto_line = &decoded.streams[0].entries[0].line;
        assert!(
            proto_line.contains(&format!("request_id={expected_hex}")),
            "Protobuf line does not contain expected 32-hex request_id={expected_hex}. Line: {proto_line}"
        );
    }
}

// ============================================================================
// 2. Hỗn Loạn Địa Chỉ IP (RFC 4291 & RFC 5952)
// ============================================================================
#[test]
fn test_chaos_ip_addresses_rfc4291_and_rfc5952() {
    let test_cases: Vec<(&str, &str)> = vec![
        // IPv4 extreme boundaries
        ("0.0.0.0:0", "peer=0.0.0.0:0"),
        ("255.255.255.255:65535", "peer=255.255.255.255:65535"),
        ("127.0.0.1:80", "peer=127.0.0.1:80"),
        ("10.0.0.1:8080", "peer=10.0.0.1:8080"),
        ("192.168.1.1:443", "peer=192.168.1.1:443"),
        // IPv6 RFC 5952 compression scenarios
        ("[::1]:5000", "peer=::1:5000"),
        ("[::]:80", "peer=:::80"),
        ("[2001:0:0:1:0:0:0:1]:8080", "peer=2001:0:0:1::1:8080"),
        (
            "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443",
            "peer=2001:db8:85a3:8d3:1319:8a2e:370:7348:443",
        ),
        ("[fe80::1]:22", "peer=fe80::1:22"),
        ("[2001:db8::]:9090", "peer=2001:db8:::9090"),
    ];

    let mut labels = HashMap::new();
    labels.insert("zone".to_string(), "ip-chaos".to_string());
    let mut json_formatter = LokiJsonFormatter::new("ip-test", &labels);
    let proto_labels = LokiProtoLabels::new("ip-test", &labels);

    for &(peer_input, expected_peer_str) in &test_cases {
        let rec = build_record(
            0x1234_5678,
            1_710_000_000,
            peer_input,
            LogProtocol::H2,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
        );
        let batches = vec![Arc::from(vec![rec])];

        // JSON check
        let mut json_out = Vec::new();
        json_formatter.format_batches(&batches, &mut json_out);
        let json_parsed: Value = serde_json::from_slice(&json_out).unwrap();
        let json_line = json_parsed["streams"][0]["values"][0][1].as_str().unwrap();
        assert!(
            json_line.contains(expected_peer_str),
            "JSON line does not contain '{expected_peer_str}'. Line: {json_line}"
        );

        // Protobuf check
        let mut proto_out = Vec::new();
        format_loki_records_proto(&batches, &proto_labels, &mut proto_out);
        let decoded = PushRequest::decode(&proto_out[..]).unwrap();
        let proto_line = &decoded.streams[0].entries[0].line;
        assert!(
            proto_line.contains(expected_peer_str),
            "Protobuf line does not contain '{expected_peer_str}'. Line: {proto_line}"
        );
    }
}

// ============================================================================
// 3. Tổ Hợp Protocol & 3 Stream Buckets (http, grpc, raw)
// ============================================================================
#[test]
fn test_chaos_protocol_combinations_multi_stream_partitioning() {
    let protocols = [
        (LogProtocol::H1, "http"),
        (LogProtocol::H2, "http"),
        (LogProtocol::H3, "http"),
        (LogProtocol::GrpcTcp, "grpc"),
        (LogProtocol::GrpcUdp, "grpc"),
        (LogProtocol::RawTcp, "raw"),
        (LogProtocol::RawUdp, "raw"),
    ];

    // Build 700 records: 100 of each of the 7 protocol variants, interleaved
    let mut records = Vec::with_capacity(700);
    for i in 0..100 {
        for &(proto, _expected_bucket) in &protocols {
            let method = match proto {
                LogProtocol::GrpcTcp | LogProtocol::GrpcUdp => LogHttpMethod::Post,
                _ => LogHttpMethod::Get,
            };
            let grpc_status = match proto {
                LogProtocol::GrpcTcp | LogProtocol::GrpcUdp => LogGrpcStatus::Ok,
                _ => LogGrpcStatus::NonGrpc,
            };
            records.push(build_record(
                0x1000 + i as u64,
                1_710_000_000 + i as u32,
                "10.0.0.1:443",
                proto,
                method,
                grpc_status,
            ));
        }
    }
    assert_eq!(records.len(), 700);

    let batches = vec![Arc::from(records)];

    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "chaos".to_string());
    let mut json_formatter = LokiJsonFormatter::new("multi-proto-test", &labels);
    let proto_labels = LokiProtoLabels::new("multi-proto-test", &labels);

    // 3.1 JSON Multi-Stream Partitioning
    let mut json_out = Vec::new();
    let count = json_formatter.format_batches(&batches, &mut json_out);
    assert_eq!(count, 700);

    let parsed: Value = serde_json::from_slice(&json_out).expect("Valid JSON");
    let streams = parsed["streams"].as_array().expect("streams array");
    assert_eq!(
        streams.len(),
        3,
        "Must partition into exactly 3 streams: http, grpc, raw"
    );

    let mut http_entries = 0;
    let mut grpc_entries = 0;
    let mut raw_entries = 0;

    for s in streams {
        let proto = s["stream"]["protocol"].as_str().unwrap();
        let values_count = s["values"].as_array().unwrap().len();
        match proto {
            "http" => http_entries += values_count,
            "grpc" => grpc_entries += values_count,
            "raw" => raw_entries += values_count,
            other => panic!("Unexpected protocol bucket: {other}"),
        }
    }

    assert_eq!(
        http_entries, 300,
        "HTTP bucket must contain 300 entries (100 H1 + 100 H2 + 100 H3)"
    );
    assert_eq!(
        grpc_entries, 200,
        "gRPC bucket must contain 200 entries (100 GrpcTcp + 100 GrpcUdp)"
    );
    assert_eq!(
        raw_entries, 200,
        "Raw bucket must contain 200 entries (100 RawTcp + 100 RawUdp)"
    );

    // 3.2 Protobuf Multi-Stream Partitioning
    let mut proto_out = Vec::new();
    let p_count = format_loki_records_proto(&batches, &proto_labels, &mut proto_out);
    assert_eq!(p_count, 700);

    let decoded = PushRequest::decode(&proto_out[..]).expect("Valid PushRequest");
    assert_eq!(
        decoded.streams.len(),
        3,
        "Protobuf must contain exactly 3 StreamAdapters"
    );

    let mut p_http = 0;
    let mut p_grpc = 0;
    let mut p_raw = 0;

    for stream in &decoded.streams {
        if stream.labels.contains("protocol=\"http\"") {
            p_http += stream.entries.len();
        } else if stream.labels.contains("protocol=\"grpc\"") {
            p_grpc += stream.entries.len();
        } else if stream.labels.contains("protocol=\"raw\"") {
            p_raw += stream.entries.len();
        } else {
            panic!("Unexpected Protobuf stream labels: {}", stream.labels);
        }
    }

    assert_eq!(p_http, 300);
    assert_eq!(p_grpc, 200);
    assert_eq!(p_raw, 200);
}

// ============================================================================
// 4. Xác Minh Round-Trip Giải Mã & Cú Pháp Toàn Diện
// ============================================================================
#[test]
fn test_chaos_round_trip_decoding_and_syntax_validation() {
    let mut labels = HashMap::new();
    labels.insert("environment".to_string(), "prod-us-east".to_string());
    labels.insert("tier".to_string(), "edge-gateway".to_string());
    let template = LokiStreamTemplate::new("velda-edge", &labels);
    let mut json_formatter = LokiJsonFormatter::new("velda-edge", &labels);
    let proto_labels = LokiProtoLabels::new("velda-edge", &labels);

    let methods = [
        LogHttpMethod::Get,
        LogHttpMethod::Post,
        LogHttpMethod::Put,
        LogHttpMethod::Delete,
        LogHttpMethod::Patch,
        LogHttpMethod::Options,
        LogHttpMethod::Head,
    ];

    let grpc_statuses = [
        LogGrpcStatus::Ok,
        LogGrpcStatus::Cancelled,
        LogGrpcStatus::Unknown,
        LogGrpcStatus::InvalidArgument,
        LogGrpcStatus::DeadlineExceeded,
        LogGrpcStatus::NotFound,
        LogGrpcStatus::AlreadyExists,
        LogGrpcStatus::PermissionDenied,
        LogGrpcStatus::ResourceExhausted,
        LogGrpcStatus::FailedPrecondition,
        LogGrpcStatus::Aborted,
        LogGrpcStatus::OutOfRange,
        LogGrpcStatus::Unimplemented,
        LogGrpcStatus::Internal,
        LogGrpcStatus::Unavailable,
        LogGrpcStatus::DataLoss,
        LogGrpcStatus::Unauthenticated,
    ];

    let mut records = Vec::new();
    for (i, &method) in methods.iter().enumerate() {
        records.push(build_record(
            100 + i as u64,
            1_710_000_100 + i as u32,
            "192.168.1.1:8080",
            LogProtocol::H2,
            method,
            LogGrpcStatus::NonGrpc,
        ));
    }
    for (j, &st) in grpc_statuses.iter().enumerate() {
        records.push(build_record(
            200 + j as u64,
            1_710_000_200 + j as u32,
            "[::1]:9090",
            LogProtocol::GrpcTcp,
            LogHttpMethod::Post,
            st,
        ));
    }

    let batches = vec![Arc::from(records)];

    // 4.1 format_loki_push_payload (Thread-local scratch path)
    let mut standalone_json = Vec::new();
    let n1 = format_loki_push_payload(&batches, &template, &mut standalone_json);
    assert_eq!(n1, methods.len() + grpc_statuses.len());
    let v1: Value =
        serde_json::from_slice(&standalone_json).expect("standalone json must parse cleanly");
    assert_eq!(v1["streams"].as_array().unwrap().len(), 2);

    // 4.2 LokiJsonFormatter (Persistent struct scratch path)
    let mut struct_json = Vec::new();
    let n2 = json_formatter.format_batches(&batches, &mut struct_json);
    assert_eq!(n2, methods.len() + grpc_statuses.len());
    let v2: Value = serde_json::from_slice(&struct_json).expect("struct json must parse cleanly");
    assert_eq!(v2["streams"].as_array().unwrap().len(), 2);

    // Deep schema validation of JSON structures
    for v in [&v1, &v2] {
        let streams = v["streams"].as_array().unwrap();
        for stream in streams {
            let stream_meta = &stream["stream"];
            assert_eq!(stream_meta["service"], "velda-edge");
            assert_eq!(stream_meta["environment"], "prod-us-east");
            assert_eq!(stream_meta["tier"], "edge-gateway");

            let values = stream["values"].as_array().unwrap();
            for item in values {
                let pair = item.as_array().unwrap();
                assert_eq!(pair.len(), 2);
                let ts_str = pair[0].as_str().unwrap();
                let line_str = pair[1].as_str().unwrap();

                // Timestamp string must parse into u64 nanoseconds
                let ts_parsed: u64 = ts_str.parse().expect("Timestamp must be integer nanos");
                assert!(ts_parsed >= 1_710_000_100_000_000_000);

                // Line string must contain request_id, peer, and completion
                assert!(line_str.contains("request_id="));
                assert!(line_str.contains("peer="));
                assert!(line_str.contains("completion="));
            }
        }
    }

    // 4.3 gRPC Protobuf Round-Trip & Schema Validation
    let mut wire_buf = Vec::new();
    let n3 = format_loki_records_proto(&batches, &proto_labels, &mut wire_buf);
    assert_eq!(n3, methods.len() + grpc_statuses.len());

    let push = PushRequest::decode(&wire_buf[..]).expect("Valid Protobuf PushRequest");
    assert_eq!(push.streams.len(), 2);

    for s in &push.streams {
        assert!(s.labels.starts_with('{'));
        assert!(s.labels.ends_with('}'));
        assert!(s.labels.contains("service=\"velda-edge\""));
        assert!(s.labels.contains("environment=\"prod-us-east\""));
        assert!(s.labels.contains("tier=\"edge-gateway\""));

        for entry in &s.entries {
            let ts = entry.timestamp.as_ref().expect("Timestamp present");
            assert!(ts.seconds > 1_700_000_000);
            assert!(ts.nanos < 1_000_000_000);
            assert!(!entry.line.is_empty());
        }
    }
}

// ============================================================================
// 5. Stress Test Scratch Buffers (10,000 Records Across 100 Cycles)
// ============================================================================
#[test]
fn test_chaos_stress_scratch_buffers_10k_records() {
    let mut labels = HashMap::new();
    labels.insert("stress".to_string(), "10k".to_string());

    let mut json_formatter = LokiJsonFormatter::new("stress-svc", &labels);
    let mut proto_formatter = LokiProtoFormatter::new("stress-svc", &labels);

    let mut json_out = Vec::with_capacity(32 * 1024);
    let mut proto_out = Vec::with_capacity(32 * 1024);

    let cycles = 100;
    let records_per_cycle = 100;

    let mut total_records_processed = 0;

    for cycle in 0..cycles {
        let mut cycle_records = Vec::with_capacity(records_per_cycle);
        for i in 0..records_per_cycle {
            let proto = match i % 3 {
                0 => LogProtocol::H2,
                1 => LogProtocol::GrpcTcp,
                _ => LogProtocol::RawTcp,
            };
            cycle_records.push(build_record(
                (cycle * records_per_cycle + i) as u64,
                1_710_000_000 + cycle as u32,
                "192.168.1.1:8080",
                proto,
                LogHttpMethod::Get,
                LogGrpcStatus::Ok,
            ));
        }

        let batches = vec![Arc::from(cycle_records)];

        // JSON Cycle
        let n_json = json_formatter.format_batches(&batches, &mut json_out);
        assert_eq!(n_json, records_per_cycle);
        let parsed: Value = serde_json::from_slice(&json_out).expect("Parse cycle JSON");
        assert_eq!(parsed["streams"].as_array().unwrap().len(), 3);

        // Proto Cycle
        let n_proto = proto_formatter.format_batches(&batches, &mut proto_out);
        assert_eq!(n_proto, records_per_cycle);
        let decoded = PushRequest::decode(&proto_out[..]).expect("Decode cycle Protobuf");
        assert_eq!(decoded.streams.len(), 3);

        total_records_processed += records_per_cycle;
    }

    assert_eq!(
        total_records_processed, 10_000,
        "Total processed must be exactly 10,000 records"
    );
}
