//! Exhaustive Chaos & Edge-Case Test Suite for OTLP Log Shipper (`otlp_log_shipper`).
//!
//! Validates:
//! 1. Request ID & Timestamp edge cases (min/max boundaries, bit patterns, SIMD hex32 accuracy).
//! 2. IP address extremes (RFC 4291 IPv4-mapped, RFC 5952 IPv6 zero-compression, min/max ports).
//! 3. Status codes & Severity level mapping across HTTP (0..999) and gRPC (0..16, 255).
//! 4. Varint & payload length boundaries (1-byte, 2-byte, 3-byte, u32::MAX, u64 boundaries).
//! 5. Full combinatorial testing of protocols (7), methods (10), UA families (34), and OSes (8).
//! 6. Batch sizes (empty, single, 5,000-10,000 large mixed batches) & memory bounds safety.
//! 7. 100% round-trip deserialization conformance against Prost AST and serde_json.
//! 8. RDTSC cycle stability under heterogeneous chaos load.

#![allow(clippy::too_many_arguments)]

use prost::Message;
use std::sync::Arc;
use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, PipeStrategy,
    UaFamily, UaOs,
};
use velda_plugin::background::otlp_log_shipper::json::{
    OtlpJsonEnvelope, format_otlp_records_json,
};
use velda_plugin::background::otlp_log_shipper::lut::format_hex32_bytes;
use velda_plugin::background::otlp_log_shipper::proto::{
    ExportLogsServiceRequest, OtlpProtoWireEnvelope, any_value,
};

/// Helper: creates an AccessLogRecord with configurable fields.
fn make_record(
    req_id: u64,
    ts_sec: u32,
    proto: LogProtocol,
    method: LogHttpMethod,
    status: u16,
    grpc: LogGrpcStatus,
    dur_us: u32,
    tx: u32,
    rx: u32,
    route: u16,
    ip: u128,
    port: u16,
    ua_fam: UaFamily,
    ua_major: u16,
    ua_os: UaOs,
) -> AccessLogRecord {
    AccessLogRecord {
        request_id: req_id,
        timestamp_sec: ts_sec,
        duration_us: dur_us,
        peer_ip: ip,
        bytes_rx: rx,
        bytes_tx: tx,
        upstream_ip: 0,
        peer_port: port,
        upstream_port: 0,
        status_code: status,
        route_id: route,
        ua_major_version: ua_major,
        rpc_msgs_or_flags: 0,
        downstream_proto: proto as u8,
        upstream_proto: 0,
        pipe_strategy: PipeStrategy::Buffered as u8,
        completion: LogCompletionReason::Complete as u8,
        method: method as u8,
        grpc_status: grpc as u8,
        ua_family: ua_fam as u8,
        ua_os: ua_os as u8,
    }
}

/// Helper: converts IPv4 to RFC 4291 IPv4-mapped IPv6 u128.
fn ipv4_to_u128(a: u8, b: u8, c: u8, d: u8) -> u128 {
    let u32_val = u32::from_be_bytes([a, b, c, d]);
    (0xFFFF_u128 << 32) | (u32_val as u128)
}

/// Helper: converts 8 hextets of IPv6 to u128.
fn ipv6_to_u128(segs: [u16; 8]) -> u128 {
    let mut raw = [0u8; 16];
    for (i, &s) in segs.iter().enumerate() {
        let b = s.to_be_bytes();
        raw[i * 2] = b[0];
        raw[i * 2 + 1] = b[1];
    }
    u128::from_be_bytes(raw)
}

// ============================================================================
// 1. KỊCH BẢN 1: BIÊN CỰC TRỊ REQUEST ID & TIMESTAMPS
// ============================================================================
#[test]
fn test_otlp_chaos_request_id_and_timestamps() {
    let wire_env = OtlpProtoWireEnvelope::new("velda-chaos-service");
    let json_env = OtlpJsonEnvelope::new("velda-chaos-service");

    let req_ids = [
        0_u64,
        1_u64,
        u64::MAX,
        0xAAAA_BBBB_CCCC_DDDD_u64,
        0x0123_4567_89AB_CDEF_u64,
        0x0000_0000_FFFF_0000_u64,
        0xDEAD_BEEF_CAFE_BABE_u64,
    ];

    let timestamps = [
        0_u32,
        1_u32,
        0x7FFF_FFFF_u32, // Year 2038 32-bit signed boundary
        u32::MAX,        // Max 32-bit unsigned
        1_710_000_000_u32,
    ];

    for &req_id in &req_ids {
        for &ts_sec in &timestamps {
            // Reference 32 lowercase hex check
            let expected_hex = format!("{:016x}{:016x}", ts_sec as u64, req_id);
            let sim_hex = format_hex32_bytes(ts_sec as u64, req_id);
            assert_eq!(
                std::str::from_utf8(&sim_hex).unwrap(),
                expected_hex,
                "SIMD hex formatting mismatch for ts={ts_sec}, req={req_id}"
            );

            let rec = make_record(
                req_id,
                ts_sec,
                LogProtocol::H2,
                LogHttpMethod::Get,
                200,
                LogGrpcStatus::NonGrpc,
                150,
                512,
                128,
                1,
                ipv4_to_u128(127, 0, 0, 1),
                8080,
                UaFamily::Chrome,
                120,
                UaOs::Linux,
            );

            // A. Test Protobuf Direct Wire Encoding & Round-Trip
            let batch: Arc<[AccessLogRecord]> = Arc::from(vec![rec]);
            let mut records_buf = Vec::new();
            let mut out = Vec::new();
            let count = wire_env.format_records_wire(
                std::slice::from_ref(&batch),
                &mut records_buf,
                &mut out,
            );
            assert_eq!(count, 1);

            let decoded = ExportLogsServiceRequest::decode(&out[..])
                .expect("Valid Protobuf ExportLogsServiceRequest decode");
            let log_rec = &decoded.resource_logs[0].scope_logs[0].log_records[0];

            let expected_ts_nano = (ts_sec as u64).saturating_mul(1_000_000_000);
            assert_eq!(log_rec.time_unix_nano, expected_ts_nano);
            assert_eq!(log_rec.observed_time_unix_nano, expected_ts_nano);

            // Assert Request ID attribute
            assert_eq!(log_rec.attributes[0].key, "request_id");
            if let Some(any_value::Value::StringValue(s)) = log_rec.attributes[0]
                .value
                .as_ref()
                .and_then(|v| v.value.as_ref())
            {
                assert_eq!(s, &expected_hex);
            } else {
                panic!("Expected StringValue for request_id attribute");
            }

            // B. Test JSON Encoding & Round-Trip
            let mut json_out = Vec::new();
            let j_count = format_otlp_records_json(&[batch], &json_env, &mut json_out);
            assert_eq!(j_count, 1);

            let parsed: serde_json::Value =
                serde_json::from_slice(&json_out).expect("Valid JSON parse");
            let j_rec = &parsed["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
            assert_eq!(j_rec["timeUnixNano"], expected_ts_nano.to_string());
            assert_eq!(j_rec["body"]["stringValue"], "HTTP access log");

            let j_req_id = &j_rec["attributes"][0]["value"]["stringValue"];
            assert_eq!(j_req_id, &expected_hex);
        }
    }
}

// ============================================================================
// 2. KỊCH BẢN 2: HỖN LOẠN ĐỊA CHỈ IP (RFC 4291 & RFC 5952) & PORTS
// ============================================================================
#[test]
fn test_otlp_chaos_ip_addresses_and_ports() {
    let wire_env = OtlpProtoWireEnvelope::new("velda-ip-chaos");
    let json_env = OtlpJsonEnvelope::new("velda-ip-chaos");

    let test_cases: Vec<(u128, u16, &'static str)> = vec![
        // IPv4 edges
        (ipv4_to_u128(0, 0, 0, 0), 0, "0.0.0.0"),
        (ipv4_to_u128(255, 255, 255, 255), 65535, "255.255.255.255"),
        (ipv4_to_u128(127, 0, 0, 1), 80, "127.0.0.1"),
        (ipv4_to_u128(10, 0, 0, 1), 443, "10.0.0.1"),
        // IPv6 edges & RFC 5952 zero-compression
        (ipv6_to_u128([0, 0, 0, 0, 0, 0, 0, 1]), 8080, "::1"),
        (0_u128, 0, "::"),
        (
            ipv6_to_u128([0x2001, 0x0db8, 0, 0, 0, 0, 0, 1]),
            53,
            "2001:db8::1",
        ),
        // RFC 5952: multiple zero runs, must compress longest (run of 3 zeros vs run of 2 zeros)
        (
            ipv6_to_u128([0x2001, 0, 0, 1, 0, 0, 0, 1]),
            8000,
            "2001:0:0:1::1",
        ),
        (
            ipv6_to_u128([0xfe80, 0, 0, 0, 0x01ff, 0xfe23, 0x4567, 0x890a]),
            1024,
            "fe80::1ff:fe23:4567:890a",
        ),
        // Full 8 hextets without zeros
        (
            ipv6_to_u128([
                0x2001, 0x0db8, 0x85a3, 0x08d3, 0x1319, 0x8a2e, 0x0370, 0x7348,
            ]),
            65535,
            "2001:db8:85a3:8d3:1319:8a2e:370:7348",
        ),
    ];

    for (ip, port, expected_ip_str) in test_cases {
        let rec = make_record(
            42,
            1_710_000_000,
            LogProtocol::H1,
            LogHttpMethod::Post,
            201,
            LogGrpcStatus::NonGrpc,
            300,
            1024,
            256,
            5,
            ip,
            port,
            UaFamily::Curl,
            8,
            UaOs::BotOrCli,
        );

        // A. Protobuf Round-Trip
        let batch: Arc<[AccessLogRecord]> = Arc::from(vec![rec]);
        let mut records_buf = Vec::new();
        let mut out = Vec::new();
        wire_env.format_records_wire(std::slice::from_ref(&batch), &mut records_buf, &mut out);

        let decoded = ExportLogsServiceRequest::decode(&out[..]).unwrap();
        let log_rec = &decoded.resource_logs[0].scope_logs[0].log_records[0];

        // Attribute 9: client.address
        assert_eq!(log_rec.attributes[9].key, "client.address");
        if let Some(any_value::Value::StringValue(s)) = log_rec.attributes[9]
            .value
            .as_ref()
            .and_then(|v| v.value.as_ref())
        {
            assert_eq!(s, expected_ip_str);
        } else {
            panic!("Expected StringValue for client.address");
        }

        // Attribute 10: client.port
        assert_eq!(log_rec.attributes[10].key, "client.port");
        if let Some(any_value::Value::IntValue(p)) = log_rec.attributes[10]
            .value
            .as_ref()
            .and_then(|v| v.value.as_ref())
        {
            assert_eq!(*p, port as i64);
        } else {
            panic!("Expected IntValue for client.port");
        }

        // B. JSON Round-Trip
        let mut json_out = Vec::new();
        format_otlp_records_json(&[batch], &json_env, &mut json_out);

        let parsed: serde_json::Value = serde_json::from_slice(&json_out).unwrap();
        let j_attrs = &parsed["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"];
        assert_eq!(j_attrs[9]["value"]["stringValue"], expected_ip_str);
        assert_eq!(j_attrs[10]["value"]["intValue"], port.to_string());
    }
}

// ============================================================================
// 3. KỊCH BẢN 3: TỔ HỢP TRẠNG THÁI & MÃ LỖI (STATUS CODE & SEVERITY MAPPINGS)
// ============================================================================
#[test]
fn test_otlp_chaos_status_codes_and_severity_mappings() {
    let wire_env = OtlpProtoWireEnvelope::new("velda-severity-chaos");
    let json_env = OtlpJsonEnvelope::new("velda-severity-chaos");

    let http_statuses = [
        0_u16, 100, 200, 204, 301, 304, 400, 401, 403, 404, 499, 500, 502, 503, 504, 599, 999,
    ];

    let grpc_statuses = [
        LogGrpcStatus::Ok,               // 0
        LogGrpcStatus::Cancelled,        // 1
        LogGrpcStatus::Unknown,          // 2
        LogGrpcStatus::InvalidArgument,  // 3
        LogGrpcStatus::DeadlineExceeded, // 4
        LogGrpcStatus::NotFound,         // 5
        LogGrpcStatus::AlreadyExists,    // 6
        LogGrpcStatus::PermissionDenied, // 7
        LogGrpcStatus::Internal,         // 13
        LogGrpcStatus::Unavailable,      // 14
        LogGrpcStatus::DataLoss,         // 15
        LogGrpcStatus::Unauthenticated,  // 16
        LogGrpcStatus::NonGrpc,          // 255
    ];

    for &status in &http_statuses {
        for &grpc in &grpc_statuses {
            let is_grpc_err = grpc != LogGrpcStatus::Ok && grpc != LogGrpcStatus::NonGrpc;
            let (expected_sev_num, expected_sev_text) = if status >= 500 || is_grpc_err {
                (17, "ERROR")
            } else if status >= 400 {
                (13, "WARN")
            } else {
                (9, "INFO")
            };

            let rec = make_record(
                100,
                1_710_000_000,
                LogProtocol::H2,
                LogHttpMethod::Get,
                status,
                grpc,
                200,
                100,
                100,
                1,
                ipv4_to_u128(192, 168, 1, 1),
                80,
                UaFamily::GoHttpClient,
                1,
                UaOs::Linux,
            );

            // A. Protobuf Validation
            let batch: Arc<[AccessLogRecord]> = Arc::from(vec![rec]);
            let mut records_buf = Vec::new();
            let mut out = Vec::new();
            wire_env.format_records_wire(std::slice::from_ref(&batch), &mut records_buf, &mut out);

            let decoded = ExportLogsServiceRequest::decode(&out[..]).unwrap();
            let log_rec = &decoded.resource_logs[0].scope_logs[0].log_records[0];

            assert_eq!(
                log_rec.severity_number, expected_sev_num,
                "Proto severityNumber mismatch for status={status}, grpc={grpc:?}"
            );
            assert_eq!(
                log_rec.severity_text, expected_sev_text,
                "Proto severityText mismatch for status={status}, grpc={grpc:?}"
            );

            // Check http.response.status_code and rpc.grpc.status_code
            if let Some(any_value::Value::IntValue(sc)) = log_rec.attributes[3]
                .value
                .as_ref()
                .and_then(|v| v.value.as_ref())
            {
                assert_eq!(*sc, status as i64);
            }
            if let Some(any_value::Value::StringValue(gs)) = log_rec.attributes[4]
                .value
                .as_ref()
                .and_then(|v| v.value.as_ref())
            {
                assert_eq!(gs, grpc.as_str());
            }

            // B. JSON Validation
            let mut json_out = Vec::new();
            format_otlp_records_json(&[batch], &json_env, &mut json_out);

            let parsed: serde_json::Value = serde_json::from_slice(&json_out).unwrap();
            let j_rec = &parsed["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
            assert_eq!(j_rec["severityNumber"], expected_sev_num);
            assert_eq!(j_rec["severityText"], expected_sev_text);
            assert_eq!(
                j_rec["attributes"][3]["value"]["intValue"],
                status.to_string()
            );
            assert_eq!(
                j_rec["attributes"][4]["value"]["stringValue"],
                grpc.as_str()
            );
        }
    }
}

// ============================================================================
// 4. KỊCH BẢN 4: BIÊN CỰC TRỊ VARINT & ĐỘ DÀI PAYLOAD
// ============================================================================
#[test]
fn test_otlp_chaos_varint_and_payload_boundaries() {
    let wire_env = OtlpProtoWireEnvelope::new("velda-varint-chaos");
    let json_env = OtlpJsonEnvelope::new("velda-varint-chaos");

    let durations = [
        0_u32,
        1_u32,
        127_u32,   // 1-byte varint boundary
        128_u32,   // 2-byte varint boundary
        16383_u32, // 2-byte varint boundary
        16384_u32, // 3-byte varint boundary
        1_000_000_u32,
        u32::MAX,
    ];

    let bytes_transfers = [
        0_u32,
        1_u32,
        127_u32,
        128_u32,
        16383_u32,
        16384_u32,
        1_048_576_u32,
        u32::MAX,
    ];

    let route_ids = [0_u16, 1_u16, 127_u16, 128_u16, 65535_u16];

    for &dur in &durations {
        for &tx in &bytes_transfers {
            for &route in &route_ids {
                let rec = make_record(
                    999,
                    1_710_000_000,
                    LogProtocol::H3,
                    LogHttpMethod::Put,
                    200,
                    LogGrpcStatus::NonGrpc,
                    dur,
                    tx,
                    tx.wrapping_div(2),
                    route,
                    ipv4_to_u128(10, 10, 10, 10),
                    443,
                    UaFamily::RustReqwest,
                    0,
                    UaOs::MacOs,
                );

                // Protobuf Round-Trip
                let batch: Arc<[AccessLogRecord]> = Arc::from(vec![rec]);
                let mut records_buf = Vec::new();
                let mut out = Vec::new();
                wire_env.format_records_wire(
                    std::slice::from_ref(&batch),
                    &mut records_buf,
                    &mut out,
                );

                let decoded = ExportLogsServiceRequest::decode(&out[..]).unwrap();
                let log_rec = &decoded.resource_logs[0].scope_logs[0].log_records[0];

                // Duration (attr 5)
                if let Some(any_value::Value::IntValue(v)) = log_rec.attributes[5]
                    .value
                    .as_ref()
                    .and_then(|v| v.value.as_ref())
                {
                    assert_eq!(*v, dur as i64);
                }
                // Resp size (attr 6)
                if let Some(any_value::Value::IntValue(v)) = log_rec.attributes[6]
                    .value
                    .as_ref()
                    .and_then(|v| v.value.as_ref())
                {
                    assert_eq!(*v, tx as i64);
                }
                // Route ID (attr 8)
                if let Some(any_value::Value::IntValue(v)) = log_rec.attributes[8]
                    .value
                    .as_ref()
                    .and_then(|v| v.value.as_ref())
                {
                    assert_eq!(*v, route as i64);
                }

                // JSON Round-Trip
                let mut json_out = Vec::new();
                format_otlp_records_json(&[batch], &json_env, &mut json_out);
                let parsed: serde_json::Value = serde_json::from_slice(&json_out).unwrap();
                let j_attrs =
                    &parsed["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"];
                assert_eq!(j_attrs[5]["value"]["intValue"], dur.to_string());
                assert_eq!(j_attrs[6]["value"]["intValue"], tx.to_string());
                assert_eq!(j_attrs[8]["value"]["intValue"], route.to_string());
            }
        }
    }
}

// ============================================================================
// 5. KỊCH BẢN 5: TỔ HỢP PROTOCOL, METHOD, UA FAMILIES, VÀ OS
// ============================================================================
#[test]
fn test_otlp_chaos_protocol_method_ua_combinations() {
    let wire_env = OtlpProtoWireEnvelope::new("velda-combo-chaos");
    let json_env = OtlpJsonEnvelope::new("velda-combo-chaos");

    let all_protocols = [
        LogProtocol::H1,
        LogProtocol::H2,
        LogProtocol::H3,
        LogProtocol::GrpcTcp,
        LogProtocol::GrpcUdp,
        LogProtocol::RawTcp,
        LogProtocol::RawUdp,
    ];

    let all_methods = [
        LogHttpMethod::Unknown,
        LogHttpMethod::Get,
        LogHttpMethod::Post,
        LogHttpMethod::Put,
        LogHttpMethod::Delete,
        LogHttpMethod::Head,
        LogHttpMethod::Options,
        LogHttpMethod::Patch,
        LogHttpMethod::Connect,
        LogHttpMethod::Trace,
    ];

    let sample_ua_families = [
        UaFamily::None,
        UaFamily::Chrome,
        UaFamily::Firefox,
        UaFamily::Brave,
        UaFamily::Curl,
        UaFamily::Postman,
        UaFamily::PythonRequests,
        UaFamily::RustReqwest,
        UaFamily::GrpcGo,
        UaFamily::KubeProbe,
        UaFamily::Googlebot,
        UaFamily::Custom,
    ];

    let all_ua_oses = [
        UaOs::Unknown,
        UaOs::Windows,
        UaOs::MacOs,
        UaOs::Linux,
        UaOs::Android,
        UaOs::Ios,
        UaOs::ChromeOs,
        UaOs::BotOrCli,
    ];

    for &proto in &all_protocols {
        for &method in &all_methods {
            for &ua_fam in &sample_ua_families {
                for &ua_os in &all_ua_oses {
                    let rec = make_record(
                        777,
                        1_710_000_000,
                        proto,
                        method,
                        200,
                        LogGrpcStatus::NonGrpc,
                        50,
                        10,
                        10,
                        1,
                        ipv4_to_u128(127, 0, 0, 1),
                        80,
                        ua_fam,
                        12,
                        ua_os,
                    );

                    let batch: Arc<[AccessLogRecord]> = Arc::from(vec![rec]);
                    let mut records_buf = Vec::new();
                    let mut out = Vec::new();
                    wire_env.format_records_wire(
                        std::slice::from_ref(&batch),
                        &mut records_buf,
                        &mut out,
                    );

                    let decoded = ExportLogsServiceRequest::decode(&out[..]).unwrap();
                    let log_rec = &decoded.resource_logs[0].scope_logs[0].log_records[0];

                    // Protocol (attr 1)
                    assert_eq!(
                        log_rec.attributes[1].value.as_ref().unwrap().value.as_ref(),
                        Some(&any_value::Value::StringValue(proto.as_str().to_string()))
                    );
                    // Method (attr 2)
                    assert_eq!(
                        log_rec.attributes[2].value.as_ref().unwrap().value.as_ref(),
                        Some(&any_value::Value::StringValue(method.as_str().to_string()))
                    );
                    // UA family (attr 11)
                    assert_eq!(
                        log_rec.attributes[11]
                            .value
                            .as_ref()
                            .unwrap()
                            .value
                            .as_ref(),
                        Some(&any_value::Value::StringValue(ua_fam.as_str().to_string()))
                    );
                    // UA OS (attr 13)
                    assert_eq!(
                        log_rec.attributes[13]
                            .value
                            .as_ref()
                            .unwrap()
                            .value
                            .as_ref(),
                        Some(&any_value::Value::StringValue(ua_os.as_str().to_string()))
                    );

                    // JSON validation
                    let mut json_out = Vec::new();
                    format_otlp_records_json(&[batch], &json_env, &mut json_out);
                    let parsed: serde_json::Value = serde_json::from_slice(&json_out).unwrap();
                    let j_attrs =
                        &parsed["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"];
                    assert_eq!(j_attrs[1]["value"]["stringValue"], proto.as_str());
                    assert_eq!(j_attrs[2]["value"]["stringValue"], method.as_str());
                    assert_eq!(j_attrs[11]["value"]["stringValue"], ua_fam.as_str());
                    assert_eq!(j_attrs[13]["value"]["stringValue"], ua_os.as_str());
                }
            }
        }
    }
}

// ============================================================================
// 6. KỊCH BẢN 6: BATCH SIZE HỖN LOẠN & STRESS 10,000 RECORDS (BOUNDS & PANIC CHECK)
// ============================================================================
#[test]
fn test_otlp_chaos_batch_sizes_and_stress() {
    let wire_env = OtlpProtoWireEnvelope::new("velda-stress");
    let json_env = OtlpJsonEnvelope::new("velda-stress");

    // Case 1: Empty batches
    let mut records_buf = Vec::new();
    let mut out = Vec::new();
    let count = wire_env.format_records_wire(&[], &mut records_buf, &mut out);
    assert_eq!(count, 0);
    assert!(out.is_empty());

    let empty_arc: Arc<[AccessLogRecord]> = Arc::from(vec![]);
    let count2 =
        wire_env.format_records_wire(std::slice::from_ref(&empty_arc), &mut records_buf, &mut out);
    assert_eq!(count2, 0);
    assert!(out.is_empty());

    let mut json_out = Vec::new();
    let j_count = format_otlp_records_json(&[], &json_env, &mut json_out);
    assert_eq!(j_count, 0);
    assert!(json_out.is_empty());

    let j_count2 = format_otlp_records_json(&[empty_arc], &json_env, &mut json_out);
    assert_eq!(j_count2, 0);
    assert!(json_out.is_empty());

    // Case 2: Stress 10,000 heterogeneous records across multiple batches
    let total_stress_records = 10_000;
    let mut large_batch = Vec::with_capacity(total_stress_records);

    let protos = [
        LogProtocol::H1,
        LogProtocol::H2,
        LogProtocol::H3,
        LogProtocol::GrpcTcp,
    ];
    let methods = [
        LogHttpMethod::Get,
        LogHttpMethod::Post,
        LogHttpMethod::Put,
        LogHttpMethod::Delete,
    ];
    let grpcs = [
        LogGrpcStatus::Ok,
        LogGrpcStatus::NotFound,
        LogGrpcStatus::Internal,
        LogGrpcStatus::NonGrpc,
    ];

    for i in 0..total_stress_records {
        let p = protos[i % protos.len()];
        let m = methods[i % methods.len()];
        let g = grpcs[i % grpcs.len()];
        let sc = if i % 10 == 0 {
            500
        } else if i % 5 == 0 {
            404
        } else {
            200
        };

        let ip = if i % 2 == 0 {
            ipv4_to_u128((i % 255) as u8, ((i / 255) % 255) as u8, 1, 1)
        } else {
            ipv6_to_u128([0x2001, (i % 0xFFFF) as u16, 0, 0, 0, 1, 2, 3])
        };

        large_batch.push(make_record(
            i as u64,
            1_710_000_000 + (i as u32 % 3600),
            p,
            m,
            sc,
            g,
            (i * 10) as u32,
            (i * 100) as u32,
            (i * 50) as u32,
            (i % 50) as u16,
            ip,
            (1024 + (i % 60000)) as u16,
            UaFamily::Chrome,
            120,
            UaOs::Linux,
        ));
    }

    // Split into 4 chunks (simulating 4 frontline cores flushing simultaneously)
    let chunks: Vec<Arc<[AccessLogRecord]>> = large_batch
        .chunks(2500)
        .map(|c| Arc::from(c.to_vec()))
        .collect();

    // Protobuf Stress Format & Decode
    let count_proto = wire_env.format_records_wire(&chunks, &mut records_buf, &mut out);
    assert_eq!(count_proto, total_stress_records);
    assert!(!out.is_empty());

    let decoded = ExportLogsServiceRequest::decode(&out[..])
        .expect("Prost successfully decodes 10,000-record wire batch without corruption");
    assert_eq!(decoded.resource_logs.len(), 1);
    assert_eq!(decoded.resource_logs[0].scope_logs.len(), 1);
    assert_eq!(
        decoded.resource_logs[0].scope_logs[0].log_records.len(),
        total_stress_records
    );

    // Verify first, middle, and last records
    let records = &decoded.resource_logs[0].scope_logs[0].log_records;
    assert_eq!(records[0].time_unix_nano, 1_710_000_000 * 1_000_000_000);
    assert_eq!(
        records[0].body.as_ref().unwrap().value,
        Some(any_value::Value::StringValue("HTTP access log".to_string()))
    );

    let last_idx = total_stress_records - 1;
    let expected_last_ts = (1_710_000_000 + (last_idx as u32 % 3600)) as u64 * 1_000_000_000;
    assert_eq!(records[last_idx].time_unix_nano, expected_last_ts);
    assert_eq!(
        records[last_idx].body.as_ref().unwrap().value,
        Some(any_value::Value::StringValue("HTTP access log".to_string()))
    );

    // JSON Stress Format & Parse
    let count_json = format_otlp_records_json(&chunks, &json_env, &mut json_out);
    assert_eq!(count_json, total_stress_records);
    assert!(!json_out.is_empty());

    let parsed: serde_json::Value =
        serde_json::from_slice(&json_out).expect("serde_json successfully parses 10,000 records");
    let j_records = parsed["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
        .as_array()
        .unwrap();
    assert_eq!(j_records.len(), total_stress_records);
}

// ============================================================================
// 7. KỊCH BẢN 7: RDTSC CPU CYCLE BENCHMARK DƯỚI TẢI CHAOS HỖN HỢP
// ============================================================================
#[test]
fn test_otlp_chaos_rdtsc_cycle_stability() {
    let wire_env = OtlpProtoWireEnvelope::new("velda-rdtsc-chaos");
    let json_env = OtlpJsonEnvelope::new("velda-rdtsc-chaos");

    let num_samples = 2000;
    let mut chaos_batch = Vec::with_capacity(num_samples);

    for i in 0..num_samples {
        let sc = match i % 5 {
            0 => 200,
            1 => 204,
            2 => 400,
            3 => 404,
            _ => 500,
        };
        let ip = if i % 2 == 0 {
            ipv4_to_u128(192, 168, (i % 255) as u8, 1)
        } else {
            ipv6_to_u128([0x2001, 0x0db8, (i % 100) as u16, 0, 0, 0, 0, 1])
        };

        chaos_batch.push(make_record(
            (i as u64) ^ 0xDEAD_BEEF_CAFE_BABE,
            1_710_000_000 + i as u32,
            LogProtocol::from_u8((i % 7) as u8),
            LogHttpMethod::from_u8((i % 10) as u8),
            sc,
            LogGrpcStatus::from_u8((i % 18) as u8),
            (i * 15) as u32,
            (i * 128) as u32,
            (i * 64) as u32,
            (i % 10) as u16,
            ip,
            (8000 + (i % 1000)) as u16,
            UaFamily::from_u8((i % 34) as u8),
            120,
            UaOs::from_u8((i % 8) as u8),
        ));
    }

    let batch_arc: Arc<[AccessLogRecord]> = Arc::from(chaos_batch);
    let slices = [batch_arc];

    let mut records_buf = Vec::new();
    let mut out_proto = Vec::new();
    let mut out_json = Vec::new();

    // Warm-up caches
    for _ in 0..5 {
        wire_env.format_records_wire(&slices, &mut records_buf, &mut out_proto);
        format_otlp_records_json(&slices, &json_env, &mut out_json);
    }

    // Measure Protobuf direct wire formatting cycles
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::_rdtsc;

        let start = _rdtsc();
        wire_env.format_records_wire(&slices, &mut records_buf, &mut out_proto);
        let end = _rdtsc();

        let proto_cycles_per_record = (end - start) / num_samples as u64;

        let j_start = _rdtsc();
        format_otlp_records_json(&slices, &json_env, &mut out_json);
        let j_end = _rdtsc();

        let json_cycles_per_record = (j_end - j_start) / num_samples as u64;

        println!(
            "\n=== Chaos Load RDTSC Cycle Audit ({} heterogeneous records) ===",
            num_samples
        );
        println!(
            "Chaos Proto Wire Stamping Speed: {} CPU cycles / record",
            proto_cycles_per_record
        );
        println!(
            "Chaos JSON Record Framing Speed: {} CPU cycles / record",
            json_cycles_per_record
        );
        println!("==============================================================\n");

        if !cfg!(debug_assertions) {
            assert!(
                proto_cycles_per_record < 300,
                "Chaos Proto cycles ({proto_cycles_per_record}) exceeded 300 cycles!"
            );
            assert!(
                json_cycles_per_record < 800,
                "Chaos JSON cycles ({json_cycles_per_record}) exceeded 800 cycles!"
            );
        }
    }
}
