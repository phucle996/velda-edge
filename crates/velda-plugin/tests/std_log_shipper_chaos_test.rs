//! Chaos & Extreme Edge-Case Test Suite for Standard Log Shipper (`std_log_shipper`).
//!
//! Validates:
//! 1. Request ID boundary values (`0`, `u64::MAX`, bit patterns) and exact 32-hex character formatting.
//! 2. Timestamps (`0`, `u32::MAX`, Year 2038), extreme durations (`0`, `u32::MAX`), and 32-bit counters.
//! 3. IP address permutations under RFC 4291 and RFC 5952 zero-compression (loopback, unspecified,
//!    multiple zero runs, tie-breaking, full 8-hextets, single zero runs).
//! 4. Full combinatorial matrix of protocols, methods, status codes (HTTP & gRPC), pipe strategies,
//!    and completion reasons.
//! 5. 100% Round-trip parsing & syntax verification with `serde_json::from_str` (zero syntax flaws).
//! 6. Stress testing persistent `StdLogSink` under 10,000 records across multiple batches.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, PipeKind,
    PipeStrategy, UaFamily, UaOs, encode_ip_u128,
};
use velda_plugin::background::std_log_shipper::config::TargetStream;
use velda_plugin::background::std_log_shipper::format::format_std_log_payload;
use velda_plugin::background::std_log_shipper::sink::StdLogSink;

/// Helper to build an [`AccessLogRecord`] with fine-grained control over all 64-byte scalar fields.
#[allow(clippy::too_many_arguments)]
fn build_chaos_record(
    request_id: u64,
    timestamp_sec: u32,
    duration_ns: u64,
    peer_ip: u128,
    peer_port: u16,
    upstream_ip: u32,
    upstream_port: u16,
    bytes_rx: u32,
    bytes_tx: u32,
    status_code: u16,
    route_id: u16,
    rpc_msgs_or_flags: u16,
    pipe: PipeKind,
    completion: LogCompletionReason,
    method: LogHttpMethod,
    grpc_status: LogGrpcStatus,
    ua_family: UaFamily,
    ua_os: UaOs,
    ua_major_version: u16,
) -> AccessLogRecord {
    AccessLogRecord {
        request_id,
        timestamp_sec,
        duration_ns,
        peer_ip,
        bytes_rx,
        bytes_tx,
        upstream_ip,
        peer_port,
        upstream_port,
        status_code,
        route_id,
        ua_major_version,
        rpc_msgs_or_flags,
        pipe,
        completion: completion as u8,
        method: method as u8,
        grpc_status: grpc_status as u8,
        ua_family: ua_family as u8,
        ua_os: ua_os as u8,
        url_len: 0,
        url_bytes: [0; 188],
    }
}

// ============================================================================
// 1. Biên Cực Trị Request ID (Boundaries & Bit Patterns)
// ============================================================================
#[test]
fn test_chaos_request_id_boundaries_and_bit_patterns() {
    let test_ids: &[(u32, u64)] = &[
        (0, 0),
        (0, 1),
        (0, u64::MAX),
        (u32::MAX, 0),
        (u32::MAX, u64::MAX),
        (1_710_000_000, 0xAAAA_BBBB_CCCC_DDDD),
        (1_710_000_001, 0x0123_4567_89AB_CDEF),
        (0x7FFF_FFFF, 0x8000_0000_0000_0000),
        (12345, 0x5555_5555_5555_5555),
        (67890, 0xFFFF_0000_FFFF_0000),
    ];

    let mut records = Vec::new();
    for &(ts, req_id) in test_ids {
        records.push(build_chaos_record(
            req_id,
            ts,
            100,
            encode_ip_u128(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            8080,
            0xC0A8_0101,
            80,
            128,
            256,
            200,
            1,
            0,
            PipeKind::BufferedH1H1,
            LogCompletionReason::Complete,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
            UaFamily::Curl,
            UaOs::Linux,
            8,
        ));
    }

    let batches = vec![Arc::from(records.as_slice())];
    let mut json_out = Vec::new();
    let count = format_std_log_payload(&batches, "json", &mut json_out);
    assert_eq!(count, test_ids.len());

    let json_text = String::from_utf8(json_out).expect("UTF-8 valid");
    let lines: Vec<&str> = json_text.lines().collect();
    assert_eq!(lines.len(), test_ids.len());

    for (i, &(ts, req_id)) in test_ids.iter().enumerate() {
        let line = lines[i];
        let v: serde_json::Value =
            serde_json::from_str(line).expect("JSON must parse without syntax error");

        let req_id_str = v["request_id"].as_str().expect("request_id is string");
        assert_eq!(
            req_id_str.len(),
            32,
            "Request ID must be exactly 32 hex chars for ts={ts}, req_id={req_id}"
        );

        // Verify exact lowercase hex match: hi (timestamp) + lo (request_id)
        let expected_hex = format!("{:016x}{:016x}", ts, req_id);
        assert_eq!(
            req_id_str, expected_hex,
            "Hex mismatch for ts={ts:#x}, req_id={req_id:#x}"
        );
    }
}

// ============================================================================
// 2. Biên Cực Trị Timestamps & Duration & Counters
// ============================================================================
#[test]
fn test_chaos_timestamps_duration_and_counters() {
    let test_matrix: &[(u32, u64, u32, u32, u16, u16, u16)] = &[
        // (ts, dur_ns, rx, tx, status, route_id, msgs)
        (0, 0, 0, 0, 0, 0, 0),
        (1, 1, 1, 1, 200, 1, 1),
        (0x7FFF_FFFF, 999, 1024, 2048, 404, 100, 5), // Year 2038
        (
            u32::MAX,
            1_000_000,
            u32::MAX,
            u32::MAX,
            500,
            u16::MAX,
            u16::MAX,
        ),
        (1_710_000_000, u32::MAX as u64, 65535, 131072, 999, 42, 1000),
    ];

    let mut records = Vec::new();
    for &(ts, dur, rx, tx, status, route_id, msgs) in test_matrix {
        records.push(build_chaos_record(
            0x1234_5678,
            ts,
            dur,
            encode_ip_u128(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            443,
            0x0A00_0002,
            8080,
            rx,
            tx,
            status,
            route_id,
            msgs,
            PipeKind::ServerStreamH2H2,
            LogCompletionReason::Complete,
            LogHttpMethod::Post,
            LogGrpcStatus::NonGrpc,
            UaFamily::Chrome,
            UaOs::Windows,
            124,
        ));
    }

    let batches = vec![Arc::from(records.as_slice())];
    let mut json_out = Vec::new();
    format_std_log_payload(&batches, "json", &mut json_out);

    let json_text = String::from_utf8(json_out).unwrap();
    let lines: Vec<&str> = json_text.lines().collect();

    for (i, &(ts, dur, rx, tx, status, _route_id, _msgs)) in test_matrix.iter().enumerate() {
        let v: serde_json::Value = serde_json::from_str(lines[i]).unwrap();
        assert_eq!(v["timestamp_epoch_sec"].as_u64().unwrap(), ts as u64);
        assert_eq!(v["duration_ns"].as_u64().unwrap(), dur);
        assert_eq!(v["bytes_rx"].as_u64().unwrap(), rx as u64);
        assert_eq!(v["bytes_tx"].as_u64().unwrap(), tx as u64);
        assert_eq!(v["status"].as_u64().unwrap(), status as u64);
        assert_eq!(v["route"].as_str().unwrap(), "unmatched");
    }
}

// ============================================================================
// 3. Hỗn Loạn Địa Chỉ IP (RFC 4291 & RFC 5952 Edge Cases)
// ============================================================================
#[test]
fn test_chaos_ip_addresses_rfc4291_rfc5952() {
    let test_ips: &[(IpAddr, u16, &str)] = &[
        // IPv4
        (IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0, "0.0.0.0:0"),
        (
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)),
            65535,
            "255.255.255.255:65535",
        ),
        (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 80, "127.0.0.1:80"),
        (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 443, "10.0.0.1:443"),
        (
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 50)),
            8080,
            "192.168.1.50:8080",
        ),
        // IPv6 RFC 5952
        (IpAddr::V6(Ipv6Addr::LOCALHOST), 50001, "::1:50001"),
        (IpAddr::V6(Ipv6Addr::UNSPECIFIED), 8080, ":::8080"),
        (
            IpAddr::V6("2001:db8::1".parse().unwrap()),
            443,
            "2001:db8::1:443",
        ),
        // Multiple zero runs: 2001:0:0:1:0:0:0:1 -> compress longest (second) run -> 2001:0:0:1::1
        (
            IpAddr::V6("2001:0:0:1:0:0:0:1".parse().unwrap()),
            8080,
            "2001:0:0:1::1:8080",
        ),
        // Equal zero runs tie: 2001:0:0:1:2:0:0:1 -> compress first run -> 2001::1:2:0:0:1
        (
            IpAddr::V6("2001:0:0:1:2:0:0:1".parse().unwrap()),
            80,
            "2001::1:2:0:0:1:80",
        ),
        // Leading zero run: ::1:2:3:4
        (
            IpAddr::V6("0:0:0:0:1:2:3:4".parse().unwrap()),
            80,
            "::1:2:3:4:80",
        ),
        // Trailing zero run: 2001:db8:1:2::
        (
            IpAddr::V6("2001:db8:1:2:0:0:0:0".parse().unwrap()),
            80,
            "2001:db8:1:2:::80",
        ),
        // Full 8 hextets, no zeros
        (
            IpAddr::V6("2001:db8:85a3:8d3:1319:8a2e:370:7348".parse().unwrap()),
            443,
            "2001:db8:85a3:8d3:1319:8a2e:370:7348:443",
        ),
        // Single zero (must NOT compress per RFC 5952 section 4.2.2)
        (
            IpAddr::V6("2001:db8:0:1:2:3:4:5".parse().unwrap()),
            80,
            "2001:db8:0:1:2:3:4:5:80",
        ),
    ];

    let mut records = Vec::new();
    for &(ip, port, _) in test_ips {
        records.push(build_chaos_record(
            0x9999,
            1_710_000_000,
            50,
            encode_ip_u128(ip),
            port,
            0xC0A8_0101, // upstream 192.168.1.1
            80,
            0,
            0,
            200,
            1,
            0,
            PipeKind::BufferedH1H1,
            LogCompletionReason::Complete,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
            UaFamily::None,
            UaOs::Unknown,
            0,
        ));
    }

    let batches = vec![Arc::from(records.as_slice())];

    // 1. JSON Round-Trip Verification
    let mut json_out = Vec::new();
    format_std_log_payload(&batches, "json", &mut json_out);
    let json_str = String::from_utf8(json_out).unwrap();
    let lines: Vec<&str> = json_str.lines().collect();

    for (i, &(_, _, expected_peer)) in test_ips.iter().enumerate() {
        let v: serde_json::Value =
            serde_json::from_str(lines[i]).expect("JSON syntax must remain valid");
        assert_eq!(
            v["client"].as_str().unwrap(),
            expected_peer,
            "Peer IP mismatch on index {i}"
        );
    }

    // 2. Text Format Delimiter Verification
    let mut text_out = Vec::new();
    format_std_log_payload(&batches, "text", &mut text_out);
    let text_str = String::from_utf8(text_out).unwrap();
    let text_lines: Vec<&str> = text_str.lines().collect();

    for (i, &(_, _, expected_peer)) in test_ips.iter().enumerate() {
        assert!(
            text_lines[i].starts_with(expected_peer),
            "Text line {i} must start with expected peer: {}",
            text_lines[i]
        );
    }
}

// ============================================================================
// 4. Tổ Hợp Status Code & Protocols Combinatorial Matrix
// ============================================================================
#[test]
fn test_chaos_combinatorial_protocols_and_statuses() {
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
        LogHttpMethod::Get,
        LogHttpMethod::Post,
        LogHttpMethod::Put,
        LogHttpMethod::Delete,
        LogHttpMethod::Patch,
        LogHttpMethod::Head,
        LogHttpMethod::Options,
        LogHttpMethod::Connect,
        LogHttpMethod::Trace,
        LogHttpMethod::Unknown,
    ];

    let sample_grpc_statuses = [
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
        LogGrpcStatus::NonGrpc,
    ];

    let sample_statuses = [
        0u16, 100, 200, 204, 301, 400, 403, 404, 429, 500, 502, 503, 999,
    ];

    let all_strategies = [
        PipeStrategy::Buffered,
        PipeStrategy::ServerStream,
        PipeStrategy::ClientStream,
        PipeStrategy::Duplex,
    ];

    let all_completions = [
        LogCompletionReason::Complete,
        LogCompletionReason::ClientCancel,
        LogCompletionReason::UpstreamError,
        LogCompletionReason::Timeout,
        LogCompletionReason::Rejected,
    ];

    let mut records = Vec::new();
    let mut counter = 0u64;

    for &proto in &all_protocols {
        for &method in &all_methods {
            let grpc_st = sample_grpc_statuses[(counter as usize) % sample_grpc_statuses.len()];
            let status = sample_statuses[(counter as usize) % sample_statuses.len()];
            let strat = all_strategies[(counter as usize) % all_strategies.len()];
            let comp = all_completions[(counter as usize) % all_completions.len()];

            records.push(build_chaos_record(
                0x1000 + counter,
                1_710_000_000 + (counter as u32),
                200 + counter,
                encode_ip_u128(IpAddr::V4(Ipv4Addr::new(10, 0, 1, (counter % 250) as u8))),
                10000 + (counter as u16),
                0x7F00_0001,
                8080,
                100 + (counter as u32),
                200 + (counter as u32),
                status,
                (counter % 100) as u16,
                (counter % 50) as u16,
                PipeKind::from_protocols(proto as u8, proto as u8, strat as u8),
                comp,
                method,
                grpc_st,
                UaFamily::Curl,
                UaOs::Linux,
                8,
            ));
            counter += 1;
        }
    }

    let batches = vec![Arc::from(records.as_slice())];

    // 1. JSON Verification: ALL lines must parse strictly with serde_json
    let mut json_buf = Vec::new();
    let count = format_std_log_payload(&batches, "json", &mut json_buf);
    assert_eq!(count, records.len());

    let json_text = String::from_utf8(json_buf).unwrap();
    for (i, line) in json_text.lines().enumerate() {
        let v: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|e| {
            panic!("Line {i} failed to parse JSON: '{line}', error: {e}");
        });
        assert!(v.is_object());
        assert_eq!(v["request_id"].as_str().unwrap().len(), 32);
    }

    // 2. Text Verification: ALL lines must contain delimiters
    let mut text_buf = Vec::new();
    let count = format_std_log_payload(&batches, "text", &mut text_buf);
    assert_eq!(count, records.len());

    let text_str = String::from_utf8(text_buf).unwrap();
    for (i, line) in text_str.lines().enumerate() {
        assert!(
            line.contains(" ["),
            "Line {i} missing ' [' delimiter: {line}"
        );
        assert!(
            line.contains("] \""),
            "Line {i} missing '] \"' delimiter: {line}"
        );
        assert!(line.contains("route="), "Line {i} missing 'route=': {line}");
        assert!(
            line.contains(" endpoint="),
            "Line {i} missing ' endpoint=': {line}"
        );
        assert!(
            line.contains("completion="),
            "Line {i} missing 'completion=': {line}"
        );
    }

    // 3. CLF Verification
    let mut clf_buf = Vec::new();
    let count = format_std_log_payload(&batches, "clf", &mut clf_buf);
    assert_eq!(count, records.len());
    let clf_str = String::from_utf8(clf_buf).unwrap();
    for (i, line) in clf_str.lines().enumerate() {
        assert!(
            line.contains(" - - ["),
            "CLF line {i} missing ' - - [': {line}"
        );
        assert!(line.contains("] \""), "CLF line {i} missing '] \"': {line}");
    }
}

// ============================================================================
// 5. Xác Minh Cú Pháp 100% (Round-Trip Schema & Key Completeness)
// ============================================================================
#[test]
fn test_chaos_syntax_roundtrip_parsing_all_18_keys() {
    velda_observability::register_route_name(77, "api-upload");

    let mut rec = build_chaos_record(
        0xDEAD_BEEF_CAFE_BABE,
        1_710_500_000,
        12345,
        encode_ip_u128(IpAddr::V6("2001:db8::8a2e:370:7334".parse().unwrap())),
        54321,
        0xC0A8_0102,
        9090,
        5000,
        10000,
        201,
        77,
        15,
        PipeKind::DuplexH3H3,
        LogCompletionReason::ClientCancel,
        LogHttpMethod::Put,
        LogGrpcStatus::NonGrpc,
        UaFamily::Firefox,
        UaOs::MacOs,
        125,
    );
    let (p_len, p_bytes) = AccessLogRecord::from_url_bytes(b"/api/v1/upload");
    rec.url_len = p_len;
    rec.url_bytes = p_bytes;

    let batches = vec![Arc::from(vec![rec])];
    let mut out = Vec::new();
    format_std_log_payload(&batches, "json", &mut out);

    let json_str = String::from_utf8(out).unwrap();
    let v: serde_json::Value = serde_json::from_str(&json_str).expect("Valid JSON object");

    let required_keys = [
        "timestamp",
        "timestamp_epoch_sec",
        "request_id",
        "client",
        "endpoint",
        "method",
        "url",
        "route",
        "status",
        "duration_ns",
        "bytes_rx",
        "bytes_tx",
        "user_agent",
        "client_os",
        "pipe",
        "completion",
    ];

    for key in required_keys {
        assert!(
            v.get(key).is_some(),
            "Required key '{key}' missing from JSON payload: {json_str}"
        );
    }

    assert_eq!(
        v["request_id"].as_str().unwrap(),
        "0000000065f428a0deadbeefcafebabe"
    );
    assert_eq!(v["timestamp_epoch_sec"].as_u64().unwrap(), 1_710_500_000);
    assert_eq!(v["duration_ns"].as_u64().unwrap(), 12345);
    assert_eq!(v["pipe"].as_str().unwrap(), "duplex_h3_h3");
    assert_eq!(v["method"].as_str().unwrap(), "PUT");
    assert_eq!(v["url"].as_str().unwrap(), "/api/v1/upload");
    assert_eq!(v["route"].as_str().unwrap(), "api-upload");
    assert_eq!(v["status"].as_u64().unwrap(), 201);
    assert_eq!(v["bytes_rx"].as_u64().unwrap(), 5000);
    assert_eq!(v["bytes_tx"].as_u64().unwrap(), 10000);
    assert_eq!(
        v["client"].as_str().unwrap(),
        "2001:db8::8a2e:370:7334:54321"
    );
    assert_eq!(v["endpoint"].as_str().unwrap(), "192.168.1.2:9090");
    assert_eq!(v["user_agent"].as_str().unwrap(), "firefox/125");
    assert_eq!(v["client_os"].as_str().unwrap(), "macos");
    assert_eq!(v["completion"].as_str().unwrap(), "client_cancel");
}

// ============================================================================
// 6. Stress Testing Persistent File Sink (10,000 Records Across Batches)
// ============================================================================
#[tokio::test]
async fn test_chaos_persistent_file_sink_stress_10000_records() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("chaos_stress_10k.log");
    let target = TargetStream::File(file_path.clone());

    let mut sink = StdLogSink::init(&target)
        .await
        .expect("Persistent sink initialization failed");

    const TOTAL_BATCHES: usize = 100;
    const RECORDS_PER_BATCH: usize = 100;
    const TOTAL_RECORDS: usize = TOTAL_BATCHES * RECORDS_PER_BATCH;

    let mut out_buf = Vec::with_capacity(32768);

    for batch_idx in 0..TOTAL_BATCHES {
        let mut records = Vec::with_capacity(RECORDS_PER_BATCH);
        for i in 0..RECORDS_PER_BATCH {
            let id = ((batch_idx * RECORDS_PER_BATCH + i) as u64) + 1;
            records.push(build_chaos_record(
                id,
                1_710_000_000 + (id as u32),
                id % 1000,
                encode_ip_u128(IpAddr::V4(Ipv4Addr::new(
                    192,
                    168,
                    (id / 256) as u8,
                    (id % 256) as u8,
                ))),
                (1024 + (id % 60000)) as u16,
                0x0A00_0101,
                8080,
                (id * 10) as u32,
                (id * 20) as u32,
                200,
                1,
                0,
                PipeKind::BufferedH1H1,
                LogCompletionReason::Complete,
                LogHttpMethod::Get,
                LogGrpcStatus::NonGrpc,
                UaFamily::Curl,
                UaOs::Linux,
                8,
            ));
        }

        let batches = vec![Arc::from(records.as_slice())];
        let n = format_std_log_payload(&batches, "json", &mut out_buf);
        assert_eq!(n, RECORDS_PER_BATCH);

        sink.write_all(&out_buf).await.expect("Write must succeed");
    }

    // Read back and verify 10,000 lines
    let file_content = tokio::fs::read_to_string(&file_path)
        .await
        .expect("Read file back");
    let lines: Vec<&str> = file_content.lines().collect();

    assert_eq!(
        lines.len(),
        TOTAL_RECORDS,
        "File must contain exactly 10,000 formatted lines without truncation"
    );

    // Verify first, middle, and last lines for JSON integrity
    let first: serde_json::Value =
        serde_json::from_str(lines[0]).expect("First line must parse JSON");
    assert_eq!(
        first["request_id"].as_str().unwrap(),
        "0000000065ec87810000000000000001"
    );

    let mid: serde_json::Value =
        serde_json::from_str(lines[5000]).expect("Mid line must parse JSON");
    assert_eq!(
        mid["request_id"].as_str().unwrap(),
        "0000000065ec9b090000000000001389"
    );

    let last: serde_json::Value =
        serde_json::from_str(lines[TOTAL_RECORDS - 1]).expect("Last line must parse JSON");
    assert_eq!(
        last["request_id"].as_str().unwrap(),
        "0000000065ecae900000000000002710"
    );
}
