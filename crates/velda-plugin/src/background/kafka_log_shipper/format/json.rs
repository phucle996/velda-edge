//! Zero-Allocation JSON Access Log Formatting (`kafka_log_shipper::format::json`).
//!
//! Serializes [`TelemetryRecord`] into compact single-line JSON records using
//! pre-baked structural byte slices and direct lookup table (LUT) formatting.

use velda_core::lut::decimal::{fast_write_u16_ptr, fast_write_u64_ptr};
use velda_core::lut::ip::{fast_write_ip_ptr, fast_write_ipv4_ptr};
use velda_observability::TelemetryRecord;

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

#[inline]
unsafe fn write_json_escaped_ptr(ptr: *mut u8, s: &[u8]) -> usize {
    let mut p = ptr;
    let mut i = 0;
    let len = s.len();

    // 64-bit register SWAR fast path: process 8 bytes per iteration when all bytes are clean ASCII
    while i + 8 <= len {
        let chunk = unsafe { (s.as_ptr().add(i) as *const u64).read_unaligned() };

        // Check if any byte is < 0x20:
        let sub20 = chunk.wrapping_sub(0x2020_2020_2020_2020);
        let has_ctrl = (sub20 & !chunk) & 0x8080_8080_8080_8080;

        // Check if any byte == '"' (0x22):
        let xor_quote = chunk ^ 0x2222_2222_2222_2222;
        let has_quote =
            (xor_quote.wrapping_sub(0x0101_0101_0101_0101) & !xor_quote) & 0x8080_8080_8080_8080;

        // Check if any byte == '\\' (0x5C):
        let xor_slash = chunk ^ 0x5C5C_5C5C_5C5C_5C5C;
        let has_slash =
            (xor_slash.wrapping_sub(0x0101_0101_0101_0101) & !xor_slash) & 0x8080_8080_8080_8080;

        if (has_ctrl | has_quote | has_slash) == 0 {
            // All 8 bytes are safe printable ASCII: write all 8 bytes directly in 1 store!
            unsafe {
                (p as *mut u64).write_unaligned(chunk);
                p = p.add(8);
            }
            i += 8;
        } else {
            break;
        }
    }

    // Byte-by-byte escape loop for remaining bytes or blocks containing special characters
    while i < len {
        let b = unsafe { *s.as_ptr().add(i) };
        unsafe {
            match b {
                b'"' => {
                    *p = b'\\';
                    *p.add(1) = b'"';
                    p = p.add(2);
                }
                b'\\' => {
                    *p = b'\\';
                    *p.add(1) = b'\\';
                    p = p.add(2);
                }
                b'\n' => {
                    *p = b'\\';
                    *p.add(1) = b'n';
                    p = p.add(2);
                }
                b'\r' => {
                    *p = b'\\';
                    *p.add(1) = b'r';
                    p = p.add(2);
                }
                b'\t' => {
                    *p = b'\\';
                    *p.add(1) = b't';
                    p = p.add(2);
                }
                0..=0x1F => {
                    *p = b'\\';
                    *p.add(1) = b'u';
                    *p.add(2) = b'0';
                    *p.add(3) = b'0';
                    *p.add(4) = HEX_DIGITS[(b >> 4) as usize];
                    *p.add(5) = HEX_DIGITS[(b & 0xF) as usize];
                    p = p.add(6);
                }
                _ => {
                    *p = b;
                    p = p.add(1);
                }
            }
        }
        i += 1;
    }
    p as usize - ptr as usize
}

// Prebaked structural JSON field prefixes and separators
const JSON_PREFIX: &[u8] = b"{\"request_id\":";
const JSON_DURATION: &[u8] = b",\"duration_ns\":";
const JSON_PEER_IP: &[u8] = b",\"peer_ip\":\"";
const JSON_INGRESS_TIME: &[u8] = b"\",\"ingress_time_nanos\":";
const JSON_BYTES_RX: &[u8] = b",\"bytes_rx\":";
const JSON_BYTES_TX: &[u8] = b",\"bytes_tx\":";
const JSON_UPSTREAM_IP: &[u8] = b",\"upstream_ip\":\"";
const JSON_UPSTREAM_PORT: &[u8] = b"\",\"upstream_port\":";
const JSON_ROUTE_ID: &[u8] = b",\"route_id\":";
const JSON_STATUS: &[u8] = b",\"status_code\":";
const JSON_PIPE: &[u8] = b",\"pipe\":\"";
const JSON_METHOD: &[u8] = b"\",\"method\":\"";
const JSON_URL: &[u8] = b"\",\"url\":\"";
const JSON_COMPLETION: &[u8] = b"\",\"completion_reason\":\"";
const JSON_GRPC_STATUS: &[u8] = b"\",\"grpc_status\":\"";
const JSON_UA_FAMILY: &[u8] = b"\",\"user_agent_family\":\"";
const JSON_UA_OS: &[u8] = b"\",\"user_agent_os\":\"";
const JSON_SUFFIX: &[u8] = b"\"}";

/// Serializes an individual [`TelemetryRecord`] into a compact JSON record.
///
/// Guaranteed zero heap allocations per record via direct LUT pointer stamping.
pub fn format_single_record_json(rec: &TelemetryRecord, out: &mut Vec<u8>) {
    let start_len = out.len();
    out.reserve(512);

    unsafe {
        let base_ptr = out.as_mut_ptr();
        let mut ptr = base_ptr.add(start_len);

        // 1. `{"request_id":`
        core::ptr::copy_nonoverlapping(JSON_PREFIX.as_ptr(), ptr, JSON_PREFIX.len());
        ptr = ptr.add(JSON_PREFIX.len());

        // 2. request_id
        let r_len = fast_write_u64_ptr(ptr, rec.request_id);
        ptr = ptr.add(r_len);

        // 3. `,"duration_ns":`
        core::ptr::copy_nonoverlapping(JSON_DURATION.as_ptr(), ptr, JSON_DURATION.len());
        ptr = ptr.add(JSON_DURATION.len());

        // 4. duration_ns
        let d_len = fast_write_u64_ptr(ptr, rec.duration_ns);
        ptr = ptr.add(d_len);

        // 5. `,"peer_ip":"`
        core::ptr::copy_nonoverlapping(JSON_PEER_IP.as_ptr(), ptr, JSON_PEER_IP.len());
        ptr = ptr.add(JSON_PEER_IP.len());

        // 6. peer_ip (IPv4 or IPv6)
        let ip_len = fast_write_ip_ptr(ptr, rec.peer_ip);
        ptr = ptr.add(ip_len);

        // 7. `","ingress_time_nanos":`
        core::ptr::copy_nonoverlapping(JSON_INGRESS_TIME.as_ptr(), ptr, JSON_INGRESS_TIME.len());
        ptr = ptr.add(JSON_INGRESS_TIME.len());

        // 8. ingress_time_nanos
        let t_len = fast_write_u64_ptr(ptr, rec.ingress_time_nanos);
        ptr = ptr.add(t_len);

        // 9. `,"bytes_rx":`
        core::ptr::copy_nonoverlapping(JSON_BYTES_RX.as_ptr(), ptr, JSON_BYTES_RX.len());
        ptr = ptr.add(JSON_BYTES_RX.len());

        let rx_len = fast_write_u64_ptr(ptr, rec.bytes_rx as u64);
        ptr = ptr.add(rx_len);

        // 10. `,"bytes_tx":`
        core::ptr::copy_nonoverlapping(JSON_BYTES_TX.as_ptr(), ptr, JSON_BYTES_TX.len());
        ptr = ptr.add(JSON_BYTES_TX.len());

        let tx_len = fast_write_u64_ptr(ptr, rec.bytes_tx as u64);
        ptr = ptr.add(tx_len);

        // 10b. `,"upstream_ip":"`
        core::ptr::copy_nonoverlapping(JSON_UPSTREAM_IP.as_ptr(), ptr, JSON_UPSTREAM_IP.len());
        ptr = ptr.add(JSON_UPSTREAM_IP.len());

        let u_ip_len = fast_write_ipv4_ptr(ptr, rec.upstream_ip);
        ptr = ptr.add(u_ip_len);

        // 11. `","upstream_port":`
        core::ptr::copy_nonoverlapping(JSON_UPSTREAM_PORT.as_ptr(), ptr, JSON_UPSTREAM_PORT.len());
        ptr = ptr.add(JSON_UPSTREAM_PORT.len());

        // 12. upstream_port
        let p_len = fast_write_u16_ptr(ptr, rec.upstream_port);
        ptr = ptr.add(p_len);

        // 13. `,"route_id":`
        core::ptr::copy_nonoverlapping(JSON_ROUTE_ID.as_ptr(), ptr, JSON_ROUTE_ID.len());
        ptr = ptr.add(JSON_ROUTE_ID.len());

        // 14. route_id
        let r_len = fast_write_u16_ptr(ptr, rec.route_id);
        ptr = ptr.add(r_len);

        // 15. `,"status_code":`
        core::ptr::copy_nonoverlapping(JSON_STATUS.as_ptr(), ptr, JSON_STATUS.len());
        ptr = ptr.add(JSON_STATUS.len());

        // 16. status_code
        let s_len = fast_write_u16_ptr(ptr, rec.status_code);
        ptr = ptr.add(s_len);

        // 17. `,"pipe":"`
        core::ptr::copy_nonoverlapping(JSON_PIPE.as_ptr(), ptr, JSON_PIPE.len());
        ptr = ptr.add(JSON_PIPE.len());

        // 18. pipe string
        let pipe_bytes = rec.pipe.as_bytes();
        core::ptr::copy_nonoverlapping(pipe_bytes.as_ptr(), ptr, pipe_bytes.len());
        ptr = ptr.add(pipe_bytes.len());

        // 19. `","method":"`
        core::ptr::copy_nonoverlapping(JSON_METHOD.as_ptr(), ptr, JSON_METHOD.len());
        ptr = ptr.add(JSON_METHOD.len());

        // 20. method string
        let m_bytes = rec.method.as_bytes();
        core::ptr::copy_nonoverlapping(m_bytes.as_ptr(), ptr, m_bytes.len());
        ptr = ptr.add(m_bytes.len());

        // 21. `","url":"`
        core::ptr::copy_nonoverlapping(JSON_URL.as_ptr(), ptr, JSON_URL.len());
        ptr = ptr.add(JSON_URL.len());

        // 22. url (escaped)
        let url_len = write_json_escaped_ptr(ptr, rec.url().as_bytes());
        ptr = ptr.add(url_len);

        // 25. `","completion_reason":"`
        core::ptr::copy_nonoverlapping(JSON_COMPLETION.as_ptr(), ptr, JSON_COMPLETION.len());
        ptr = ptr.add(JSON_COMPLETION.len());

        // 26. completion_reason string
        let c_bytes = rec.completion.as_bytes();
        core::ptr::copy_nonoverlapping(c_bytes.as_ptr(), ptr, c_bytes.len());
        ptr = ptr.add(c_bytes.len());

        // 27. `","grpc_status":"`
        core::ptr::copy_nonoverlapping(JSON_GRPC_STATUS.as_ptr(), ptr, JSON_GRPC_STATUS.len());
        ptr = ptr.add(JSON_GRPC_STATUS.len());

        // 28. grpc_status string
        let g_bytes = rec.grpc_status.as_bytes();
        core::ptr::copy_nonoverlapping(g_bytes.as_ptr(), ptr, g_bytes.len());
        ptr = ptr.add(g_bytes.len());

        // 31. `","user_agent_family":"`
        core::ptr::copy_nonoverlapping(JSON_UA_FAMILY.as_ptr(), ptr, JSON_UA_FAMILY.len());
        ptr = ptr.add(JSON_UA_FAMILY.len());

        // 32. user_agent_family string
        let ua_f_bytes = rec.ua_family.as_bytes();
        core::ptr::copy_nonoverlapping(ua_f_bytes.as_ptr(), ptr, ua_f_bytes.len());
        ptr = ptr.add(ua_f_bytes.len());

        // 33. `","user_agent_os":"`
        core::ptr::copy_nonoverlapping(JSON_UA_OS.as_ptr(), ptr, JSON_UA_OS.len());
        ptr = ptr.add(JSON_UA_OS.len());

        // 34. user_agent_os string
        let ua_os_bytes = rec.ua_os.as_bytes();
        core::ptr::copy_nonoverlapping(ua_os_bytes.as_ptr(), ptr, ua_os_bytes.len());
        ptr = ptr.add(ua_os_bytes.len());

        // 35. Suffix: `"}`
        core::ptr::copy_nonoverlapping(JSON_SUFFIX.as_ptr(), ptr, JSON_SUFFIX.len());
        ptr = ptr.add(JSON_SUFFIX.len());

        let total_written = ptr as usize - (base_ptr as usize + start_len);
        out.set_len(start_len + total_written);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs};

    #[test]
    fn test_format_single_record_json() {
        let mut rec = TelemetryRecord::zeroed();
        rec.request_id = 12345;
        rec.duration_ns = 500_000;
        rec.peer_ip = 0x0000_0000_0000_0000_0000_FFFF_7F00_0001; // 127.0.0.1
        rec.ingress_time_nanos = 1_700_000_000_000_000_000;
        rec.status_code = 200;
        rec.pipe = PipeKind::BufferedH1H1;
        rec.method = HttpMethod::Get;
        rec.completion = CompletionReason::Complete;
        rec.grpc_status = GrpcStatus::Ok;
        rec.ua_family = UaFamily::Curl;
        rec.ua_os = UaOs::Linux;
        rec.set_url("/api/v1/resource?id=10&name=\"test\"");

        let mut out = Vec::new();
        format_single_record_json(&rec, &mut out);

        let parsed: serde_json::Value =
            serde_json::from_slice(&out).expect("Must produce valid JSON");
        assert_eq!(parsed["request_id"], 12345);
        assert_eq!(parsed["status_code"], 200);
        assert_eq!(parsed["method"], "GET");
        assert_eq!(parsed["peer_ip"], "127.0.0.1");
        assert_eq!(parsed["url"], "/api/v1/resource?id=10&name=\"test\"");
    }
}
