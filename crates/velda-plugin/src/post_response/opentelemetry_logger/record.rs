//! Cacheline-Aligned 256-Byte OpenTelemetry Log Record (`opentelemetry_logger::record`).
//!
//! Provides a zero-allocation, cacheline-aligned (256-byte) POD record structure (`PackedOtlpRecord`)
//! designed for non-temporal streaming writes to RAM, lock-free batch transport, and zero-allocation
//! OTLP JSON / Protobuf serialization on Shadow Worker threads.
//!
//! # Micro-Architectural Guarantees
//! - **Exact 256 Bytes**: Exactly 4 CPU cache lines (64 bytes each).
//! - **Cacheline Aligned**: `#[repr(C, align(64))]` enables aligned vector loads and `_mm256_stream_si256`.
//! - **Zero Heap Allocation**: Pure POD layout, zero pointers, zero dynamic heap allocations.

use itoa::Buffer as ItoaBuf;
use std::fmt::Write as _;

/// Ultra-compact 256-byte cacheline-aligned OTLP log record.
///
/// # Layout (256 bytes total, 64-byte aligned across 4 cache lines)
/// - **Cache Line 1 (0..64)**: Integer scalars, status, method, trace/span binary IDs
///   - `0..8`: `time_unix_nano: u64`
///   - `8..12`: `duration_us: u32`
///   - `12..14`: `status: u16`
///   - `14..15`: `method: u8` (1=GET, 2=POST, 3=PUT, 4=DELETE, 5=HEAD, 6=OPTIONS, 7=PATCH, 0=Other)
///   - `15..16`: `ip_kind: u8` (0=None, 4=IPv4, 6=IPv6)
///   - `16..17`: `trace_id_len: u8` (max 16)
///   - `17..18`: `span_id_len: u8` (max 8)
///   - `18..19`: `route_id_len: u8` (max 24)
///   - `19..20`: `upstream_len: u8` (max 24)
///   - `20..21`: `path_len: u8` (max 144)
///   - `21..24`: `_reserved: [u8; 3]`
///   - `24..40`: `trace_id: [u8; 16]` (binary UUID / 128-bit trace ID)
///   - `40..48`: `span_id: [u8; 8]` (binary 64-bit span ID)
///   - `48..64`: `client_ip: [u8; 16]` (IPv4 mapped in first 4 bytes or full IPv6)
/// - **Cache Line 2 & 3 (64..112)**: Route ID and Upstream target address
///   - `64..88`: `route_id: [u8; 24]`
///   - `88..112`: `upstream: [u8; 24]`
/// - **Cache Line 3 & 4 (112..256)**: Inline URL Path
///   - `112..256`: `path: [u8; 144]` (144 bytes inline ASCII path)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackedOtlpRecord {
    // --- Cache Line 1 (0..64) ---
    pub time_unix_nano: u64, // 8 bytes (0..8)
    pub duration_us: u32,    // 4 bytes (8..12)
    pub status: u16,         // 2 bytes (12..14)
    pub method: u8,          // 1 byte  (14..15)
    pub ip_kind: u8,         // 1 byte  (15..16)
    pub trace_id_len: u8,    // 1 byte  (16..17)
    pub span_id_len: u8,     // 1 byte  (17..18)
    pub route_id_len: u8,    // 1 byte  (18..19)
    pub upstream_len: u8,    // 1 byte  (19..20)
    pub path_len: u8,        // 1 byte  (20..21)
    pub _reserved: [u8; 3],  // 3 bytes (21..24)
    pub trace_id: [u8; 16],  // 16 bytes (24..40)
    pub span_id: [u8; 8],    // 8 bytes (40..48)
    pub client_ip: [u8; 16], // 16 bytes (48..64)
    // --- Cache Line 2 & 3 (64..112) ---
    pub route_id: [u8; 24], // 24 bytes (64..88)
    pub upstream: [u8; 24], // 24 bytes (88..112)
    // --- Cache Line 3 & 4 (112..256) ---
    pub path: [u8; 144], // 144 bytes (112..256)
}

const _: () = assert!(std::mem::size_of::<PackedOtlpRecord>() == 256);
const _: () = assert!(std::mem::align_of::<PackedOtlpRecord>() == 64);

impl Default for PackedOtlpRecord {
    #[inline(always)]
    fn default() -> Self {
        Self::empty()
    }
}

impl PackedOtlpRecord {
    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            time_unix_nano: 0,
            duration_us: 0,
            status: 0,
            method: 0,
            ip_kind: 0,
            trace_id_len: 0,
            span_id_len: 0,
            route_id_len: 0,
            upstream_len: 0,
            path_len: 0,
            _reserved: [0; 3],
            trace_id: [0; 16],
            span_id: [0; 8],
            client_ip: [0; 16],
            route_id: [0; 24],
            upstream: [0; 24],
            path: [0; 144],
        }
    }

    #[inline(always)]
    pub fn severity(&self) -> (u8, &'static str) {
        match self.status {
            100..=399 => (9, "INFO"),
            400..=499 => (13, "WARN"),
            _ => (17, "ERROR"),
        }
    }

    #[inline(always)]
    pub fn method_str(&self) -> &'static str {
        match self.method {
            1 => "GET",
            2 => "POST",
            3 => "PUT",
            4 => "DELETE",
            5 => "HEAD",
            6 => "OPTIONS",
            7 => "PATCH",
            _ => "GET",
        }
    }

    #[inline(always)]
    pub fn encode_method(m: &http::Method) -> u8 {
        match *m {
            http::Method::GET => 1,
            http::Method::POST => 2,
            http::Method::PUT => 3,
            http::Method::DELETE => 4,
            http::Method::HEAD => 5,
            http::Method::OPTIONS => 6,
            http::Method::PATCH => 7,
            _ => 0,
        }
    }

    #[inline(always)]
    pub fn set_trace_id(&mut self, src: &[u8]) {
        let len = src.len().min(16);
        self.trace_id[..len].copy_from_slice(&src[..len]);
        self.trace_id_len = len as u8;
    }

    #[inline(always)]
    pub fn set_span_id(&mut self, src: &[u8]) {
        let len = src.len().min(8);
        self.span_id[..len].copy_from_slice(&src[..len]);
        self.span_id_len = len as u8;
    }

    #[inline(always)]
    pub fn set_route_id(&mut self, src: &[u8]) {
        let len = src.len().min(24);
        self.route_id[..len].copy_from_slice(&src[..len]);
        self.route_id_len = len as u8;
    }

    #[inline(always)]
    pub fn set_upstream(&mut self, src: &[u8]) {
        let len = src.len().min(24);
        self.upstream[..len].copy_from_slice(&src[..len]);
        self.upstream_len = len as u8;
    }

    #[inline(always)]
    pub fn set_path(&mut self, src: &[u8]) {
        let len = src.len().min(144);
        self.path[..len].copy_from_slice(&src[..len]);
        self.path_len = len as u8;
    }

    #[inline(always)]
    pub fn set_client_ip(&mut self, ip: std::net::IpAddr) {
        match ip {
            std::net::IpAddr::V4(v4) => {
                self.ip_kind = 4;
                self.client_ip[..4].copy_from_slice(&v4.octets());
            }
            std::net::IpAddr::V6(v6) => {
                self.ip_kind = 6;
                self.client_ip.copy_from_slice(&v6.octets());
            }
        }
    }

    #[inline(always)]
    pub fn trace_id(&self) -> &[u8] {
        &self.trace_id[..self.trace_id_len as usize]
    }

    #[inline(always)]
    pub fn span_id(&self) -> &[u8] {
        &self.span_id[..self.span_id_len as usize]
    }

    #[inline(always)]
    pub fn route_id(&self) -> &str {
        std::str::from_utf8(&self.route_id[..self.route_id_len as usize]).unwrap_or("")
    }

    #[inline(always)]
    pub fn upstream(&self) -> &str {
        std::str::from_utf8(&self.upstream[..self.upstream_len as usize]).unwrap_or("")
    }

    #[inline(always)]
    pub fn path(&self) -> &str {
        let p = std::str::from_utf8(&self.path[..self.path_len as usize]).unwrap_or("");
        if p.is_empty() { "/" } else { p }
    }

    #[inline(always)]
    pub fn format_client_ip(&self, buf: &mut String) {
        match self.ip_kind {
            4 => {
                let _ = write!(
                    buf,
                    "{}.{}.{}.{}",
                    self.client_ip[0], self.client_ip[1], self.client_ip[2], self.client_ip[3]
                );
            }
            6 => {
                let v6 = std::net::Ipv6Addr::from(self.client_ip);
                let _ = write!(buf, "{v6}");
            }
            _ => {}
        }
    }
}

/// Decodes an ASCII hex slice into raw bytes in `out`.
/// Returns the number of bytes written.
pub fn decode_hex(hex: &[u8], out: &mut [u8]) -> usize {
    #[inline]
    fn val(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }
    let max_bytes = out.len().min(hex.len() / 2);
    let mut written = 0;
    for i in 0..max_bytes {
        let hi = match val(hex[i * 2]) {
            Some(v) => v,
            None => break,
        };
        let lo = match val(hex[i * 2 + 1]) {
            Some(v) => v,
            None => break,
        };
        out[i] = (hi << 4) | lo;
        written += 1;
    }
    written
}

/// Encodes a slice of [`PackedOtlpRecord`] into compliant OpenTelemetry ResourceLogs JSON directly into `buf`.
///
/// Zero dynamic `serde_json::Value` heap allocations.
pub fn format_otlp_records_json(
    records: &[PackedOtlpRecord],
    service_name: &str,
    buf: &mut String,
) {
    let mut itoa_buf = ItoaBuf::new();

    buf.push_str("{\"resourceLogs\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"");
    buf.push_str(service_name);
    buf.push_str("\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"velda-edge\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"rust\"}}]},\"scopeLogs\":[{\"scope\":{\"name\":\"velda-edge\",\"version\":\"");
    buf.push_str(env!("CARGO_PKG_VERSION"));
    buf.push_str("\"},\"logRecords\":[");

    let mut first_rec = true;
    for item in records {
        if !first_rec {
            buf.push(',');
        }
        first_rec = false;

        let (severity_num, severity_txt) = item.severity();
        let method = item.method_str();
        let path = item.path();

        buf.push_str("{\"timeUnixNano\":\"");
        buf.push_str(itoa_buf.format(item.time_unix_nano));
        buf.push_str("\",\"severityNumber\":");
        buf.push_str(itoa_buf.format(severity_num));
        buf.push_str(",\"severityText\":\"");
        buf.push_str(severity_txt);
        buf.push_str("\",\"body\":{\"stringValue\":\"");
        buf.push_str(method);
        buf.push(' ');
        buf.push_str(path);
        buf.push_str(" -> ");
        buf.push_str(itoa_buf.format(item.status));
        buf.push_str(
            "\"},\"attributes\":[{\"key\":\"http.request.method\",\"value\":{\"stringValue\":\"",
        );
        buf.push_str(method);
        buf.push_str("\"}},{\"key\":\"http.response.status_code\",\"value\":{\"intValue\":");
        buf.push_str(itoa_buf.format(item.status));
        buf.push_str("}},{\"key\":\"url.path\",\"value\":{\"stringValue\":\"");
        buf.push_str(path);
        buf.push_str("\"}}");

        if item.ip_kind != 0 {
            buf.push_str(",{\"key\":\"client.address\",\"value\":{\"stringValue\":\"");
            item.format_client_ip(buf);
            buf.push_str("\"}}");
        }

        let route = item.route_id();
        if !route.is_empty() {
            buf.push_str(",{\"key\":\"http.route\",\"value\":{\"stringValue\":\"");
            buf.push_str(route);
            buf.push_str("\"}}");
        }

        let up = item.upstream();
        if !up.is_empty() {
            buf.push_str(",{\"key\":\"server.address\",\"value\":{\"stringValue\":\"");
            buf.push_str(up);
            buf.push_str("\"}}");
        }

        if item.duration_us > 0 {
            let lat_ms = item.duration_us as f64 / 1000.0;
            buf.push_str(",{\"key\":\"http.duration_ms\",\"value\":{\"doubleValue\":");
            let _ = write!(buf, "{lat_ms:.3}");
            buf.push_str("}}");
        }

        buf.push(']');

        if item.trace_id_len > 0 {
            buf.push_str(",\"traceId\":\"");
            for b in item.trace_id() {
                let _ = write!(buf, "{:02x}", b);
            }
            buf.push('"');
        }

        if item.span_id_len > 0 {
            buf.push_str(",\"spanId\":\"");
            for b in item.span_id() {
                let _ = write!(buf, "{:02x}", b);
            }
            buf.push('"');
        }

        buf.push('}');
    }

    buf.push_str("]}]}]}");
}
