//! Cacheline-Aligned 256-Byte OpenTelemetry Span (`opentelemetry_tracer::span`).
//!
//! Provides a zero-allocation, cacheline-aligned (256-byte) POD span structure (`PackedOtlpSpan`)
//! designed for non-temporal streaming writes to RAM, lock-free batch transport, and zero-allocation
//! OTLP JSON / Protobuf serialization on Shadow Worker threads.
//!
//! # Micro-Architectural Guarantees
//! - **Exact 256 Bytes**: Exactly 4 CPU cache lines (64 bytes each).
//! - **Cacheline Aligned**: `#[repr(C, align(64))]` enables aligned vector loads and `_mm256_stream_si256`.
//! - **Zero Heap Allocation**: Pure POD layout, zero pointers, zero dynamic heap allocations.

use itoa::Buffer as ItoaBuf;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

/// Structured legacy span record retained for backward-compatible interface and testing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OtlpSpanItem {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub start_time_unix_nano: u128,
    pub end_time_unix_nano: u128,
    pub status_code: u16,
    pub method: String,
    pub path: String,
    pub route_id: Option<String>,
    pub client_ip: Option<String>,
    pub upstream: Option<String>,
}

/// Ultra-compact 256-byte cacheline-aligned OTLP span record.
///
/// # Layout (256 bytes total, 64-byte aligned across 4 cache lines)
/// - **Cache Line 1 (0..64)**: Timestamps, status, method, trace/span/parent binary IDs
///   - `0..8`: `start_time_unix_nano: u64`
///   - `8..16`: `end_time_unix_nano: u64`
///   - `16..18`: `status_code: u16`
///   - `18..19`: `otlp_status: u8` (1=OK, 2=ERROR, 0=UNSET)
///   - `19..20`: `method: u8` (1=GET, 2=POST, 3=PUT, 4=DELETE, 5=HEAD, 6=OPTIONS, 7=PATCH, 0=Other)
///   - `20..21`: `span_kind: u8` (2=SPAN_KIND_SERVER)
///   - `21..22`: `has_parent: u8` (1=valid parent_span_id, 0=root span)
///   - `22..23`: `ip_kind: u8` (0=None, 4=IPv4, 6=IPv6)
///   - `23..24`: `flags: u8` (W3C trace flags: 1=Sampled)
///   - `24..25`: `route_id_len: u8` (max 24)
///   - `25..26`: `upstream_len: u8` (max 24)
///   - `26..27`: `path_len: u8` (max 128)
///   - `27..32`: `_reserved: [u8; 5]`
///   - `32..48`: `trace_id: [u8; 16]` (binary 128-bit W3C Trace ID)
///   - `48..56`: `span_id: [u8; 8]` (binary 64-bit W3C Span ID)
///   - `56..64`: `parent_span_id: [u8; 8]` (binary 64-bit Parent Span ID)
/// - **Cache Line 2 (64..128)**: Client IP, Route ID, Upstream address
///   - `64..80`: `client_ip: [u8; 16]`
///   - `80..104`: `route_id: [u8; 24]`
///   - `104..128`: `upstream: [u8; 24]`
/// - **Cache Line 3 & 4 (128..256)**: Inline URL Path
///   - `128..256`: `path: [u8; 128]` (128 bytes inline ASCII path)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackedOtlpSpan {
    // --- Cache Line 1 (0..64) ---
    pub start_time_unix_nano: u64, // 8 bytes (0..8)
    pub end_time_unix_nano: u64,   // 8 bytes (8..16)
    pub status_code: u16,          // 2 bytes (16..18)
    pub otlp_status: u8,           // 1 byte  (18..19)
    pub method: u8,                // 1 byte  (19..20)
    pub span_kind: u8,             // 1 byte  (20..21)
    pub has_parent: u8,            // 1 byte  (21..22)
    pub ip_kind: u8,               // 1 byte  (22..23)
    pub flags: u8,                 // 1 byte  (23..24)
    pub route_id_len: u8,          // 1 byte  (24..25)
    pub upstream_len: u8,          // 1 byte  (25..26)
    pub path_len: u8,              // 1 byte  (26..27)
    pub _reserved: [u8; 5],        // 5 bytes (27..32)
    pub trace_id: [u8; 16],        // 16 bytes (32..48)
    pub span_id: [u8; 8],          // 8 bytes (48..56)
    pub parent_span_id: [u8; 8],   // 8 bytes (56..64)
    // --- Cache Line 2 (64..128) ---
    pub client_ip: [u8; 16], // 16 bytes (64..80)
    pub route_id: [u8; 24],  // 24 bytes (80..104)
    pub upstream: [u8; 24],  // 24 bytes (104..128)
    // --- Cache Line 3 & 4 (128..256) ---
    pub path: [u8; 128], // 128 bytes (128..256)
}

const _: () = assert!(std::mem::size_of::<PackedOtlpSpan>() == 256);
const _: () = assert!(std::mem::align_of::<PackedOtlpSpan>() == 64);

impl Default for PackedOtlpSpan {
    #[inline(always)]
    fn default() -> Self {
        Self::empty()
    }
}

impl PackedOtlpSpan {
    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            start_time_unix_nano: 0,
            end_time_unix_nano: 0,
            status_code: 0,
            otlp_status: 0,
            method: 1,    // Default GET
            span_kind: 2, // SPAN_KIND_SERVER
            has_parent: 0,
            ip_kind: 0,
            flags: 1, // Sampled
            route_id_len: 0,
            upstream_len: 0,
            path_len: 0,
            _reserved: [0; 5],
            trace_id: [0; 16],
            span_id: [0; 8],
            parent_span_id: [0; 8],
            client_ip: [0; 16],
            route_id: [0; 24],
            upstream: [0; 24],
            path: [0; 128],
        }
    }

    #[inline(always)]
    pub fn encode_method(method: &http::Method) -> u8 {
        match *method {
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
    pub fn method_str(&self) -> &'static str {
        match self.method {
            1 => "GET",
            2 => "POST",
            3 => "PUT",
            4 => "DELETE",
            5 => "HEAD",
            6 => "OPTIONS",
            7 => "PATCH",
            _ => "OTHER",
        }
    }

    #[inline(always)]
    pub fn set_path(&mut self, src: &[u8]) {
        let copy_len = src.len().min(128);
        self.path[..copy_len].copy_from_slice(&src[..copy_len]);
        self.path_len = copy_len as u8;
    }

    #[inline(always)]
    pub fn path(&self) -> &str {
        let p = std::str::from_utf8(&self.path[..self.path_len as usize]).unwrap_or("");
        if p.is_empty() { "/" } else { p }
    }

    #[inline(always)]
    pub fn set_route_id(&mut self, src: &[u8]) {
        let copy_len = src.len().min(24);
        self.route_id[..copy_len].copy_from_slice(&src[..copy_len]);
        self.route_id_len = copy_len as u8;
    }

    #[inline(always)]
    pub fn route_id(&self) -> &str {
        std::str::from_utf8(&self.route_id[..self.route_id_len as usize]).unwrap_or("")
    }

    #[inline(always)]
    pub fn set_upstream(&mut self, src: &[u8]) {
        let copy_len = src.len().min(24);
        self.upstream[..copy_len].copy_from_slice(&src[..copy_len]);
        self.upstream_len = copy_len as u8;
    }

    #[inline(always)]
    pub fn upstream(&self) -> &str {
        std::str::from_utf8(&self.upstream[..self.upstream_len as usize]).unwrap_or("")
    }

    #[inline(always)]
    pub fn set_client_ip(&mut self, ip: std::net::IpAddr) {
        match ip {
            std::net::IpAddr::V4(v4) => {
                self.ip_kind = 4;
                self.client_ip[0..4].copy_from_slice(&v4.octets());
            }
            std::net::IpAddr::V6(v6) => {
                self.ip_kind = 6;
                self.client_ip.copy_from_slice(&v6.octets());
            }
        }
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

    #[inline(always)]
    pub fn set_trace_id(&mut self, src: &[u8]) {
        let copy_len = src.len().min(16);
        self.trace_id[..copy_len].copy_from_slice(&src[..copy_len]);
    }

    #[inline(always)]
    pub fn set_span_id(&mut self, src: &[u8]) {
        let copy_len = src.len().min(8);
        self.span_id[..copy_len].copy_from_slice(&src[..copy_len]);
    }

    #[inline(always)]
    pub fn set_parent_span_id(&mut self, src: &[u8]) {
        let copy_len = src.len().min(8);
        self.parent_span_id[..copy_len].copy_from_slice(&src[..copy_len]);
        self.has_parent = if copy_len == 8 && self.parent_span_id != [0; 8] {
            1
        } else {
            0
        };
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

/// Encodes a slice of [`PackedOtlpSpan`] into compliant OpenTelemetry ResourceSpans JSON directly into `buf`.
///
/// Zero dynamic `serde_json::Value` heap allocations.
pub fn format_packed_spans_json(spans: &[PackedOtlpSpan], service_name: &str, buf: &mut String) {
    let mut itoa_buf = ItoaBuf::new();

    buf.push_str("{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"");
    buf.push_str(service_name);
    buf.push_str("\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"velda-edge\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"rust\"}}]},\"scopeSpans\":[{\"scope\":{\"name\":\"velda-edge\",\"version\":\"");
    buf.push_str(env!("CARGO_PKG_VERSION"));
    buf.push_str("\"},\"spans\":[");

    let mut first_span = true;
    for item in spans {
        if !first_span {
            buf.push(',');
        }
        first_span = false;

        let method = item.method_str();
        let path = item.path();

        buf.push_str("{\"traceId\":\"");
        for b in &item.trace_id {
            let _ = write!(buf, "{:02x}", b);
        }
        buf.push_str("\",\"spanId\":\"");
        for b in &item.span_id {
            let _ = write!(buf, "{:02x}", b);
        }
        buf.push('"');

        if item.has_parent == 1 {
            buf.push_str(",\"parentSpanId\":\"");
            for b in &item.parent_span_id {
                let _ = write!(buf, "{:02x}", b);
            }
            buf.push('"');
        }

        buf.push_str(",\"name\":\"");
        buf.push_str(method);
        buf.push(' ');
        buf.push_str(path);
        buf.push_str("\",\"kind\":1,\"startTimeUnixNano\":\"");
        buf.push_str(itoa_buf.format(item.start_time_unix_nano));
        buf.push_str("\",\"endTimeUnixNano\":\"");
        buf.push_str(itoa_buf.format(item.end_time_unix_nano));
        buf.push_str(
            "\",\"attributes\":[{\"key\":\"http.request.method\",\"value\":{\"stringValue\":\"",
        );
        buf.push_str(method);
        buf.push_str("\"}},{\"key\":\"http.response.status_code\",\"value\":{\"intValue\":");
        buf.push_str(itoa_buf.format(item.status_code));
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

        buf.push_str("],\"status\":{\"code\":");
        let status_code = if item.status_code < 400 { 1 } else { 2 };
        buf.push_str(itoa_buf.format(status_code));
        buf.push_str("}}");
    }

    buf.push_str("]}]}]}");
}

/// Encodes a slice of legacy [`OtlpSpanItem`] records into compliant OpenTelemetry ResourceSpans JSON.
///
/// Retained for test backward-compatibility.
pub fn format_otlp_spans_json(spans: &[OtlpSpanItem], service_name: &str) -> String {
    let mut packed_list = Vec::with_capacity(spans.len());
    for s in spans {
        let mut p = PackedOtlpSpan::empty();
        p.start_time_unix_nano = s.start_time_unix_nano as u64;
        p.end_time_unix_nano = s.end_time_unix_nano as u64;
        p.status_code = s.status_code;

        if let Ok(method) = s.method.parse::<http::Method>() {
            p.method = PackedOtlpSpan::encode_method(&method);
        }
        p.set_path(s.path.as_bytes());

        if let Some(ip) = &s.client_ip
            && let Ok(addr) = ip.parse::<std::net::IpAddr>()
        {
            p.set_client_ip(addr);
        }
        if let Some(route) = &s.route_id {
            p.set_route_id(route.as_bytes());
        }
        if let Some(up) = &s.upstream {
            p.set_upstream(up.as_bytes());
        }

        let mut trace_raw = [0u8; 16];
        if decode_hex(s.trace_id.as_bytes(), &mut trace_raw) == 16 {
            p.set_trace_id(&trace_raw);
        }

        let mut span_raw = [0u8; 8];
        if decode_hex(s.span_id.as_bytes(), &mut span_raw) == 8 {
            p.set_span_id(&span_raw);
        }

        if let Some(parent) = &s.parent_span_id {
            let mut parent_raw = [0u8; 8];
            if decode_hex(parent.as_bytes(), &mut parent_raw) == 8 {
                p.set_parent_span_id(&parent_raw);
            }
        }

        packed_list.push(p);
    }

    let mut buf = String::with_capacity(1024 * spans.len().max(1));
    format_packed_spans_json(&packed_list, service_name, &mut buf);
    buf
}
