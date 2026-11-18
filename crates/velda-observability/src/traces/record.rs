//! Canonical 64-Byte Cacheline-Aligned Span Record (`velda_observability::traces::record`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────┐
//! │ Exactly 64 Bytes ──► 1 CPU L1D Cache Line                              │
//! ├────────────────────────────────────────────────────────────────────────┤
//! │ 0..16  : Timestamps (start_nano: u64, end_nano: u64)                   │
//! │ 16..40 : IDs (trace_id: [u8; 16], span_id: [u8; 8])                    │
//! │ 40..44 : Client IP (client_ip: [u8; 4])                                │
//! │ 44..48 : Dense LUT IDs (route_id: u16, upstream_id: u16)               │
//! │ 48..50 : Status Code (status_code: u16)                                │
//! │ 50..55 : Metadata (otlp_status, method, span_kind, ip_kind, flags)     │
//! │ 55..64 : Reserved padding (_reserved: [u8; 9])                         │
//! └────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt::Write as _;
use velda_core::lut::HttpMethod;

/// Canonical 64-byte cacheline-aligned OpenTelemetry Span Record.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanRecord {
    pub start_time_unix_nano: u64, // 8 bytes (0..8)
    pub end_time_unix_nano: u64,   // 8 bytes (8..16)
    pub trace_id: [u8; 16],        // 16 bytes (16..32)
    pub span_id: [u8; 8],          // 8 bytes (32..40)
    pub client_ip: [u8; 4],        // 4 bytes (40..44)
    pub route_id: u16,             // 2 bytes (44..46)
    pub upstream_id: u16,          // 2 bytes (46..48)
    pub status_code: u16,          // 2 bytes (48..50)
    pub otlp_status: u8,           // 1 byte  (50..51)
    pub method: u8,                // 1 byte  (51..52)
    pub span_kind: u8,             // 1 byte  (52..53)
    pub ip_kind: u8,               // 1 byte  (53..54)
    pub flags: u8,                 // 1 byte  (54..55)
    pub _reserved: [u8; 9],        // 9 bytes (55..64)
}

const _: () = assert!(std::mem::size_of::<SpanRecord>() == 64);
const _: () = assert!(std::mem::align_of::<SpanRecord>() == 64);

impl Default for SpanRecord {
    #[inline(always)]
    fn default() -> Self {
        Self::empty()
    }
}

impl SpanRecord {
    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            start_time_unix_nano: 0,
            end_time_unix_nano: 0,
            trace_id: [0; 16],
            span_id: [0; 8],
            client_ip: [0; 4],
            route_id: velda_core::UNMATCHED_ROUTE_ID,
            upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
            status_code: 0,
            otlp_status: 0,
            method: 1,    // Default GET
            span_kind: 2, // SPAN_KIND_SERVER
            ip_kind: 0,
            flags: 1, // Sampled
            _reserved: [0; 9],
        }
    }

    /// Constructs a root Server Span record for an ingress hop.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn new_root_span(
        request_id_raw: u128,
        entropy: u64,
        start_nano: u64,
        end_nano: u64,
        status_code: u16,
        method: HttpMethod,
        peer_ip: std::net::IpAddr,
        route_id: u16,
        upstream_id: u16,
    ) -> Self {
        let mut span = Self::empty();
        span.start_time_unix_nano = start_nano;
        span.end_time_unix_nano = end_nano;
        span.status_code = status_code;
        span.otlp_status = if status_code < 400 { 1 } else { 2 };
        span.method = Self::encode_http_method(method);
        span.set_client_ip(peer_ip);
        span.trace_id = request_id_raw.to_be_bytes();
        span.span_id = entropy.to_be_bytes();
        span.route_id = route_id;
        span.upstream_id = upstream_id;
        span
    }

    #[inline(always)]
    pub fn encode_http_method(method: HttpMethod) -> u8 {
        match method {
            HttpMethod::Get => 1,
            HttpMethod::Post => 2,
            HttpMethod::Put => 3,
            HttpMethod::Delete => 4,
            HttpMethod::Head => 5,
            HttpMethod::Options => 6,
            HttpMethod::Patch => 7,
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
    pub fn set_client_ip(&mut self, ip: std::net::IpAddr) {
        match ip {
            std::net::IpAddr::V4(v4) => {
                self.ip_kind = 4;
                self.client_ip.copy_from_slice(&v4.octets());
            }
            std::net::IpAddr::V6(v6) => {
                if let Some(v4) = v6.to_ipv4_mapped() {
                    self.ip_kind = 4;
                    self.client_ip.copy_from_slice(&v4.octets());
                } else {
                    self.ip_kind = 0;
                }
            }
        }
    }

    #[inline(always)]
    pub fn format_client_ip(&self, buf: &mut String) {
        if self.ip_kind == 4 {
            let _ = write!(
                buf,
                "{}.{}.{}.{}",
                self.client_ip[0], self.client_ip[1], self.client_ip[2], self.client_ip[3]
            );
        }
    }

    /// Formats hex trace ID (32 chars) into `out`.
    #[inline(always)]
    pub fn format_trace_id(&self, out: &mut [u8; 32]) {
        const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
        for (i, &b) in self.trace_id.iter().enumerate() {
            out[i * 2] = HEX_CHARS[(b >> 4) as usize];
            out[i * 2 + 1] = HEX_CHARS[(b & 0x0f) as usize];
        }
    }

    /// Formats hex span ID (16 chars) into `out`.
    #[inline(always)]
    pub fn format_span_id(&self, out: &mut [u8; 16]) {
        const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
        for (i, &b) in self.span_id.iter().enumerate() {
            out[i * 2] = HEX_CHARS[(b >> 4) as usize];
            out[i * 2 + 1] = HEX_CHARS[(b & 0x0f) as usize];
        }
    }
}
