//! Canonical 64-Byte Cacheline-Aligned Span Record (`velda_observability::traces::record`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────┐
//! │ Exactly 64 Bytes ──► 1 CPU L1D Cache Line                              │
//! ├────────────────────────────────────────────────────────────────────────┤
//! │ 0..16  : Client IP (client_ip: u128, RFC 4291 mapped or IPv6)          │
//! │ 16..32 : Trace ID (trace_id: [u8; 16])                                 │
//! │ 32..40 : Timestamps (start_nano: u64)                                  │
//! │ 40..48 : Timestamps (end_nano: u64)                                    │
//! │ 48..56 : Span ID (span_id: [u8; 8])                                    │
//! │ 56..62 : IDs (route_id: u16, upstream_id: u16, status_code: u16)        │
//! │ 62..64 : Metadata (method: HttpMethod, flags: u8)                        │
//! └────────────────────────────────────────────────────────────────────────┘
//! ```

use velda_core::lut::HttpMethod;

/// Canonical 64-byte cacheline-aligned OpenTelemetry Span Record.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanRecord {
    pub client_ip: u128,           // 16 bytes (0..16)
    pub trace_id: [u8; 16],        // 16 bytes (16..32)
    pub start_time_unix_nano: u64, // 8 bytes (32..40)
    pub end_time_unix_nano: u64,   // 8 bytes (40..48)
    pub span_id: [u8; 8],          // 8 bytes (48..56)
    pub route_id: u16,             // 2 bytes (56..58)
    pub upstream_id: u16,          // 2 bytes (58..60)
    pub status_code: u16,          // 2 bytes (60..62)
    pub method: HttpMethod,        // 1 byte  (62..63)
    pub flags: u8,                 // 1 byte  (63..64)
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
            client_ip: 0,
            trace_id: [0; 16],
            start_time_unix_nano: 0,
            end_time_unix_nano: 0,
            span_id: [0; 8],
            route_id: velda_core::UNMATCHED_ROUTE_ID,
            upstream_id: velda_core::UNASSIGNED_UPSTREAM_ID,
            status_code: 0,
            method: HttpMethod::Get,
            flags: 1, // Sampled
        }
    }
}
