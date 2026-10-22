//! W3C TraceContext Specification Codec (`w3c_trace_context`).
//!
//! Handles parsing, validation, generation, and formatting of W3C `traceparent` headers
//! conforming to the W3C TraceContext Recommendation:
//! `version-trace_id-parent_id-trace_flags` (e.g. `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`).

use std::sync::atomic::{AtomicU64, Ordering};

/// Global atomic sequence counter for generating unique 64-bit span IDs.
static SPAN_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Parsed view of a valid incoming W3C `traceparent` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTraceParent<'a> {
    pub version: &'a str,
    pub trace_id: &'a str,
    pub parent_id: &'a str,
    pub flags: &'a str,
}

/// W3C TraceContext codec utility.
pub struct TraceParentCodec;

impl TraceParentCodec {
    /// Generates a new 16-hex lowercase string span ID using monotonic sequence and high-resolution entropy.
    #[inline]
    pub fn generate_span_id() -> [u8; 16] {
        let seq = SPAN_COUNTER.fetch_add(1, Ordering::Relaxed);
        let time_part = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
        let val = (time_part << 32) ^ seq;
        let mut out = [0u8; 16];
        velda_core::request_id::format_hex_16(val.max(1), &mut out);
        out
    }

    /// Generates a new 32-hex lowercase string trace ID using unix timestamp and monotonic entropy.
    #[inline]
    pub fn generate_trace_id() -> String {
        let sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let seq = SPAN_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut trace_buf = [0u8; 32];
        velda_core::request_id::format_hex_16(sec, (&mut trace_buf[0..16]).try_into().unwrap());
        velda_core::request_id::format_hex_16(seq, (&mut trace_buf[16..32]).try_into().unwrap());
        std::str::from_utf8(&trace_buf)
            .unwrap_or("00000000000000000000000000000001")
            .to_string()
    }

    /// Parses and validates an incoming W3C `traceparent` header value.
    ///
    /// Conformance requirements:
    /// - Exactly 55 characters: `00-{32hex}-{16hex}-{2hex}`
    /// - Version strictly `00`
    /// - Non-zero `trace_id` (16 bytes = 32 hex chars)
    /// - Non-zero `parent_id` (8 bytes = 16 hex chars)
    /// - Valid lowercase hex characters
    #[inline]
    pub fn parse(val: &str) -> Option<ParsedTraceParent<'_>> {
        let trimmed = val.trim();
        // Exact length of W3C traceparent: 2 + 1 + 32 + 1 + 16 + 1 + 2 = 55 chars
        if trimmed.len() != 55 {
            return None;
        }

        let bytes = trimmed.as_bytes();
        if bytes[2] != b'-' || bytes[35] != b'-' || bytes[52] != b'-' {
            return None;
        }

        let version = &trimmed[0..2];
        if version != "00" {
            return None;
        }

        let trace_id = &trimmed[3..35];
        if trace_id == "00000000000000000000000000000000"
            || !trace_id.chars().all(|c| c.is_ascii_hexdigit())
        {
            return None;
        }

        let parent_id = &trimmed[36..52];
        if parent_id == "0000000000000000" || !parent_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        let flags = &trimmed[53..55];
        if !flags.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        Some(ParsedTraceParent {
            version,
            trace_id,
            parent_id,
            flags,
        })
    }

    /// Formats a complete W3C `traceparent` header string.
    #[inline]
    pub fn format(trace_id: &str, span_id: &str, flags: &str) -> String {
        format!("00-{trace_id}-{span_id}-{flags}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_traceparent_parsing() {
        let raw = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let parsed = TraceParentCodec::parse(raw).expect("Valid traceparent");
        assert_eq!(parsed.version, "00");
        assert_eq!(parsed.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(parsed.parent_id, "00f067aa0ba902b7");
        assert_eq!(parsed.flags, "01");
    }

    #[test]
    fn test_invalid_traceparent_rejected() {
        assert!(TraceParentCodec::parse("").is_none());
        assert!(
            TraceParentCodec::parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .is_none()
        );
        assert!(
            TraceParentCodec::parse("00-00000000000000000000000000000000-00f067aa0ba902b7-01")
                .is_none()
        );
        assert!(
            TraceParentCodec::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01")
                .is_none()
        );
        assert!(TraceParentCodec::parse("short").is_none());
    }

    #[test]
    fn test_generate_ids() {
        let span = TraceParentCodec::generate_span_id();
        assert_eq!(span.len(), 16);
        let trace = TraceParentCodec::generate_trace_id();
        assert_eq!(trace.len(), 32);
    }
}
