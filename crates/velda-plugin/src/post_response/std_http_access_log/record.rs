//! Standard Access Log Record Structure and Serializers (`std_http_access_log`).
//!
//! Provides an ultra-compact, cacheline-aligned 256-byte POD record structure (`PackedLogRecord`)
//! designed for zero-copy register harmony, lock-free batch transport, and zero-allocation
//! slot-filling byte formatting (JSON, CLF, and Text).
//!
//! # Micro-Architectural Guarantees
//! - **Exact 256 Bytes**: Exactly 4 CPU cache lines (64 bytes each), fitting into 8 YMM registers (AVX2),
//!   16 NEON registers (ARM64), or 32 64-bit ALU registers without cache spill or alignment penalties.
//! - **Cacheline Aligned**: `#[repr(C, align(64))]` ensures aligned loads and non-temporal streaming writes.
//! - **Zero Heap Allocation**: Completely POD (Plain Old Data), zero pointer indirection on hot path.
//! - **192-Byte Inline User-Agent**: Ample capacity to store full web/browser User-Agent headers inline.

/// Output serialization style for access log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    #[default]
    Json,
    Text,
    Clf,
}

impl LogFormat {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "text" => Self::Text,
            "clf" | "combined" => Self::Clf,
            _ => Self::Json,
        }
    }
}

/// Ultra-compact 256-byte cacheline-aligned log record.
///
/// # Layout (256 bytes total, 64-byte aligned)
/// - `0..24`: Aligned integer scalars (`timestamp_ms`, `content_length`, `duration_us`, `status`, `version`, `method`, lengths)
/// - `24..64`: Fast inline identifiers (`request_id: 16B`, `route_id: 8B`, `upstream: 16B`)
/// - `64..256`: Inline User-Agent ASCII payload (192 bytes)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackedLogRecord {
    // --- 64-byte Core Metadata ---
    pub timestamp_ms: u64,    // 8 bytes (offset 0..8)
    pub content_length: u32,  // 4 bytes (offset 8..12)
    pub duration_us: u32,     // 4 bytes (offset 12..16)
    pub status: u16,          // 2 bytes (offset 16..18)
    pub version: u8, // 1 byte  (offset 18..19): 1=HTTP/1.0, 2=HTTP/1.1, 3=HTTP/2, 4=HTTP/3, 0=Other
    pub method: u8, // 1 byte  (offset 19..20): 1=GET, 2=POST, 3=PUT, 4=DELETE, 5=HEAD, 6=OPTIONS, 7=PATCH, 0=Other
    pub user_agent_len: u8, // 1 byte  (offset 20..21): max 192
    pub request_id_len: u8, // 1 byte  (offset 21..22): max 16
    pub route_id_len: u8, // 1 byte  (offset 22..23): max 8
    pub upstream_len: u8, // 1 byte  (offset 23..24): max 16
    pub request_id: [u8; 16], // 16 bytes (offset 24..40)
    pub route_id: [u8; 8], // 8 bytes  (offset 40..48)
    pub upstream: [u8; 16], // 16 bytes (offset 48..64)
    // --- 192-byte Inline User-Agent Payload ---
    pub user_agent: [u8; 192], // 192 bytes (offset 64..256)
}

const _: () = assert!(std::mem::size_of::<PackedLogRecord>() == 256);
const _: () = assert!(std::mem::align_of::<PackedLogRecord>() == 64);

#[inline(always)]
fn write_int<I: itoa::Integer>(buf: &mut Vec<u8>, val: I) {
    let mut ibuf = itoa::Buffer::new();
    buf.extend_from_slice(ibuf.format(val).as_bytes());
}

impl Default for PackedLogRecord {
    #[inline(always)]
    fn default() -> Self {
        Self::empty()
    }
}

impl PackedLogRecord {
    /// Creates a zero-initialized empty record.
    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            timestamp_ms: 0,
            content_length: 0,
            duration_us: 0,
            status: 0,
            version: 0,
            method: 0,
            user_agent_len: 0,
            request_id_len: 0,
            route_id_len: 0,
            upstream_len: 0,
            request_id: [0u8; 16],
            route_id: [0u8; 8],
            upstream: [0u8; 16],
            user_agent: [0u8; 192],
        }
    }

    /// Stores the incoming request UUID/ID into the inline buffer.
    #[inline(always)]
    pub fn set_request_id(&mut self, src: &[u8]) {
        let len = src.len().min(16);
        self.request_id[..len].copy_from_slice(&src[..len]);
        self.request_id_len = len as u8;
    }

    /// Stores the matched route identifier into the inline buffer.
    #[inline(always)]
    pub fn set_route_id(&mut self, src: &[u8]) {
        let len = src.len().min(8);
        self.route_id[..len].copy_from_slice(&src[..len]);
        self.route_id_len = len as u8;
    }

    /// Stores the upstream target address into the inline buffer.
    #[inline(always)]
    pub fn set_upstream(&mut self, src: &[u8]) {
        let len = src.len().min(16);
        self.upstream[..len].copy_from_slice(&src[..len]);
        self.upstream_len = len as u8;
    }

    /// Stores the downstream client User-Agent into the inline buffer (up to 192 bytes).
    #[inline(always)]
    pub fn set_user_agent(&mut self, src: &[u8]) {
        let len = src.len().min(192);
        self.user_agent[..len].copy_from_slice(&src[..len]);
        self.user_agent_len = len as u8;
    }

    /// Encodes HTTP method enum into compact `u8`.
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

    /// Encodes HTTP version enum into compact `u8`.
    #[inline(always)]
    pub fn encode_version(ver: http::Version) -> u8 {
        match ver {
            http::Version::HTTP_10 => 1,
            http::Version::HTTP_11 => 2,
            http::Version::HTTP_2 => 3,
            http::Version::HTTP_3 => 4,
            _ => 0,
        }
    }

    #[inline(always)]
    pub fn request_id(&self) -> &str {
        std::str::from_utf8(&self.request_id[..self.request_id_len as usize]).unwrap_or("-")
    }

    #[inline(always)]
    pub fn route_id(&self) -> &str {
        std::str::from_utf8(&self.route_id[..self.route_id_len as usize]).unwrap_or("-")
    }

    #[inline(always)]
    pub fn upstream(&self) -> &str {
        std::str::from_utf8(&self.upstream[..self.upstream_len as usize]).unwrap_or("-")
    }

    #[inline(always)]
    pub fn user_agent(&self) -> &str {
        std::str::from_utf8(&self.user_agent[..self.user_agent_len as usize]).unwrap_or("-")
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
            _ => "UNKNOWN",
        }
    }

    #[inline(always)]
    pub fn version_str(&self) -> &'static str {
        match self.version {
            1 => "HTTP/1.0",
            2 => "HTTP/1.1",
            3 => "HTTP/2.0",
            4 => "HTTP/3.0",
            _ => "HTTP/?",
        }
    }

    #[inline(always)]
    pub fn latency_ms(&self) -> f64 {
        self.duration_us as f64 / 1000.0
    }

    /// Serializes this record directly into a byte buffer using static JSON templating ("slot-filling").
    ///
    /// # Performance
    /// - Zero heap allocations when `buf` has reserved capacity.
    /// - Uses `itoa` directly on contiguous memory.
    pub fn write_json(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(b"{\"timestamp\":");
        write_int(buf, self.timestamp_ms);

        buf.extend_from_slice(b",\"status\":");
        write_int(buf, self.status);

        buf.extend_from_slice(b",\"method\":\"");
        buf.extend_from_slice(self.method_str().as_bytes());

        buf.extend_from_slice(b"\",\"version\":\"");
        buf.extend_from_slice(self.version_str().as_bytes());

        buf.extend_from_slice(b"\",\"content_length\":");
        write_int(buf, self.content_length);

        buf.extend_from_slice(b",\"latency_us\":");
        write_int(buf, self.duration_us);

        buf.extend_from_slice(b",\"request_id\":");
        if self.request_id_len > 0 {
            buf.push(b'"');
            buf.extend_from_slice(&self.request_id[..self.request_id_len as usize]);
            buf.push(b'"');
        } else {
            buf.extend_from_slice(b"null");
        }

        buf.extend_from_slice(b",\"route_id\":");
        if self.route_id_len > 0 {
            buf.push(b'"');
            buf.extend_from_slice(&self.route_id[..self.route_id_len as usize]);
            buf.push(b'"');
        } else {
            buf.extend_from_slice(b"null");
        }

        buf.extend_from_slice(b",\"upstream\":");
        if self.upstream_len > 0 {
            buf.push(b'"');
            buf.extend_from_slice(&self.upstream[..self.upstream_len as usize]);
            buf.push(b'"');
        } else {
            buf.extend_from_slice(b"null");
        }

        buf.extend_from_slice(b",\"user_agent\":");
        if self.user_agent_len > 0 {
            buf.push(b'"');
            let ua_bytes = &self.user_agent[..self.user_agent_len as usize];
            for &b in ua_bytes {
                if b == b'"' || b == b'\\' {
                    buf.push(b'\\');
                }
                buf.push(b);
            }
            buf.push(b'"');
        } else {
            buf.extend_from_slice(b"null");
        }

        buf.extend_from_slice(b"}\n");
    }

    /// Serializes this record directly into a byte buffer as single-line Text format.
    pub fn write_text(&self, buf: &mut Vec<u8>) {
        buf.push(b'[');
        write_int(buf, self.timestamp_ms);
        buf.extend_from_slice(b"] status=");
        write_int(buf, self.status);
        buf.extend_from_slice(b" method=");
        buf.extend_from_slice(self.method_str().as_bytes());
        buf.extend_from_slice(b" version=");
        buf.extend_from_slice(self.version_str().as_bytes());
        buf.extend_from_slice(b" bytes=");
        write_int(buf, self.content_length);
        buf.extend_from_slice(b" latency_us=");
        write_int(buf, self.duration_us);
        buf.extend_from_slice(b" req_id=");
        buf.extend_from_slice(self.request_id().as_bytes());
        buf.extend_from_slice(b" route=");
        buf.extend_from_slice(self.route_id().as_bytes());
        buf.extend_from_slice(b" upstream=");
        buf.extend_from_slice(self.upstream().as_bytes());
        buf.extend_from_slice(b" ua=\"");
        buf.extend_from_slice(self.user_agent().as_bytes());
        buf.extend_from_slice(b"\"\n");
    }

    /// Serializes this record directly in Common Log Format (CLF / Nginx Combined format).
    pub fn write_clf(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(b"- - - [");
        write_int(buf, self.timestamp_ms);
        buf.extend_from_slice(b"] \"");
        buf.extend_from_slice(self.method_str().as_bytes());
        buf.extend_from_slice(b" / ");
        buf.extend_from_slice(self.version_str().as_bytes());
        buf.extend_from_slice(b"\" ");
        write_int(buf, self.status);
        buf.push(b' ');
        write_int(buf, self.content_length);
        buf.extend_from_slice(b" \"-\" \"");
        buf.extend_from_slice(self.user_agent().as_bytes());
        buf.extend_from_slice(b"\"\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_memory_layout() {
        assert_eq!(std::mem::size_of::<PackedLogRecord>(), 256);
        assert_eq!(std::mem::align_of::<PackedLogRecord>(), 64);
    }

    #[test]
    fn test_record_json_slot_filling() {
        let mut record = PackedLogRecord::empty();
        record.timestamp_ms = 1700000000000;
        record.status = 200;
        record.duration_us = 12340;
        record.version = 3; // HTTP/2
        record.method = 1; // GET
        record.content_length = 2048;
        record.set_request_id(b"req-12345");
        record.set_route_id(b"orders");
        record.set_upstream(b"10.0.1.5:8080");
        record
            .set_user_agent(b"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36");

        let mut buf = Vec::with_capacity(512);
        record.write_json(&mut buf);

        let json = std::str::from_utf8(&buf).unwrap();
        assert!(json.contains("\"status\":200"));
        assert!(json.contains("\"method\":\"GET\""));
        assert!(json.contains("\"version\":\"HTTP/2.0\""));
        assert!(json.contains("\"latency_us\":12340"));
        assert!(json.contains("\"request_id\":\"req-12345\""));
        assert!(json.contains("\"route_id\":\"orders\""));
        assert!(json.contains("\"upstream\":\"10.0.1.5:8080\""));
        assert!(json.contains("Mozilla/5.0"));
        assert!(json.ends_with('\n'));
    }

    #[test]
    fn test_record_text_and_clf() {
        let mut record = PackedLogRecord::empty();
        record.timestamp_ms = 1700000000000;
        record.status = 404;
        record.duration_us = 450;
        record.version = 2; // HTTP/1.1
        record.method = 2; // POST
        record.set_user_agent(b"curl/8.5.0");

        let mut text_buf = Vec::new();
        record.write_text(&mut text_buf);
        let text = std::str::from_utf8(&text_buf).unwrap();
        assert!(text.contains("status=404"));
        assert!(text.contains("method=POST"));
        assert!(text.contains("ua=\"curl/8.5.0\""));

        let mut clf_buf = Vec::new();
        record.write_clf(&mut clf_buf);
        let clf = std::str::from_utf8(&clf_buf).unwrap();
        assert!(clf.contains("\"POST / HTTP/1.1\" 404 0"));
        assert!(clf.contains("\"curl/8.5.0\""));
    }
}
