//! Access Log Record Structure and Serializers.
//!
//! Provides structured, strongly typed access log entries with zero-allocation,
//! template-based byte injection ("form filling") for JSON and Text serialization.
//!
//! # Architecture Invariant
//! - Zero external serialization dependencies (No dynamic JSON trees, no dynamic maps).
//! - Static skeleton byte slices placed directly in memory.
//! - Stack-based integer/float to ASCII formatting directly into reusable buffers.

/// Formatting output style for serialized access log entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    #[default]
    Json,
    Text,
}

impl LogFormat {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "text" | "combined" | "clf" => Self::Text,
            _ => Self::Json,
        }
    }
}

/// Strongly typed, structured record emitted by the `access_log` hook on every response.
#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub timestamp_ms: u64,
    pub status: u16,
    pub version: &'static str,
    pub content_length: Option<u64>,
    pub request_id: Option<String>,
    pub route_id: Option<String>,
    pub upstream: Option<String>,
    pub latency_ms: Option<f64>,
}

#[inline(always)]
fn write_u64(buf: &mut Vec<u8>, mut n: u64) {
    if n == 0 {
        buf.push(b'0');
        return;
    }
    let mut temp = [0u8; 20];
    let mut i = 20;
    while n > 0 {
        i -= 1;
        temp[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    buf.extend_from_slice(&temp[i..]);
}

#[inline(always)]
fn write_f64_2dp(buf: &mut Vec<u8>, mut val: f64) {
    if val < 0.0 {
        buf.push(b'-');
        val = -val;
    }
    let whole = val as u64;
    write_u64(buf, whole);
    buf.push(b'.');
    let frac = ((val - whole as f64) * 100.0).round() as u64;
    if frac < 10 {
        buf.push(b'0');
    }
    write_u64(buf, frac);
}

impl LogRecord {
    /// Serializes this record directly into a byte buffer as JSON using static byte templating ("filling in the blanks").
    ///
    /// # Performance
    /// - Zero heap allocations when `buf` has sufficient capacity (e.g. >= 256 bytes).
    /// - Zero dynamic tree traversal, zero reflection, zero external crates.
    pub fn write_json(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(b"{\"timestamp\":");
        write_u64(buf, self.timestamp_ms);

        buf.extend_from_slice(b",\"status\":");
        write_u64(buf, self.status as u64);

        buf.extend_from_slice(b",\"version\":\"");
        buf.extend_from_slice(self.version.as_bytes());
        buf.push(b'"');

        buf.extend_from_slice(b",\"content_length\":");
        match self.content_length {
            Some(cl) => write_u64(buf, cl),
            None => buf.extend_from_slice(b"null"),
        }

        buf.extend_from_slice(b",\"request_id\":");
        match &self.request_id {
            Some(id) => {
                buf.push(b'"');
                buf.extend_from_slice(id.as_bytes());
                buf.push(b'"');
            }
            None => buf.extend_from_slice(b"null"),
        }

        buf.extend_from_slice(b",\"route_id\":");
        match &self.route_id {
            Some(id) => {
                buf.push(b'"');
                buf.extend_from_slice(id.as_bytes());
                buf.push(b'"');
            }
            None => buf.extend_from_slice(b"null"),
        }

        buf.extend_from_slice(b",\"upstream\":");
        match &self.upstream {
            Some(up) => {
                buf.push(b'"');
                buf.extend_from_slice(up.as_bytes());
                buf.push(b'"');
            }
            None => buf.extend_from_slice(b"null"),
        }

        buf.extend_from_slice(b",\"latency_ms\":");
        match self.latency_ms {
            Some(lat) => write_f64_2dp(buf, lat),
            None => buf.extend_from_slice(b"null"),
        }

        buf.push(b'}');
    }

    /// Serializes this record directly into a byte buffer as single-line Text format.
    pub fn write_text(&self, buf: &mut Vec<u8>) {
        buf.push(b'[');
        write_u64(buf, self.timestamp_ms);
        buf.extend_from_slice(b"] status=");
        write_u64(buf, self.status as u64);
        buf.extend_from_slice(b" version=");
        buf.extend_from_slice(self.version.as_bytes());
        buf.extend_from_slice(b" bytes=");
        write_u64(buf, self.content_length.unwrap_or(0));
        buf.extend_from_slice(b" req_id=");
        buf.extend_from_slice(self.request_id.as_deref().unwrap_or("-").as_bytes());
        buf.extend_from_slice(b" route=");
        buf.extend_from_slice(self.route_id.as_deref().unwrap_or("-").as_bytes());
        buf.extend_from_slice(b" upstream=");
        buf.extend_from_slice(self.upstream.as_deref().unwrap_or("-").as_bytes());
        buf.extend_from_slice(b" latency_ms=");
        match self.latency_ms {
            Some(lat) => write_f64_2dp(buf, lat),
            None => buf.push(b'-'),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_json_byte_templating() {
        let record = LogRecord {
            timestamp_ms: 1700000000000,
            status: 200,
            version: "HTTP/2.0",
            content_length: Some(1024),
            request_id: Some("req-123".into()),
            route_id: Some("route-api".into()),
            upstream: Some("backend-v1".into()),
            latency_ms: Some(12.34),
        };

        let mut buf = Vec::with_capacity(256);
        record.write_json(&mut buf);

        let json_str = std::str::from_utf8(&buf).expect("Valid UTF-8 byte stream");
        assert!(json_str.contains("\"status\":200"));
        assert!(json_str.contains("\"request_id\":\"req-123\""));
        assert!(json_str.contains("\"latency_ms\":12.34"));
        assert!(json_str.starts_with('{') && json_str.ends_with('}'));
    }

    #[test]
    fn test_record_text_byte_templating() {
        let record = LogRecord {
            timestamp_ms: 1700000000000,
            status: 404,
            version: "HTTP/1.1",
            content_length: None,
            request_id: None,
            route_id: Some("route-404".into()),
            upstream: None,
            latency_ms: None,
        };

        let mut buf = Vec::with_capacity(256);
        record.write_text(&mut buf);

        let text_str = std::str::from_utf8(&buf).expect("Valid UTF-8 byte stream");
        assert!(text_str.contains("status=404"));
        assert!(text_str.contains("req_id=-"));
        assert!(text_str.contains("bytes=0"));
        assert!(text_str.contains("latency_ms=-"));
    }
}
