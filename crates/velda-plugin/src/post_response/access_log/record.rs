//! Access Log Record Structure and Serializers.
//!
//! Provides structured, strongly typed access log entries with zero-blocking JSON and Text serialization.

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

impl LogRecord {
    /// Serializes this record into a structured JSON string.
    pub fn to_json_string(&self) -> String {
        serde_json::json!({
            "timestamp": self.timestamp_ms,
            "status": self.status,
            "version": self.version,
            "content_length": self.content_length,
            "request_id": self.request_id,
            "route_id": self.route_id,
            "upstream": self.upstream,
            "latency_ms": self.latency_ms,
        })
        .to_string()
    }

    /// Serializes this record into a human-readable, single-line log format.
    pub fn to_text_string(&self) -> String {
        format!(
            "[{}] status={} version={} bytes={} req_id={} route={} upstream={} latency_ms={}",
            self.timestamp_ms,
            self.status,
            self.version,
            self.content_length.unwrap_or(0),
            self.request_id.as_deref().unwrap_or("-"),
            self.route_id.as_deref().unwrap_or("-"),
            self.upstream.as_deref().unwrap_or("-"),
            self.latency_ms
                .map_or("-".to_string(), |l| format!("{:.2}", l)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_json_serialization() {
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

        let json_str = record.to_json_string();
        assert!(json_str.contains("\"status\":200"));
        assert!(json_str.contains("\"request_id\":\"req-123\""));
    }

    #[test]
    fn test_record_text_serialization() {
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

        let text_str = record.to_text_string();
        assert!(text_str.contains("status=404"));
        assert!(text_str.contains("req_id=-"));
    }
}
