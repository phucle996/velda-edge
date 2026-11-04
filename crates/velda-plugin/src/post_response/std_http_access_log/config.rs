//! Configuration Contract for Standard HTTP Access Log (`std_http_access_log::config`).

use super::record::LogFormat;
use super::sampling::SamplingTable;

/// Target standard stream for log emission. Direct file logging is forbidden.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TargetStream {
    #[default]
    Stdout,
    Stderr,
}

impl TargetStream {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "stdout" => Ok(Self::Stdout),
            "stderr" => Ok(Self::Stderr),
            other => Err(format!(
                "Invalid target stream '{other}': expected 'stdout' or 'stderr' (direct file logging is forbidden)"
            )),
        }
    }
}

/// Configuration settings for the Standard HTTP Access Log plugin (`std_http_access_log`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StdHttpAccessLogConfig {
    /// Target standard stream (stdout or stderr).
    pub target: TargetStream,
    /// Output serialization format (JSON, Text, or CLF).
    pub format: LogFormat,
    /// Extracts and records the client User-Agent header inline up to 192 bytes.
    pub include_user_agent: bool,
    /// Extracts and records the canonical `x-request-id` header from request workspace.
    pub include_request_id: bool,
    /// Extracts and records the matched route identifier.
    pub include_route_id: bool,
    /// Extracts and records the target upstream address.
    pub include_upstream: bool,
    /// Extracts and records the request latency breakdown.
    pub include_latency: bool,
    /// Capacity of the per-shard lock-free ring buffer.
    pub buffer_capacity: usize,
    /// Status-aware sampling table. Unconfigured status codes have rate = 0.0 (never logged).
    pub sampling: SamplingTable,
}

impl Default for StdHttpAccessLogConfig {
    fn default() -> Self {
        Self {
            target: TargetStream::Stdout,
            format: LogFormat::Json,
            include_user_agent: true,
            include_request_id: true,
            include_route_id: true,
            include_upstream: true,
            include_latency: true,
            buffer_capacity: 4096,
            sampling: SamplingTable::default(),
        }
    }
}
