//! Configuration Contract for Standard gRPC Access Log (`std_grpc_access_log::config`).

use super::record::GrpcLogFormat;
use super::sampling::GrpcSamplingTable;

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

/// Configuration settings for the Standard gRPC Access Log plugin (`std_grpc_access_log`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StdGrpcAccessLogConfig {
    /// Target standard stream (stdout or stderr).
    pub target: TargetStream,
    /// Output serialization format (JSON or Text).
    pub format: GrpcLogFormat,
    /// Extracts and records the canonical `x-request-id` header from request workspace.
    pub include_request_id: bool,
    /// Extracts and records the matched route identifier.
    pub include_route_id: bool,
    /// Extracts and records the target upstream address.
    pub include_upstream: bool,
    /// Extracts and records the client peer socket address.
    pub include_peer_addr: bool,
    /// Extracts and records RPC duration.
    pub include_latency: bool,
    /// Capacity of the per-shard lock-free ring buffer.
    pub buffer_capacity: usize,
    /// Status-aware sampling table for canonical gRPC status codes.
    pub sampling: GrpcSamplingTable,
}

impl Default for StdGrpcAccessLogConfig {
    fn default() -> Self {
        Self {
            target: TargetStream::Stdout,
            format: GrpcLogFormat::Json,
            include_request_id: true,
            include_route_id: true,
            include_upstream: true,
            include_peer_addr: true,
            include_latency: true,
            buffer_capacity: 4096,
            sampling: GrpcSamplingTable::default(),
        }
    }
}
