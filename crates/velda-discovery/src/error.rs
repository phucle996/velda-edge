//! Error types for endpoint discovery.

use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;

/// Result alias for discovery operations.
pub type Result<T> = std::result::Result<T, DiscoveryError>;

/// Errors encountered during endpoint topology discovery.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DiscoveryError {
    /// DNS resolution failure across all configured upstream nameservers.
    #[error("DNS resolution failed for '{host}': {reason}")]
    DnsResolutionFailed { host: String, reason: String },

    /// Negative cache hit: domain previously returned NXDOMAIN.
    #[error("DNS resolution blocked by negative cache for '{host}' (NXDOMAIN)")]
    NegativeCacheHit { host: String },

    /// Authoritative NXDOMAIN from a nameserver.
    ///
    /// `negative_ttl` is derived from the response's SOA record (RFC 2308) when the
    /// server supplied one, so the resolver can cache the absence for as long as the
    /// zone owner allows instead of a fixed guess.
    #[error("DNS name '{host}' does not exist (NXDOMAIN)")]
    NameNotFound {
        host: String,
        negative_ttl: Option<Duration>,
    },

    /// Failure to connect or query an individual DNS nameserver.
    #[error("DNS server '{address}' unreachable: {reason}")]
    ServerUnreachable { address: SocketAddr, reason: String },

    /// Failure to resolve DNS server address from host/domain target.
    #[error("Failed to resolve DNS server target '{target}': {reason}")]
    ServerTargetResolutionFailed { target: String, reason: String },

    /// Error reading or parsing the local `/etc/hosts` bootstrap file.
    #[error("Hosts file error at '{path}': {reason}")]
    HostsParsingFailed { path: String, reason: String },

    /// No DNS servers configured for resolution.
    #[error("No DNS nameservers available for resolution")]
    EmptyServerList,
}
