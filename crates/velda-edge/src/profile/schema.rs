//! Data Transfer Objects (DTOs) and Serde schemas for `runtime.json`.

use serde::{Deserialize, Serialize};

/// Hardware information snapshot recorded in `runtime.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HardwareProfile {
    pub detected_ram_bytes: usize,
    pub detected_cores: usize,
    pub worker_threads: usize,
    pub cpu_tier: String,
    pub memory_tier: String,
    pub tier: String,
}

/// Discovery subsystem tuning parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiscoveryRuntimeConfig {
    pub max_dns_cache_capacity: usize,
    pub max_lkg_capacity: usize,
    #[serde(default = "default_max_negative_ttl_secs")]
    pub max_negative_ttl_secs: u64,
    pub query_timeout_ms: u64,
    pub negative_ttl_secs: u64,
    pub positive_ttl_secs: u64,
}

pub(crate) const fn default_max_negative_ttl_secs() -> u64 {
    60
}

pub(crate) const fn default_reuseport() -> bool {
    cfg!(unix)
}

pub(crate) const fn default_concurrency_shards() -> usize {
    1
}

pub(crate) const fn default_true() -> bool {
    true
}

pub(crate) const fn default_session_shards() -> usize {
    16
}

/// TCP-specific runtime configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TcpRuntimeConfig {
    pub backlog: u32,
    pub nodelay: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepalive_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recv_buffer_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_buffer_size: Option<usize>,
    pub copy_buffer_size: usize,
    #[serde(default = "default_reuseport")]
    pub reuseport: bool,
    #[serde(default = "default_concurrency_shards")]
    pub concurrency_shards: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quickack: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defer_accept_secs: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fastopen_backlog: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub busy_poll_us: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incoming_cpu: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notsent_lowat: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freebind: Option<bool>,
}

/// UDP-specific runtime configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UdpRuntimeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recv_buffer_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_buffer_size: Option<usize>,
    #[serde(default = "default_reuseport")]
    pub reuseport: bool,
    #[serde(default = "default_concurrency_shards")]
    pub concurrency_shards: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freebind: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gro: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rxq_ovfl: Option<bool>,
}

/// Transport subsystem tuning parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportRuntimeConfig {
    /// Number of network I/O event loop workers.
    pub io_workers: usize,
    /// Maximum concurrent active connections allowed before L4 rate-limiting/drop to protect RAM.
    pub max_active_connections: usize,
    /// Depth of the administrative reconciliation command channel.
    pub reconcile_channel_capacity: usize,
    /// Enforce CPU core pinning for Tokio network worker threads.
    #[serde(default = "default_true")]
    pub cpu_pinning: bool,
    /// TCP socket options and buffer sizing.
    pub tcp: TcpRuntimeConfig,
    /// UDP socket options and buffer sizing.
    pub udp: UdpRuntimeConfig,
}

/// Downstream TLS termination tuning parameters recorded in `runtime.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TlsRuntimeConfig {
    /// In-memory TLS session resumption cache capacity.
    pub session_cache_capacity: usize,
    /// Number of cache-aligned shards partitioning session storage.
    #[serde(default = "default_session_shards")]
    pub session_shards: usize,
    /// Maximum size in bytes of TLS 1.3 0-RTT early data accepted from clients.
    pub max_early_data_size: u32,
    /// Number of single-use TLS 1.3 session tickets issued per handshake.
    pub send_tls13_tickets: usize,
    /// Maximum downstream TLS handshake timeout in seconds.
    pub handshake_timeout_secs: u64,
}

impl TlsRuntimeConfig {
    /// Converts this runtime configuration to a strongly-typed [`velda_tls::TlsServerParams`].
    pub fn to_tls_server_params(&self) -> velda_tls::TlsServerParams {
        velda_tls::TlsServerParams {
            session_cache_capacity: self.session_cache_capacity,
            session_shards: self.session_shards,
            max_early_data_size: self.max_early_data_size,
            send_tls13_tickets: self.send_tls13_tickets,
            handshake_timeout: std::time::Duration::from_secs(self.handshake_timeout_secs),
        }
    }
}
