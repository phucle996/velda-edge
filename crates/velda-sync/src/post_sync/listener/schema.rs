use serde::{Deserialize, Serialize};

// ============================================================================
// Binary Protocol Constants & Domain Header
// ============================================================================

pub const MAGIC_BYTES: [u8; 4] = *b"VLDA";
pub const CURRENT_FORMAT_VERSION: u32 = 1;
pub const CURRENT_COMPILER_VERSION: u32 = 1;
pub const TAG_LISTENERS: [u8; 8] = *b"LISTENER";

/// Self-describing binary header prepended to listeners.bin.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainHeader {
    pub magic: [u8; 4],
    pub format_version: u32,
    pub compiler_version: u32,
    pub schema_version: u32,
    pub domain_tag: [u8; 8],
    pub revision: u64,
    pub source_checksum: [u8; 32],
    pub payload_size: u64,
    pub payload_checksum: [u8; 32],
}

pub const DOMAIN_HEADER_SIZE: usize = std::mem::size_of::<DomainHeader>();

// ============================================================================
// Phase 1: Entity & Schema Definitions
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenersFile {
    pub schema_version: u32,
    pub listeners: Vec<ListenerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenerTransportConfig {
    pub protocol: String, // "tcp", "udp"
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenerApplicationConfig {
    pub protocol: String, // Canonical Protocol Family: "http", "grpc", "raw"
    #[serde(default)]
    pub version: Option<String>,
    pub streaming: velda_core::StreamingMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Http1ListenerConfig {
    /// Maximum body size in bytes. Strictly enforced ONLY for `Buffered` strategy.
    /// Streaming strategies scale dynamically with constant memory footprint.
    #[serde(default)]
    pub max_body_size: Option<usize>,
    #[serde(default)]
    pub max_header_size: Option<usize>,
    #[serde(default)]
    pub max_headers: Option<usize>,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_keepalive_requests: Option<u32>,
    #[serde(default)]
    pub header_read_timeout_ms: Option<u64>,
    #[serde(default)]
    pub client_body_buffer_size: Option<usize>,
    #[serde(default)]
    pub initial_buffer_capacity: Option<usize>,
    #[serde(default)]
    pub shrink_threshold: Option<usize>,
    #[serde(default)]
    pub alt_svc: Option<AltSvcConfig>,
    #[serde(default)]
    pub alt_svc_port: Option<u16>,
}

impl Http1ListenerConfig {
    /// Returns effective Alt-Svc configuration, preferring structured `alt_svc` over legacy `alt_svc_port`.
    pub fn effective_alt_svc(&self) -> Option<AltSvcConfig> {
        self.alt_svc
            .or_else(|| self.alt_svc_port.map(AltSvcConfig::new))
    }
}

const fn default_alt_svc_max_age() -> u32 {
    86400
}

/// Structured Alt-Svc configuration for advertising HTTP/3 over QUIC (RFC 9114 §3.1 & RFC 7838).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AltSvcConfig {
    /// Target UDP port for HTTP/3 (e.g. 443, 8443).
    pub port: u16,
    /// Freshness lifetime in seconds (`ma` parameter). Defaults to 86400 (24 hours).
    #[serde(default = "default_alt_svc_max_age")]
    pub max_age: u32,
    /// Whether this alternative service is persistent across network changes. Defaults to false.
    #[serde(default)]
    pub persist: bool,
}

impl AltSvcConfig {
    pub const fn new(port: u16) -> Self {
        Self {
            port,
            max_age: 86400,
            persist: false,
        }
    }

    pub const fn with_max_age(mut self, max_age: u32) -> Self {
        self.max_age = max_age;
        self
    }

    pub const fn with_persist(mut self, persist: bool) -> Self {
        self.persist = persist;
        self
    }

    /// Formats this configuration into a canonical Alt-Svc header value string.
    pub fn to_header_value_string(&self) -> String {
        if self.persist {
            format!("h3=\":{}\"; ma={}; persist=1", self.port, self.max_age)
        } else {
            format!("h3=\":{}\"; ma={}", self.port, self.max_age)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Http2ListenerConfig {
    /// Maximum body size in bytes. Strictly enforced ONLY for `Buffered` strategy.
    /// Streaming strategies scale dynamically with constant memory footprint.
    #[serde(default)]
    pub max_body_size: Option<usize>,
    #[serde(default)]
    pub max_header_size: Option<usize>,
    #[serde(default)]
    pub max_headers: Option<usize>,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_concurrent_streams: Option<u32>,
    #[serde(default)]
    pub initial_connection_window_size: Option<u32>,
    #[serde(default)]
    pub initial_stream_window_size: Option<u32>,
    #[serde(default)]
    pub max_frame_size: Option<u32>,
    #[serde(default)]
    pub enable_push: Option<bool>,
    #[serde(default)]
    pub max_consecutive_resets: Option<u32>,
    #[serde(default)]
    pub max_pending_control_frames: Option<u32>,
    #[serde(default)]
    pub max_continuation_frames: Option<u32>,
    #[serde(default)]
    pub max_send_buffer_size: Option<usize>,
    #[serde(default)]
    pub max_requests_per_connection: Option<u32>,
    #[serde(default)]
    pub max_connection_duration_secs: Option<u32>,
    #[serde(default)]
    pub alt_svc: Option<AltSvcConfig>,
    #[serde(default)]
    pub alt_svc_port: Option<u16>,
}

impl Http2ListenerConfig {
    /// Returns effective Alt-Svc configuration, preferring structured `alt_svc` over legacy `alt_svc_port`.
    pub fn effective_alt_svc(&self) -> Option<AltSvcConfig> {
        self.alt_svc
            .or_else(|| self.alt_svc_port.map(AltSvcConfig::new))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GrpcListenerConfig {
    #[serde(default)]
    pub max_message_size: Option<usize>,
    #[serde(default)]
    pub max_header_size: Option<usize>,
    #[serde(default)]
    pub max_headers: Option<usize>,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_concurrent_streams: Option<u32>,
    #[serde(default)]
    pub keepalive_ping_interval_ms: Option<u64>,
    #[serde(default)]
    pub keepalive_ping_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_call_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Http3ListenerConfig {
    /// Maximum body size in bytes. Strictly enforced ONLY for `Buffered` strategy.
    /// Streaming strategies scale dynamically with constant memory footprint.
    #[serde(default)]
    pub max_body_size: Option<usize>,
    #[serde(default)]
    pub max_header_size: Option<usize>,
    #[serde(default)]
    pub max_headers: Option<usize>,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_concurrent_streams: Option<u32>,
    #[serde(default)]
    pub max_concurrent_uni_streams: Option<u32>,
    #[serde(default)]
    pub max_qpack_table_capacity: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RawListenerConfig {
    #[serde(default)]
    pub copy_buffer_size: Option<usize>,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenerConfig {
    pub id: String,
    pub address: String, // e.g. "0.0.0.0:80" or "0.0.0.0:443"
    pub transport: ListenerTransportConfig,
    pub application: ListenerApplicationConfig,
    pub tls: ListenerTlsConfig,
    #[serde(default)]
    pub http1: Option<Http1ListenerConfig>,
    #[serde(default)]
    pub http2: Option<Http2ListenerConfig>,
    #[serde(default)]
    pub grpc: Option<GrpcListenerConfig>,
    #[serde(default)]
    pub http3: Option<Http3ListenerConfig>,
    #[serde(default)]
    pub raw: Option<RawListenerConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListenerTlsConfig {
    pub enabled: bool,
}
