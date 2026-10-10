//! Pre-compiled pipeline discriminants per listener, resolved at bootstrap.
//!
//! Eliminates runtime protocol branching on the hot path.
//! Each listener is mapped to a single `TcpPipeline` or `UdpPipeline` variant
//! at compile time, so dispatch becomes a flat `match` on a lookup result.

use rustc_hash::FxHashMap;

use velda_core::{MemoryTier, ProtocolFamily, StreamingMode};
use velda_grpc::GrpcConfig;
use velda_http1::Http1Config;
use velda_http2::Http2Config;
use velda_http3::Http3Config;
use velda_sync::post_sync::listener::ListenerConfig;

use crate::error::EdgeError;

/// Pre-compiled TCP protocol discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpProtocol {
    /// HTTP/1.1 stream worker.
    Http1,
    /// HTTP/2 stream worker.
    Http2,
    /// Dual-ALPN HTTP family stream worker ("h2", "http/1.1").
    HttpAuto,
    /// gRPC stream worker.
    Grpc,
}

impl TcpProtocol {
    /// Canonical application protocol string for ALPN validation.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Http1 => "http1",
            Self::Http2 => "http2",
            Self::HttpAuto => "http",
            Self::Grpc => "grpc",
        }
    }

    /// Associated canonical protocol family.
    #[inline]
    pub const fn family(&self) -> ProtocolFamily {
        match self {
            Self::Http1 | Self::Http2 | Self::HttpAuto => ProtocolFamily::Http,
            Self::Grpc => ProtocolFamily::Grpc,
        }
    }
}

/// Pre-compiled UDP protocol discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpProtocol {
    /// HTTP/3 QUIC pipeline.
    Http3,
    /// gRPC over QUIC pipeline.
    Grpc,
}

impl UdpProtocol {
    /// Canonical application protocol string for ALPN validation.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Http3 => "http3",
            Self::Grpc => "grpc",
        }
    }

    /// Associated canonical protocol family.
    #[inline]
    pub const fn family(&self) -> ProtocolFamily {
        match self {
            Self::Http3 => ProtocolFamily::Http,
            Self::Grpc => ProtocolFamily::Grpc,
        }
    }
}

/// Fully compiled TCP listener pipeline containing protocol-specific configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpPipeline {
    Http1 {
        tls_enabled: bool,
        streaming: StreamingMode,
        config: Http1Config,
    },
    Http2 {
        tls_enabled: bool,
        streaming: StreamingMode,
        config: Http2Config,
    },
    HttpAuto {
        tls_enabled: bool,
        streaming: StreamingMode,
        http1_config: Http1Config,
        http2_config: Http2Config,
    },
    Grpc {
        tls_enabled: bool,
        streaming: StreamingMode,
        config: GrpcConfig,
    },
}

impl TcpPipeline {
    #[inline]
    pub const fn protocol(&self) -> TcpProtocol {
        match self {
            Self::Http1 { .. } => TcpProtocol::Http1,
            Self::Http2 { .. } => TcpProtocol::Http2,
            Self::HttpAuto { .. } => TcpProtocol::HttpAuto,
            Self::Grpc { .. } => TcpProtocol::Grpc,
        }
    }

    #[inline]
    pub const fn protocol_family(&self) -> ProtocolFamily {
        self.protocol().family()
    }

    #[inline]
    pub const fn tls_enabled(&self) -> bool {
        match self {
            Self::Http1 { tls_enabled, .. } => *tls_enabled,
            Self::Http2 { tls_enabled, .. } => *tls_enabled,
            Self::HttpAuto { tls_enabled, .. } => *tls_enabled,
            Self::Grpc { tls_enabled, .. } => *tls_enabled,
        }
    }

    #[inline]
    pub const fn streaming(&self) -> StreamingMode {
        match self {
            Self::Http1 { streaming, .. } => *streaming,
            Self::Http2 { streaming, .. } => *streaming,
            Self::HttpAuto { streaming, .. } => *streaming,
            Self::Grpc { streaming, .. } => *streaming,
        }
    }
}

/// Fully compiled UDP listener pipeline containing protocol-specific configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UdpPipeline {
    Http3 {
        tls_enabled: bool,
        streaming: StreamingMode,
        config: Http3Config,
    },
    Grpc {
        tls_enabled: bool,
        streaming: StreamingMode,
        config: GrpcConfig,
    },
}

impl UdpPipeline {
    #[inline]
    pub const fn protocol(&self) -> UdpProtocol {
        match self {
            Self::Http3 { .. } => UdpProtocol::Http3,
            Self::Grpc { .. } => UdpProtocol::Grpc,
        }
    }

    #[inline]
    pub const fn protocol_family(&self) -> ProtocolFamily {
        self.protocol().family()
    }

    #[inline]
    pub const fn tls_enabled(&self) -> bool {
        match self {
            Self::Http3 { tls_enabled, .. } => *tls_enabled,
            Self::Grpc { tls_enabled, .. } => *tls_enabled,
        }
    }

    #[inline]
    pub const fn streaming(&self) -> StreamingMode {
        match self {
            Self::Http3 { streaming, .. } => *streaming,
            Self::Grpc { streaming, .. } => *streaming,
        }
    }
}

/// Lookup table mapping listener_id → pre-compiled pipeline.
///
/// Built once during bootstrap/reload and stored in `Runtime` for single-lookup O(1) hot-path resolution.
#[derive(Debug, Clone, Default)]
pub struct PipelineTable {
    tcp: FxHashMap<String, TcpPipeline>,
    udp: FxHashMap<String, UdpPipeline>,
}

impl PipelineTable {
    /// Compiles a pipeline table from listener configurations using default hardware topology memory tier.
    pub fn build(listeners: &[ListenerConfig]) -> Result<Self, EdgeError> {
        let tier = velda_core::global_hardware_topology().memory_tier();
        Self::build_with_tier(listeners, tier)
    }

    /// Compiles a pipeline table from listener configurations using the explicit memory tier.
    ///
    /// Resolves protocol limits from the configuration or falls back deterministically
    /// by the provided [`MemoryTier`].
    pub fn build_with_tier(
        listeners: &[ListenerConfig],
        tier: MemoryTier,
    ) -> Result<Self, EdgeError> {
        let mut tcp = FxHashMap::default();
        let mut udp = FxHashMap::default();

        for listener in listeners {
            let app = listener.application.protocol.to_ascii_lowercase();

            // L4 raw listeners do not enter L7 dispatch
            if app == "raw" {
                continue;
            }

            let is_tls = listener.tls.enabled;
            let streaming = listener.application.streaming;
            let transport = listener.transport.protocol.to_ascii_lowercase();

            if transport == "udp" {
                match app.as_str() {
                    "http" | "http3" => {
                        let config = resolve_http3_config(listener, tier);
                        udp.insert(
                            listener.id.clone(),
                            UdpPipeline::Http3 {
                                tls_enabled: is_tls,
                                streaming,
                                config,
                            },
                        );
                    }
                    "grpc" => {
                        let config = resolve_grpc_config(listener, tier);
                        udp.insert(
                            listener.id.clone(),
                            UdpPipeline::Grpc {
                                tls_enabled: is_tls,
                                streaming,
                                config,
                            },
                        );
                    }
                    _ => {
                        return Err(EdgeError::InvalidConfig {
                            detail: format!(
                                "listener '{}': unsupported UDP application protocol '{}'; must be 'http', 'http3', or 'grpc'",
                                listener.id, app
                            ),
                        });
                    }
                }
            } else {
                match app.as_str() {
                    "http" => {
                        let http1_config = resolve_http1_config(listener, tier);
                        let http2_config = resolve_http2_config(listener, tier);
                        if is_tls {
                            tcp.insert(
                                listener.id.clone(),
                                TcpPipeline::HttpAuto {
                                    tls_enabled: true,
                                    streaming,
                                    http1_config,
                                    http2_config,
                                },
                            );
                        } else {
                            tcp.insert(
                                listener.id.clone(),
                                TcpPipeline::Http1 {
                                    tls_enabled: false,
                                    streaming,
                                    config: http1_config,
                                },
                            );
                        }
                    }
                    "http1" => {
                        let config = resolve_http1_config(listener, tier);
                        tcp.insert(
                            listener.id.clone(),
                            TcpPipeline::Http1 {
                                tls_enabled: is_tls,
                                streaming,
                                config,
                            },
                        );
                    }
                    "http2" => {
                        let config = resolve_http2_config(listener, tier);
                        tcp.insert(
                            listener.id.clone(),
                            TcpPipeline::Http2 {
                                tls_enabled: is_tls,
                                streaming,
                                config,
                            },
                        );
                    }
                    "grpc" => {
                        let config = resolve_grpc_config(listener, tier);
                        tcp.insert(
                            listener.id.clone(),
                            TcpPipeline::Grpc {
                                tls_enabled: is_tls,
                                streaming,
                                config,
                            },
                        );
                    }
                    "http3" => {
                        return Err(EdgeError::InvalidConfig {
                            detail: format!(
                                "listener '{}': HTTP/3 requires UDP transport, not TCP",
                                listener.id,
                            ),
                        });
                    }
                    _ => {
                        return Err(EdgeError::InvalidConfig {
                            detail: format!(
                                "listener '{}': unsupported application protocol '{}'; must be 'http', 'http1', 'http2', 'http3', or 'grpc'",
                                listener.id, app
                            ),
                        });
                    }
                }
            }
        }

        Ok(Self { tcp, udp })
    }

    /// Returns the pre-compiled TCP pipeline for a listener, if registered.
    #[inline]
    pub fn tcp_pipeline(&self, listener_id: &str) -> Option<&TcpPipeline> {
        self.tcp.get(listener_id)
    }

    /// Returns the pre-compiled UDP pipeline for a listener, if registered.
    #[inline]
    pub fn udp_pipeline(&self, listener_id: &str) -> Option<&UdpPipeline> {
        self.udp.get(listener_id)
    }
}

fn resolve_http1_config(listener: &ListenerConfig, tier: MemoryTier) -> Http1Config {
    let mut config = Http1Config::for_tier(tier);
    if let Some(ref h1) = listener.http1 {
        if let Some(v) = h1.max_body_size {
            config.max_body_size = v;
        }
        if let Some(v) = h1.max_header_size {
            config.max_header_size = v;
        }
        if let Some(v) = h1.max_headers {
            config.max_headers = v;
        }
        if let Some(v) = h1.idle_timeout_ms {
            config.idle_timeout_ms = v;
        }
        if let Some(v) = h1.max_keepalive_requests {
            config.max_keepalive_requests = v;
        }
        if let Some(v) = h1.header_read_timeout_ms {
            config.header_read_timeout_ms = v;
        }
        if let Some(v) = h1.client_body_buffer_size {
            config.client_body_buffer_size = v;
        }
    }
    config
}

fn resolve_http2_config(listener: &ListenerConfig, tier: MemoryTier) -> Http2Config {
    let mut config = Http2Config::for_tier(tier);
    if let Some(ref h2) = listener.http2 {
        if let Some(v) = h2.max_body_size {
            config.max_body_size = v;
        }
        if let Some(v) = h2.max_header_size {
            config.max_header_size = v;
            config.max_header_list_size = v as u32;
        }
        if let Some(v) = h2.max_headers {
            config.max_headers = v;
        }
        if let Some(v) = h2.idle_timeout_ms {
            config.idle_timeout_ms = v;
        }
        if let Some(v) = h2.max_concurrent_streams {
            config.max_concurrent_streams = v;
        }
        if let Some(v) = h2.initial_connection_window_size {
            config.initial_connection_window_size = v;
        }
        if let Some(v) = h2.initial_stream_window_size {
            config.initial_stream_window_size = v;
        }
        if let Some(v) = h2.max_frame_size {
            config.max_frame_size = v;
        }
        if let Some(v) = h2.enable_push {
            config.enable_push = v;
        }
        if let Some(v) = h2.max_consecutive_resets {
            config.max_consecutive_resets = v;
        }
        if let Some(v) = h2.max_pending_control_frames {
            config.max_pending_control_frames = v;
        }
        if let Some(v) = h2.max_continuation_frames {
            config.max_continuation_frames = v;
        }
        if let Some(v) = h2.max_send_buffer_size {
            config.max_send_buffer_size = v;
        }
        if let Some(v) = h2.max_requests_per_connection {
            config.max_requests_per_connection = v;
        }
        if let Some(v) = h2.max_connection_duration_secs {
            config.max_connection_duration_secs = v;
        }
        if let Some(v) = h2.alt_svc_port {
            config.alt_svc_port = Some(v);
        }
    }
    config
}

fn resolve_grpc_config(listener: &ListenerConfig, tier: MemoryTier) -> GrpcConfig {
    let mut config = GrpcConfig::for_tier(tier);
    if let Some(ref grpc) = listener.grpc {
        if let Some(v) = grpc.max_message_size {
            config.max_message_size = v;
        }
        if let Some(v) = grpc.max_header_size {
            config.max_header_size = v;
        }
        if let Some(v) = grpc.max_headers {
            config.max_headers = v;
        }
        if let Some(v) = grpc.idle_timeout_ms {
            config.idle_timeout_ms = v;
        }
        if let Some(v) = grpc.max_concurrent_streams {
            config.max_concurrent_streams = v;
        }
        if let Some(v) = grpc.keepalive_ping_interval_ms {
            config.keepalive_ping_interval_ms = v;
        }
        if let Some(v) = grpc.keepalive_ping_timeout_ms {
            config.keepalive_ping_timeout_ms = v;
        }
        if let Some(v) = grpc.max_call_duration_ms {
            config.max_call_duration_ms = v;
        }
    }
    config
}

fn resolve_http3_config(listener: &ListenerConfig, tier: MemoryTier) -> Http3Config {
    let mut config = Http3Config::for_tier(tier);
    if let Some(ref h3) = listener.http3 {
        if let Some(v) = h3.max_body_size {
            config.max_body_size = v;
        }
        if let Some(v) = h3.max_header_size {
            config.max_header_size = v;
        }
        if let Some(v) = h3.max_headers {
            config.max_headers = v;
        }
        if let Some(v) = h3.idle_timeout_ms {
            config.idle_timeout_ms = v;
        }
        if let Some(v) = h3.max_concurrent_streams {
            config.max_concurrent_streams = v;
        }
        if let Some(v) = h3.max_concurrent_uni_streams {
            config.max_concurrent_uni_streams = v;
        }
        if let Some(v) = h3.max_qpack_table_capacity {
            config.max_qpack_table_capacity = v;
        }
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_sync::post_sync::listener::{
        ListenerApplicationConfig, ListenerTlsConfig, ListenerTransportConfig,
    };

    fn cfg(
        id: &str,
        transport: &str,
        app: &str,
        version: Option<&str>,
        tls: bool,
    ) -> ListenerConfig {
        ListenerConfig {
            id: id.into(),
            address: "0.0.0.0:80".into(),
            transport: ListenerTransportConfig {
                protocol: transport.into(),
            },
            application: ListenerApplicationConfig {
                protocol: app.into(),
                version: version.map(String::from),
                streaming: velda_sync::StreamingMode::DISABLED,
            },
            tls: ListenerTlsConfig { enabled: tls },
            http1: None,
            http2: None,
            grpc: None,
            http3: None,
            raw: None,
        }
    }

    #[test]
    fn test_build_pipeline_table() {
        let listeners = vec![
            cfg("h1-clear", "tcp", "http1", None, false),
            cfg("h2-tls", "tcp", "http2", None, true),
            cfg("grpc-clear", "tcp", "grpc", None, false),
            cfg("h3-udp", "udp", "http3", None, false),
            cfg("raw-tcp", "tcp", "raw", None, false),
        ];

        let table = PipelineTable::build(&listeners).unwrap();

        let h1 = table.tcp_pipeline("h1-clear").unwrap();
        assert_eq!(h1.protocol(), TcpProtocol::Http1);
        assert_eq!(h1.protocol_family(), ProtocolFamily::Http);
        assert!(!h1.tls_enabled());
        assert_eq!(h1.protocol().as_str(), "http1");

        let h2 = table.tcp_pipeline("h2-tls").unwrap();
        assert_eq!(h2.protocol(), TcpProtocol::Http2);
        assert_eq!(h2.protocol_family(), ProtocolFamily::Http);
        assert!(h2.tls_enabled());
        assert_eq!(h2.protocol().as_str(), "http2");

        let grpc = table.tcp_pipeline("grpc-clear").unwrap();
        assert_eq!(grpc.protocol(), TcpProtocol::Grpc);
        assert_eq!(grpc.protocol_family(), ProtocolFamily::Grpc);
        assert!(!grpc.tls_enabled());

        let h3 = table.udp_pipeline("h3-udp").unwrap();
        assert_eq!(h3.protocol(), UdpProtocol::Http3);
        assert_eq!(h3.protocol_family(), ProtocolFamily::Http);
        assert!(!h3.tls_enabled());

        assert!(table.tcp_pipeline("raw-tcp").is_none());
        assert!(table.udp_pipeline("raw-tcp").is_none());
    }

    #[test]
    fn test_http_family_dual_alpn() {
        let listeners = vec![
            cfg("web-tls", "tcp", "http", None, true),
            cfg("web-clear", "tcp", "http", None, false),
            cfg("web-udp", "udp", "http", None, true),
        ];

        let table = PipelineTable::build(&listeners).unwrap();

        // TLS HTTP family listener compiles to HttpAuto with Dual-ALPN
        let web_tls = table.tcp_pipeline("web-tls").unwrap();
        assert_eq!(web_tls.protocol(), TcpProtocol::HttpAuto);
        assert_eq!(web_tls.protocol_family(), ProtocolFamily::Http);
        assert!(web_tls.tls_enabled());

        // Cleartext HTTP family listener compiles to Http1
        let web_clear = table.tcp_pipeline("web-clear").unwrap();
        assert_eq!(web_clear.protocol(), TcpProtocol::Http1);
        assert_eq!(web_clear.protocol_family(), ProtocolFamily::Http);
        assert!(!web_clear.tls_enabled());

        // UDP HTTP family listener compiles to Http3
        let web_udp = table.udp_pipeline("web-udp").unwrap();
        assert_eq!(web_udp.protocol(), UdpProtocol::Http3);
        assert_eq!(web_udp.protocol_family(), ProtocolFamily::Http);
    }

    #[test]
    fn test_http3_on_tcp_fails() {
        let listeners = vec![cfg("bad-h3", "tcp", "http3", None, false)];
        assert!(PipelineTable::build(&listeners).is_err());
    }

    #[test]
    fn test_explicit_tier() {
        let listeners = vec![
            cfg("h1", "tcp", "http1", None, false),
            cfg("h2", "tcp", "http2", None, false),
        ];

        let table = PipelineTable::build_with_tier(&listeners, MemoryTier::Ultra).unwrap();
        assert!(table.tcp_pipeline("h1").is_some());
        assert!(table.tcp_pipeline("h2").is_some());
    }
}
