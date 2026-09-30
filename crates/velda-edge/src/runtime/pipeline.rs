//! Pre-compiled pipeline discriminants per listener, resolved at bootstrap.
//!
//! Eliminates runtime protocol branching on the hot path.
//! Each listener is mapped to a single `TcpPipeline` or `UdpPipeline` variant
//! at compile time, so dispatch becomes a flat `match` on a lookup result.

use std::collections::HashMap;

use velda_composer::ApplicationProtocol;
use velda_sync::post_sync::listener::ListenerConfig;

use crate::error::EdgeError;

/// Pre-compiled TCP pipeline discriminant.
///
/// Encodes both the transport mode (cleartext vs TLS) and the application
/// protocol, so the dispatcher can match a single enum variant without
/// nested if-else chains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpPipeline {
    /// Cleartext HTTP/1.1 stream worker.
    CleartextHttp1,
    /// Cleartext HTTP/2 stream worker.
    CleartextHttp2,
    /// Cleartext gRPC stream worker.
    CleartextGrpc,
    /// TLS-terminated HTTP/1.1 stream worker.
    TlsHttp1,
    /// TLS-terminated HTTP/2 stream worker.
    TlsHttp2,
    /// TLS-terminated gRPC stream worker.
    TlsGrpc,
}

/// Pre-compiled UDP pipeline discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpPipeline {
    /// HTTP/3 QUIC pipeline.
    Http3,
    /// gRPC over QUIC pipeline.
    Grpc,
}

/// Lookup table mapping listener_id → pre-compiled pipeline.
///
/// Built once during bootstrap/reload and stored in `Runtime` for O(1) hot-path lookup.
#[derive(Debug, Clone, Default)]
pub struct PipelineTable {
    tcp: HashMap<String, TcpPipeline>,
    udp: HashMap<String, UdpPipeline>,
}

impl PipelineTable {
    /// Compiles a pipeline table from listener configurations.
    ///
    /// Each listener is mapped to exactly one pipeline variant based on its
    /// declared transport, application protocol, and TLS configuration.
    pub fn build(listeners: &[ListenerConfig]) -> Result<Self, EdgeError> {
        let mut tcp = HashMap::new();
        let mut udp = HashMap::new();

        for listener in listeners {
            let app = listener.application.protocol.to_ascii_lowercase();

            // L4 raw listeners do not enter L7 dispatch
            if app == "raw" {
                continue;
            }

            let is_tls = listener.tls.enabled;
            let transport = listener.transport.protocol.to_ascii_lowercase();

            if transport == "udp" {
                let pipeline = if app == "grpc" {
                    UdpPipeline::Grpc
                } else {
                    UdpPipeline::Http3
                };
                udp.insert(listener.id.clone(), pipeline);
            } else {
                // Resolve application protocol to concrete variant
                let proto = Self::resolve_app_protocol(&listener.id, &app)?;

                let pipeline = match (proto, is_tls) {
                    (ApplicationProtocol::Http1, false) => TcpPipeline::CleartextHttp1,
                    (ApplicationProtocol::Http1, true) => TcpPipeline::TlsHttp1,
                    (ApplicationProtocol::Http2, false) => TcpPipeline::CleartextHttp2,
                    (ApplicationProtocol::Http2, true) => TcpPipeline::TlsHttp2,
                    (ApplicationProtocol::Grpc, false) => TcpPipeline::CleartextGrpc,
                    (ApplicationProtocol::Grpc, true) => TcpPipeline::TlsGrpc,
                    (ApplicationProtocol::Http3, _) => {
                        return Err(EdgeError::InvalidConfig {
                            detail: format!(
                                "listener '{}': HTTP/3 requires UDP transport, not TCP",
                                listener.id,
                            ),
                        });
                    }
                };
                tcp.insert(listener.id.clone(), pipeline);
            }
        }

        Ok(Self { tcp, udp })
    }

    /// Resolves application protocol string into a typed variant.
    fn resolve_app_protocol(
        listener_id: &str,
        app: &str,
    ) -> Result<ApplicationProtocol, EdgeError> {
        ApplicationProtocol::from_str_proto(app).ok_or_else(|| EdgeError::InvalidConfig {
            detail: format!(
                "listener '{}': unsupported application protocol '{}'; must be 'http1', 'http2', 'http3', or 'grpc'",
                listener_id, app
            ),
        })
    }

    /// Returns the pre-compiled TCP pipeline for a listener, if registered.
    #[inline]
    pub fn tcp_pipeline(&self, listener_id: &str) -> Option<TcpPipeline> {
        self.tcp.get(listener_id).copied()
    }

    /// Returns the pre-compiled UDP pipeline for a listener, if registered.
    #[inline]
    pub fn udp_pipeline(&self, listener_id: &str) -> Option<UdpPipeline> {
        self.udp.get(listener_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_sync::post_sync::listener::{
        ListenerApplicationConfig, ListenerLimitsConfig, ListenerTlsConfig, ListenerTransportConfig,
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
            },
            tls: ListenerTlsConfig { enabled: tls },
            limits: ListenerLimitsConfig::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
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

        assert_eq!(
            table.tcp_pipeline("h1-clear"),
            Some(TcpPipeline::CleartextHttp1)
        );
        assert_eq!(table.tcp_pipeline("h2-tls"), Some(TcpPipeline::TlsHttp2));
        assert_eq!(
            table.tcp_pipeline("grpc-clear"),
            Some(TcpPipeline::CleartextGrpc)
        );
        assert_eq!(table.udp_pipeline("h3-udp"), Some(UdpPipeline::Http3));
        assert!(table.tcp_pipeline("raw-tcp").is_none());
    }

    #[test]
    fn test_build_pipeline_generic_http_rejected() {
        let listeners = vec![
            cfg("web-h1", "tcp", "http", Some("1.1"), false),
            cfg("web-bad", "tcp", "http", None, false),
        ];

        // Generic "http" protocol is rejected — explicit protocol (http1, http2, etc.) is mandatory!
        assert!(PipelineTable::build(&listeners).is_err());
    }

    #[test]
    fn test_http3_on_tcp_fails() {
        let listeners = vec![cfg("bad-h3", "tcp", "http3", None, false)];
        assert!(PipelineTable::build(&listeners).is_err());
    }
}
