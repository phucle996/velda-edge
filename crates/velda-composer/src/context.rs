//! Connection-level composition context.
//!
//! Owns connection metadata and protocol negotiation state (SNI, ALPN)
//! produced during the composition phase before request processing.

use std::net::SocketAddr;
use std::time::Instant;

use velda_core::{ConnectionContext, ConnectionId, IngressLimits, L4Request};

use crate::config::ApplicationProtocol;

/// Metadata captured from TLS handshake (if TLS was performed).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TlsMetadata {
    /// Server Name Indication (SNI) requested by client.
    pub sni: Option<String>,
    /// Application-Layer Protocol Negotiation (ALPN) token selected.
    pub alpn: Option<String>,
    /// Negotiated TLS protocol version (e.g. "TLSv1.3").
    pub version: Option<String>,
    /// Negotiated cipher suite (e.g. "TLS_AES_128_GCM_SHA256").
    pub cipher: Option<String>,
}

impl TlsMetadata {
    /// Creates a new TLS metadata container.
    pub fn new(sni: Option<String>, alpn: Option<String>) -> Self {
        Self {
            sni,
            alpn,
            version: None,
            cipher: None,
        }
    }
}

/// Enriched connection-level context owned by Composer.
///
/// Created when `TcpL7Handoff` is received from `velda-transport` and enriched
/// after optional TLS termination with SNI and ALPN details.
#[derive(Debug, Clone)]
pub struct ComposerContext {
    /// Unique connection identifier.
    pub connection_id: ConnectionId,
    /// Listener identifier that accepted the connection.
    pub listener_id: String,
    /// Remote client socket address.
    pub peer: SocketAddr,
    /// Local server socket address.
    pub local_addr: SocketAddr,
    /// TLS session metadata (present if TLS was active and completed).
    pub tls: Option<TlsMetadata>,
    /// Active application protocol resolved for this stream.
    pub protocol: ApplicationProtocol,
    /// Ingress safety limits applicable to this connection.
    pub limits: IngressLimits,
    /// Time when the connection entered Composer.
    pub created_at: Instant,
}

impl ComposerContext {
    /// Creates a new composer context from basic TCP connection properties.
    pub fn new_tcp(
        connection_id: ConnectionId,
        listener_id: impl Into<String>,
        peer: SocketAddr,
        local_addr: SocketAddr,
        protocol: ApplicationProtocol,
    ) -> Self {
        Self {
            connection_id,
            listener_id: listener_id.into(),
            peer,
            local_addr,
            tls: None,
            protocol,
            limits: IngressLimits::default(),
            created_at: Instant::now(),
        }
    }

    /// Creates a new composer context for a UDP datagram handoff.
    ///
    /// UDP is connectionless, so a fresh [`ConnectionId`] is generated
    /// from the global monotonic counter for tracing and observability.
    pub fn new_udp(
        listener_id: impl Into<String>,
        peer: SocketAddr,
        local_addr: SocketAddr,
        protocol: ApplicationProtocol,
    ) -> Self {
        Self {
            connection_id: velda_transport::next_connection_id(),
            listener_id: listener_id.into(),
            peer,
            local_addr,
            tls: None,
            protocol,
            limits: IngressLimits::default(),
            created_at: Instant::now(),
        }
    }

    /// Configures ingress safety limits for this connection context.
    pub fn with_limits(mut self, limits: IngressLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Enriches the context with TLS metadata after successful handshake.
    ///
    /// ALPN is treated as a **validation signal**, not a protocol decision.
    /// The listener declares its protocol at bootstrap; ALPN only verifies
    /// that the client supports the declared protocol. Protocol is **never mutated**.
    pub fn with_tls_metadata(mut self, metadata: TlsMetadata) -> Self {
        if let Some(ref alpn) = metadata.alpn {
            let expected = match self.protocol {
                ApplicationProtocol::Http1 => "http/1.1",
                ApplicationProtocol::Http2 | ApplicationProtocol::Grpc => "h2",
                ApplicationProtocol::Http3 => "",
            };
            if !expected.is_empty() && !alpn.eq_ignore_ascii_case(expected) {
                tracing::warn!(
                    listener = %self.listener_id,
                    declared = %self.protocol,
                    alpn = %alpn,
                    "ALPN mismatch: client negotiated '{}' but listener declares '{}'",
                    alpn,
                    self.protocol,
                );
            }
        }

        self.tls = Some(metadata);
        self
    }

    /// Converts this connection context into a `velda_core::ConnectionContext`.
    pub fn to_core_connection_context(&self) -> ConnectionContext {
        let l4 = L4Request::new(
            self.connection_id,
            velda_core::TransportProtocol::Tcp,
            self.peer,
            self.local_addr,
        );
        ConnectionContext::new(l4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_tls_metadata_alpn_match_preserves_protocol() {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let ctx = ComposerContext::new_tcp(
            ConnectionId::new(1),
            "https-h2",
            addr,
            addr,
            ApplicationProtocol::Http2,
        );
        let metadata = TlsMetadata::new(Some("example.com".into()), Some("h2".into()));
        let enriched = ctx.with_tls_metadata(metadata);

        // Protocol stays Http2 — ALPN confirms but does not change it
        assert_eq!(enriched.protocol, ApplicationProtocol::Http2);
        assert_eq!(enriched.tls.unwrap().alpn.as_deref(), Some("h2"));
    }

    #[test]
    fn test_with_tls_metadata_alpn_mismatch_preserves_protocol() {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let ctx = ComposerContext::new_tcp(
            ConnectionId::new(1),
            "https-h1",
            addr,
            addr,
            ApplicationProtocol::Http1,
        );
        // Client sends h2 ALPN but listener is Http1 — protocol must NOT be mutated
        let metadata = TlsMetadata::new(Some("example.com".into()), Some("h2".into()));
        let enriched = ctx.with_tls_metadata(metadata);

        assert_eq!(enriched.protocol, ApplicationProtocol::Http1);
        assert_eq!(enriched.tls.unwrap().alpn.as_deref(), Some("h2"));
    }

    #[test]
    fn test_with_tls_metadata_no_alpn_preserves_protocol() {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let ctx = ComposerContext::new_tcp(
            ConnectionId::new(1),
            "https-h2",
            addr,
            addr,
            ApplicationProtocol::Http2,
        );
        let metadata = TlsMetadata::new(Some("example.com".into()), None);
        let enriched = ctx.with_tls_metadata(metadata);

        // No ALPN — protocol stays Http2 as declared
        assert_eq!(enriched.protocol, ApplicationProtocol::Http2);
        assert!(enriched.tls.unwrap().alpn.is_none());
    }
}
