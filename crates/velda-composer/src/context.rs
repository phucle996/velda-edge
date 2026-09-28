//! Connection-level composition context.
//!
//! Owns connection metadata and protocol negotiation state (SNI, ALPN)
//! produced during the composition phase before request processing.

use std::net::SocketAddr;
use std::time::Instant;

use velda_core::{ConnectionContext, ConnectionId, L4Request};

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
            created_at: Instant::now(),
        }
    }

    /// Enriches the context with TLS metadata after successful handshake,
    /// and resolves generic HTTP into a concrete application protocol (HTTP/2 or HTTP/1.1)
    /// based on the negotiated ALPN token.
    pub fn with_tls_metadata(mut self, metadata: TlsMetadata) -> Self {
        if let Some(ref alpn) = metadata.alpn {
            if alpn.eq_ignore_ascii_case("h2") {
                self.protocol = ApplicationProtocol::Http2;
            } else if alpn.eq_ignore_ascii_case("http/1.1") || alpn.eq_ignore_ascii_case("http/1.0")
            {
                self.protocol = ApplicationProtocol::Http1;
            }
        } else if self.protocol == ApplicationProtocol::Http {
            // Default fallback when client does not send ALPN: HTTP/1.1
            self.protocol = ApplicationProtocol::Http1;
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
    fn test_with_tls_metadata_resolves_h2() {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let ctx = ComposerContext::new_tcp(
            ConnectionId::new(1),
            "https",
            addr,
            addr,
            ApplicationProtocol::Http,
        );
        let metadata = TlsMetadata::new(Some("example.com".into()), Some("h2".into()));
        let enriched = ctx.with_tls_metadata(metadata);

        assert_eq!(enriched.protocol, ApplicationProtocol::Http2);
        assert_eq!(enriched.tls.unwrap().alpn.as_deref(), Some("h2"));
    }

    #[test]
    fn test_with_tls_metadata_resolves_http1() {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let ctx = ComposerContext::new_tcp(
            ConnectionId::new(1),
            "https",
            addr,
            addr,
            ApplicationProtocol::Http,
        );
        let metadata = TlsMetadata::new(Some("example.com".into()), Some("http/1.1".into()));
        let enriched = ctx.with_tls_metadata(metadata);

        assert_eq!(enriched.protocol, ApplicationProtocol::Http1);
        assert_eq!(enriched.tls.unwrap().alpn.as_deref(), Some("http/1.1"));
    }

    #[test]
    fn test_with_tls_metadata_fallback_without_alpn() {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let ctx = ComposerContext::new_tcp(
            ConnectionId::new(1),
            "https",
            addr,
            addr,
            ApplicationProtocol::Http,
        );
        let metadata = TlsMetadata::new(Some("example.com".into()), None);
        let enriched = ctx.with_tls_metadata(metadata);

        assert_eq!(enriched.protocol, ApplicationProtocol::Http1);
        assert!(enriched.tls.unwrap().alpn.is_none());
    }
}
