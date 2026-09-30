//! Core Composer implementation.
//!
//! Owns the composition boundary: receives `TcpL7Handoff` / `UdpL7Handoff`
//! from `velda-transport`, resolves the execution plan (TLS, protocol selection),
//! creates connection context, and dispatches to the appropriate protocol processor.

use std::collections::HashMap;
use std::sync::Arc;

use velda_transport::{Connection, Datagram, TcpL7Handoff, UdpL7Handoff, UdpSocket};

use crate::config::{ApplicationProtocol, CompiledListenerComposition};
use crate::context::ComposerContext;
use crate::error::ComposerError;

/// The resolved composition state for an incoming connection.
#[derive(Debug)]
pub enum ComposedStream {
    /// Stream is cleartext and ready for application protocol handling.
    Cleartext {
        /// Active connection wrapper.
        connection: Connection,
        /// Initialized connection context.
        context: ComposerContext,
    },
    /// Stream requires TLS termination before application protocol handling.
    TlsRequired {
        /// Active connection wrapper.
        connection: Connection,
        /// Initialized connection context.
        context: ComposerContext,
    },
}

impl ComposedStream {
    /// Returns a reference to the active connection.
    #[inline]
    pub const fn connection(&self) -> &Connection {
        match self {
            Self::Cleartext { connection, .. } => connection,
            Self::TlsRequired { connection, .. } => connection,
        }
    }

    /// Returns a mutable reference to the active connection.
    #[inline]
    pub fn connection_mut(&mut self) -> &mut Connection {
        match self {
            Self::Cleartext { connection, .. } => connection,
            Self::TlsRequired { connection, .. } => connection,
        }
    }

    /// Consumes the composed stream and returns the underlying connection.
    #[inline]
    pub fn into_connection(self) -> Connection {
        match self {
            Self::Cleartext { connection, .. } => connection,
            Self::TlsRequired { connection, .. } => connection,
        }
    }

    /// Returns a reference to the connection context.
    #[inline]
    pub const fn context(&self) -> &ComposerContext {
        match self {
            Self::Cleartext { context, .. } => context,
            Self::TlsRequired { context, .. } => context,
        }
    }

    /// Returns whether this stream requires TLS termination.
    #[inline]
    pub const fn is_tls_required(&self) -> bool {
        matches!(self, Self::TlsRequired { .. })
    }

    /// Returns the resolved application protocol.
    #[inline]
    pub const fn protocol(&self) -> ApplicationProtocol {
        self.context().protocol
    }
}

/// The resolved composition state for an incoming UDP datagram.
#[derive(Debug)]
pub enum ComposedDatagram {
    /// Datagram is cleartext and ready for application protocol handling.
    Cleartext {
        /// Incoming datagram payload and metadata.
        datagram: Datagram,
        /// Shared UDP socket for continued I/O (responses, further receives).
        socket: Arc<UdpSocket>,
        /// Initialized connection context.
        context: ComposerContext,
    },
    /// Datagram requires TLS/QUIC processing before application protocol handling.
    TlsRequired {
        /// Incoming datagram payload and metadata.
        datagram: Datagram,
        /// Shared UDP socket for continued I/O (QUIC handshake, responses).
        socket: Arc<UdpSocket>,
        /// Initialized connection context.
        context: ComposerContext,
    },
}

impl ComposedDatagram {
    /// Returns a reference to the incoming datagram.
    #[inline]
    pub const fn datagram(&self) -> &Datagram {
        match self {
            Self::Cleartext { datagram, .. } => datagram,
            Self::TlsRequired { datagram, .. } => datagram,
        }
    }

    /// Returns a reference to the shared UDP socket.
    #[inline]
    pub fn socket(&self) -> &Arc<UdpSocket> {
        match self {
            Self::Cleartext { socket, .. } => socket,
            Self::TlsRequired { socket, .. } => socket,
        }
    }

    /// Consumes the composed datagram, returning the underlying datagram and socket.
    #[inline]
    pub fn into_parts(self) -> (Datagram, Arc<UdpSocket>) {
        match self {
            Self::Cleartext {
                datagram, socket, ..
            } => (datagram, socket),
            Self::TlsRequired {
                datagram, socket, ..
            } => (datagram, socket),
        }
    }

    /// Returns a reference to the connection context.
    #[inline]
    pub const fn context(&self) -> &ComposerContext {
        match self {
            Self::Cleartext { context, .. } => context,
            Self::TlsRequired { context, .. } => context,
        }
    }

    /// Returns whether this datagram requires TLS/QUIC processing.
    #[inline]
    pub const fn is_tls_required(&self) -> bool {
        matches!(self, Self::TlsRequired { .. })
    }

    /// Returns the resolved application protocol.
    #[inline]
    pub const fn protocol(&self) -> ApplicationProtocol {
        self.context().protocol
    }
}

/// The Composer composition engine.
///
/// Decides whether TLS is required, determines the configured application protocol,
/// builds the connection context, and manages protocol execution.
#[derive(Debug, Default, Clone)]
pub struct Composer {
    listeners: HashMap<String, CompiledListenerComposition>,
}

impl Composer {
    /// Creates a new empty composer without pre-registered listeners.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a composer pre-populated with compiled listener compositions.
    pub fn with_listeners(
        listeners: impl IntoIterator<Item = CompiledListenerComposition>,
    ) -> Self {
        let mut map = HashMap::new();
        for l in listeners {
            map.insert(l.listener_id.clone(), l);
        }
        Self { listeners: map }
    }

    /// Registers or updates a compiled listener composition in the table.
    pub fn register_listener(&mut self, composition: CompiledListenerComposition) {
        self.listeners
            .insert(composition.listener_id.clone(), composition);
    }

    /// Returns the compiled composition for a listener ID, if present.
    pub fn get_listener(&self, id: &str) -> Option<&CompiledListenerComposition> {
        self.listeners.get(id)
    }

    /// Composes an incoming TCP L7 handoff from `velda-transport`.
    ///
    /// Validates protocol consistency against the configured listener,
    /// sets up the `ComposerContext`, and decides whether TLS termination is required.
    /// Composes an incoming TCP L7 handoff from `velda-transport`.
    ///
    /// Validates protocol consistency against the configured listener,
    /// sets up the `ComposerContext`, and decides whether TLS termination is required.
    pub fn compose_tcp_handoff(
        &self,
        handoff: TcpL7Handoff,
    ) -> Result<ComposedStream, ComposerError> {
        // 1. Resolve composition from compiled configuration
        let cfg = self.listeners.get(handoff.listener_id()).ok_or_else(|| {
            ComposerError::MissingConfiguration(handoff.listener_id().to_string())
        })?;
        let (protocol, tls_enabled, limits) = (cfg.protocol, cfg.tls_enabled, cfg.limits);

        // 2. Initialize connection context using owned listener_id directly (zero heap allocation)
        let conn_id = handoff.id();
        let peer = handoff.peer();
        let local_addr = handoff.local_addr();
        let (connection, listener_id) = handoff.into_parts();

        let context =
            ComposerContext::new_tcp(conn_id, listener_id, peer, local_addr, protocol, limits);

        // 3. Decide composition outcome
        if tls_enabled {
            Ok(ComposedStream::TlsRequired {
                connection,
                context,
            })
        } else {
            Ok(ComposedStream::Cleartext {
                connection,
                context,
            })
        }
    }

    /// Composes an incoming UDP L7 handoff from `velda-transport`.
    ///
    /// Validates protocol consistency against the configured listener,
    /// sets up the `ComposerContext`, and decides whether TLS/QUIC processing is required.
    pub fn compose_udp_handoff(
        &self,
        handoff: UdpL7Handoff,
    ) -> Result<ComposedDatagram, ComposerError> {
        // 1. Resolve composition from compiled configuration
        let cfg = self.listeners.get(handoff.listener_id()).ok_or_else(|| {
            ComposerError::MissingConfiguration(handoff.listener_id().to_string())
        })?;
        let (protocol, tls_enabled, limits) = (cfg.protocol, cfg.tls_enabled, cfg.limits);

        // 2. Initialize connection context using owned listener_id directly (zero heap allocation)
        let peer = handoff.peer();
        let local_addr = handoff.local_addr();
        let (datagram, socket, listener_id) = handoff.into_parts();

        let context = ComposerContext::new_udp(listener_id, peer, local_addr, protocol, limits);

        // 3. Decide composition outcome
        if tls_enabled {
            Ok(ComposedDatagram::TlsRequired {
                datagram,
                socket,
                context,
            })
        } else {
            Ok(ComposedDatagram::Cleartext {
                datagram,
                socket,
                context,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::net::{TcpListener, TcpStream};
    use velda_core::ConnectionId;
    use velda_transport::{Connection, TcpL7Handoff};

    async fn create_dummy_connection() -> (Connection, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

        let (server_stream, peer) = listener.accept().await.unwrap();
        let _ = client.await.unwrap();

        let conn = Connection::new(ConnectionId::new(101), server_stream, peer, addr);
        (conn, addr)
    }

    #[tokio::test]
    async fn test_compose_cleartext_http_handoff() {
        let (conn, _addr) = create_dummy_connection().await;
        let handoff = TcpL7Handoff::new(conn, "http-public");

        let mut composer = Composer::new();
        composer.register_listener(CompiledListenerComposition::new(
            "http-public",
            ApplicationProtocol::Http1,
            false,
            velda_core::IngressLimits::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
        ));
        let composed = composer.compose_tcp_handoff(handoff).unwrap();

        assert!(!composed.is_tls_required());
        // Cleartext HTTP automatically resolves to concrete Http1
        assert_eq!(composed.protocol(), ApplicationProtocol::Http1);
        assert_eq!(composed.context().listener_id, "http-public");
        assert_eq!(composed.context().connection_id, ConnectionId::new(101));
    }

    #[tokio::test]
    async fn test_compose_tls_https_handoff() {
        use crate::context::TlsMetadata;

        let (conn, _addr) = create_dummy_connection().await;
        let handoff = TcpL7Handoff::new(conn, "https-secure");

        let mut composer = Composer::new();
        composer.register_listener(CompiledListenerComposition::new(
            "https-secure",
            ApplicationProtocol::Http2,
            true,
            velda_core::IngressLimits::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
        ));

        let composed = composer.compose_tcp_handoff(handoff).unwrap();

        assert!(composed.is_tls_required());
        match composed {
            ComposedStream::TlsRequired { context, .. } => {
                assert_eq!(context.listener_id, "https-secure");
                assert_eq!(context.protocol, ApplicationProtocol::Http2);

                // After TLS handshake, ALPN validates but does NOT mutate protocol
                let enriched = context.with_tls_metadata(TlsMetadata::new(
                    Some("api.example.com".into()),
                    Some("h2".into()),
                ));
                assert_eq!(enriched.protocol, ApplicationProtocol::Http2);
            }
            ComposedStream::Cleartext { .. } => panic!("Expected TLS required"),
        }
    }

    #[tokio::test]
    async fn test_compose_udp_h3_handoff_registered() {
        let socket =
            velda_transport::UdpSocket::bind("127.0.0.1:0".parse().unwrap(), Default::default())
                .unwrap();
        let local_addr = socket.local_addr();
        let peer: SocketAddr = "192.168.1.50:12345".parse().unwrap();

        let datagram = Datagram::new(peer, local_addr, b"quic-initial".to_vec());
        let handoff = UdpL7Handoff::new(datagram, Arc::new(socket), "h3-ingress");

        let mut composer = Composer::new();
        composer.register_listener(CompiledListenerComposition::new(
            "h3-ingress",
            ApplicationProtocol::Http3,
            true,
            velda_core::IngressLimits::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
        ));
        let composed = composer.compose_udp_handoff(handoff).unwrap();

        assert!(composed.is_tls_required());
        assert_eq!(composed.protocol(), ApplicationProtocol::Http3);
        assert_eq!(composed.context().listener_id, "h3-ingress");
        assert_eq!(composed.datagram().data(), b"quic-initial");
        assert_eq!(composed.datagram().peer(), peer);
    }

    #[tokio::test]
    async fn test_compose_udp_registered_cleartext() {
        let socket =
            velda_transport::UdpSocket::bind("127.0.0.1:0".parse().unwrap(), Default::default())
                .unwrap();
        let local_addr = socket.local_addr();
        let peer: SocketAddr = "10.0.0.1:9999".parse().unwrap();

        let datagram = Datagram::new(peer, local_addr, b"custom-udp".to_vec());
        let handoff = UdpL7Handoff::new(datagram, Arc::new(socket), "udp-custom");

        let mut composer = Composer::new();
        composer.register_listener(CompiledListenerComposition::new(
            "udp-custom",
            ApplicationProtocol::Http3,
            false,
            velda_core::IngressLimits::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
        ));

        let composed = composer.compose_udp_handoff(handoff).unwrap();

        assert!(!composed.is_tls_required());
        assert_eq!(composed.protocol(), ApplicationProtocol::Http3);

        let (dgram, _sock) = composed.into_parts();
        assert_eq!(dgram.data(), b"custom-udp");
    }

    #[tokio::test]
    async fn test_compose_unregistered_listener_fails() {
        let (conn, _addr) = create_dummy_connection().await;
        let handoff = TcpL7Handoff::new(conn, "unregistered-listener");
        let composer = Composer::new();

        assert!(matches!(
            composer.compose_tcp_handoff(handoff),
            Err(ComposerError::MissingConfiguration(id)) if id == "unregistered-listener"
        ));
    }
}
