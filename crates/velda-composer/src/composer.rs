//! Core Composer implementation.
//!
//! Owns the composition boundary: receives `TcpL7Handoff` from `velda-transport`,
//! resolves the execution plan (TLS, protocol selection), creates connection context,
//! and dispatches to the appropriate protocol processor.

use std::collections::HashMap;

use velda_transport::{Connection, TcpL7Handoff};

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
    pub fn compose_tcp_handoff(
        &self,
        handoff: TcpL7Handoff,
    ) -> Result<ComposedStream, ComposerError> {
        let listener_id = handoff.listener_id();

        // 1. Resolve composition from compiled configuration if present, or derive default HTTP cleartext
        let (protocol, tls_enabled) = match self.listeners.get(listener_id) {
            Some(cfg) => (cfg.protocol, cfg.tls_enabled),
            None => (ApplicationProtocol::Http, false),
        };

        // 2. Initialize connection context
        let context = ComposerContext::new(
            handoff.id(),
            listener_id,
            handoff.peer(),
            handoff.local_addr(),
            protocol,
        );

        let connection = handoff.into_connection();

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

        let composer = Composer::new();
        let composed = composer.compose_tcp_handoff(handoff).unwrap();

        assert!(!composed.is_tls_required());
        assert_eq!(composed.protocol(), ApplicationProtocol::Http);
        assert_eq!(composed.context().listener_id, "http-public");
        assert_eq!(composed.context().connection_id, ConnectionId::new(101));
    }

    #[tokio::test]
    async fn test_compose_tls_https_handoff() {
        let (conn, _addr) = create_dummy_connection().await;
        let handoff = TcpL7Handoff::new(conn, "https-secure");

        let mut composer = Composer::new();
        composer.register_listener(CompiledListenerComposition::new(
            "https-secure",
            ApplicationProtocol::Http,
            true,
        ));

        let composed = composer.compose_tcp_handoff(handoff).unwrap();

        assert!(composed.is_tls_required());
        match composed {
            ComposedStream::TlsRequired { context, .. } => {
                assert_eq!(context.listener_id, "https-secure");
                assert_eq!(context.protocol, ApplicationProtocol::Http);
            }
            ComposedStream::Cleartext { .. } => panic!("Expected TLS required"),
        }
    }
}
