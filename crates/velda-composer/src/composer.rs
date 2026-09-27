//! Core Composer implementation.
//!
//! Owns the composition boundary: receives `L7Handoff` from `velda-transport`,
//! resolves the execution plan (TLS, protocol selection), creates connection context,
//! and dispatches to the appropriate protocol processor.

use std::collections::HashMap;

use velda_transport::{Connection, L7Handoff, PathKind};

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
        /// TLS profile reference name to use for handshake.
        profile: String,
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
    pub fn compose_tcp_handoff(&self, handoff: L7Handoff) -> Result<ComposedStream, ComposerError> {
        let listener_id = handoff.listener_id();
        let path_hint = handoff.path_hint();

        // 1. Resolve composition from compiled configuration if present, or derive from handoff metadata
        let (protocol, tls_enabled, tls_profile) = match self.listeners.get(listener_id) {
            Some(cfg) => {
                // Validate path hint against compiled protocol configuration
                Self::validate_path_compatibility(listener_id, path_hint, cfg.protocol)?;
                (cfg.protocol, cfg.tls_enabled, cfg.tls_profile.clone())
            }
            None => {
                // Fallback: derive directly from handoff metadata
                let proto = match path_hint {
                    PathKind::Http1 => ApplicationProtocol::Http1,
                    PathKind::Http2 => ApplicationProtocol::Http2,
                    PathKind::Http3 => ApplicationProtocol::Http3,
                    _ => ApplicationProtocol::Http,
                };
                let tls_enabled = handoff.is_tls() || handoff.tls_profile().is_some();
                let tls_profile = handoff.tls_profile().map(String::from);
                (proto, tls_enabled, tls_profile)
            }
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
            let profile = tls_profile.unwrap_or_else(|| "default".to_string());
            Ok(ComposedStream::TlsRequired {
                connection,
                profile,
                context,
            })
        } else {
            Ok(ComposedStream::Cleartext {
                connection,
                context,
            })
        }
    }

    /// Validates compatibility between transport path classification and configured application protocol.
    fn validate_path_compatibility(
        listener_id: &str,
        hint: PathKind,
        configured: ApplicationProtocol,
    ) -> Result<(), ComposerError> {
        match (hint, configured) {
            // Strict protocol mismatch checks
            (PathKind::Http1, ApplicationProtocol::Http2) => Err(ComposerError::ProtocolMismatch {
                listener_id: listener_id.to_string(),
                expected: "http2".to_string(),
                actual: "http1".to_string(),
            }),
            (PathKind::Http2, ApplicationProtocol::Http1) => Err(ComposerError::ProtocolMismatch {
                listener_id: listener_id.to_string(),
                expected: "http1".to_string(),
                actual: "http2".to_string(),
            }),
            // All other combinations are compatible (Generic Http accepts Http1/Http2/Tls)
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::net::{TcpListener, TcpStream};
    use velda_core::ConnectionId;
    use velda_transport::{Connection, L7Handoff, PathKind};

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
        let handoff = L7Handoff::new(conn, PathKind::Http, "http-public", None);

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
        let handoff = L7Handoff::new(
            conn,
            PathKind::Tls,
            "https-secure",
            Some("prod-cert".to_string()),
        );

        let mut composer = Composer::new();
        composer.register_listener(CompiledListenerComposition::new(
            "https-secure",
            ApplicationProtocol::Http,
            true,
            Some("prod-cert".to_string()),
        ));

        let composed = composer.compose_tcp_handoff(handoff).unwrap();

        assert!(composed.is_tls_required());
        match composed {
            ComposedStream::TlsRequired {
                profile, context, ..
            } => {
                assert_eq!(profile, "prod-cert");
                assert_eq!(context.listener_id, "https-secure");
                assert_eq!(context.protocol, ApplicationProtocol::Http);
            }
            ComposedStream::Cleartext { .. } => panic!("Expected TLS required"),
        }
    }

    #[tokio::test]
    async fn test_compose_protocol_mismatch_fails_closed() {
        let (conn, _addr) = create_dummy_connection().await;
        // Client arrived with cleartext HTTP/1 preface, but listener is configured as strict HTTP/2 only
        let handoff = L7Handoff::new(conn, PathKind::Http1, "grpc-listener", None);

        let mut composer = Composer::new();
        composer.register_listener(CompiledListenerComposition::new(
            "grpc-listener",
            ApplicationProtocol::Http2,
            false,
            None,
        ));

        let res = composer.compose_tcp_handoff(handoff);
        assert!(matches!(res, Err(ComposerError::ProtocolMismatch { .. })));
    }
}
