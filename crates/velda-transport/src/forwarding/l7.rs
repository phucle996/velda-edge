//! L7 stream handoff contract for protocol layers (TLS, HTTP).

use std::net::SocketAddr;
use tokio::net::TcpStream;
use velda_core::{ConnectionContext, ConnectionId, L4Request};

use crate::connection::Connection;
use crate::ingress::classifier::PathKind;

/// A classified connection prepared for handoff to L7 protocol engines.
///
/// Holds the underlying [`Connection`], its endpoints, listener ID, TLS profile, and path hint.
#[derive(Debug)]
pub struct L7Handoff {
    id: ConnectionId,
    connection: Connection,
    peer: SocketAddr,
    local_addr: SocketAddr,
    path_hint: PathKind,
    listener_id: String,
    tls_profile: Option<String>,
}

impl L7Handoff {
    /// Creates a new L7 handoff envelope from an accepted connection, listener ID, and TLS profile.
    pub fn new(
        connection: Connection,
        path_hint: PathKind,
        listener_id: impl Into<String>,
        tls_profile: Option<String>,
    ) -> Self {
        let id = connection.id();
        let peer = connection.peer();
        let local_addr = connection.local_addr();

        Self {
            id,
            connection,
            peer,
            local_addr,
            path_hint,
            listener_id: listener_id.into(),
            tls_profile,
        }
    }

    /// Returns the unique connection identifier.
    #[inline]
    pub const fn id(&self) -> ConnectionId {
        self.id
    }

    /// Returns the remote peer address.
    #[inline]
    pub const fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// Returns the local address on which the connection was received.
    #[inline]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the detected traffic protocol hint ([`PathKind::Tls`] or [`PathKind::Http`]).
    #[inline]
    pub const fn path_hint(&self) -> PathKind {
        self.path_hint
    }

    /// Returns the declared listener identifier (from listeners.json).
    #[inline]
    pub fn listener_id(&self) -> &str {
        &self.listener_id
    }

    /// Returns the associated TLS profile name, if any (from listeners.json).
    #[inline]
    pub fn tls_profile(&self) -> Option<&str> {
        self.tls_profile.as_deref()
    }

    /// Converts this handoff into a [`L4Request`] metadata descriptor.
    pub fn to_l4_request(&self) -> L4Request {
        self.connection.to_l4_request()
    }

    /// Converts this handoff into a [`ConnectionContext`].
    pub fn to_connection_context(&self) -> ConnectionContext {
        self.connection.to_connection_context()
    }

    /// Consumes the handoff envelope, returning the underlying [`Connection`].
    pub fn into_connection(self) -> Connection {
        self.connection
    }

    /// Consumes the handoff envelope, returning the raw [`TcpStream`].
    pub fn into_stream(self) -> TcpStream {
        self.connection.into_stream()
    }
}
