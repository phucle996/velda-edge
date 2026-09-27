//! L7 stream handoff contract for protocol layers (TLS, HTTP, etc.).

use std::net::SocketAddr;
use velda_core::ConnectionId;

use crate::connection::Connection;
use crate::ingress::classifier::PathKind;

/// A classified TCP connection prepared for handoff to L7 protocol engines.
///
/// Holds the underlying [`Connection`], listener ID, and initial classification path hint.
#[derive(Debug)]
pub struct TcpL7Handoff {
    connection: Connection,
    listener_id: String,
    path_hint: PathKind,
}

impl TcpL7Handoff {
    /// Creates a new TCP L7 handoff envelope from an accepted connection, path hint, and listener ID.
    pub fn new(
        connection: Connection,
        path_hint: PathKind,
        listener_id: impl Into<String>,
    ) -> Self {
        Self {
            connection,
            path_hint,
            listener_id: listener_id.into(),
        }
    }

    /// Returns a reference to the underlying [`Connection`].
    #[inline]
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Returns a mutable reference to the underlying [`Connection`].
    #[inline]
    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }

    /// Consumes the handoff envelope, returning the underlying [`Connection`].
    #[inline]
    pub fn into_connection(self) -> Connection {
        self.connection
    }

    /// Returns the unique connection identifier.
    #[inline]
    pub fn id(&self) -> ConnectionId {
        self.connection.id()
    }

    /// Returns the remote peer address.
    #[inline]
    pub fn peer(&self) -> SocketAddr {
        self.connection.peer()
    }

    /// Returns the local address on which the connection was accepted.
    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.connection.local_addr()
    }

    /// Returns the detected traffic protocol hint.
    #[inline]
    pub const fn path_hint(&self) -> PathKind {
        self.path_hint
    }

    /// Returns the declared listener identifier.
    #[inline]
    pub fn listener_id(&self) -> &str {
        &self.listener_id
    }
}

/// A classified UDP datagram and socket prepared for handoff to L7 protocol engines (HTTP/3, QUIC).
///
/// Holds the incoming [`Datagram`], the underlying shared [`UdpSocket`], listener ID,
/// and path hint.
#[derive(Debug, Clone)]
pub struct UdpL7Handoff {
    datagram: crate::udp::datagram::Datagram,
    socket: std::sync::Arc<crate::udp::socket::UdpSocket>,
    listener_id: String,
    path_hint: PathKind,
}

impl UdpL7Handoff {
    /// Creates a new UDP L7 handoff envelope.
    pub fn new(
        datagram: crate::udp::datagram::Datagram,
        socket: std::sync::Arc<crate::udp::socket::UdpSocket>,
        path_hint: PathKind,
        listener_id: impl Into<String>,
    ) -> Self {
        Self {
            datagram,
            socket,
            listener_id: listener_id.into(),
            path_hint,
        }
    }

    /// Returns a reference to the incoming datagram.
    #[inline]
    pub const fn datagram(&self) -> &crate::udp::datagram::Datagram {
        &self.datagram
    }

    /// Consumes the handoff envelope, returning the underlying datagram.
    #[inline]
    pub fn into_datagram(self) -> crate::udp::datagram::Datagram {
        self.datagram
    }

    /// Returns a reference to the underlying shared UDP socket.
    #[inline]
    pub fn socket(&self) -> &std::sync::Arc<crate::udp::socket::UdpSocket> {
        &self.socket
    }

    /// Returns the remote client address.
    #[inline]
    pub fn peer(&self) -> SocketAddr {
        self.datagram.peer()
    }

    /// Returns the local address on which the datagram was received.
    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.datagram.local_addr()
    }

    /// Returns the payload bytes of the datagram.
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.datagram.data()
    }

    /// Returns the traffic protocol hint.
    #[inline]
    pub const fn path_hint(&self) -> PathKind {
        self.path_hint
    }

    /// Returns the declared listener identifier.
    #[inline]
    pub fn listener_id(&self) -> &str {
        &self.listener_id
    }

    /// Sends a response datagram back to the originating client.
    pub async fn send_response(&self, payload: &[u8]) -> crate::error::Result<usize> {
        self.socket.send_to(payload, self.datagram.peer()).await
    }
}
