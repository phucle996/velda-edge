//! L7 stream handoff contract for protocol layers (TLS, HTTP).

use std::net::SocketAddr;
use tokio::net::TcpStream;
use velda_core::{ConnectionContext, ConnectionId, L4Request};

use crate::connection::Connection;
use crate::ingress::classifier::PathKind;

/// A classified connection prepared for handoff to L7 protocol engines.
///
/// Holds the underlying [`Connection`], its endpoints, listener ID, TLS status, and path hint.
#[derive(Debug)]
pub struct L7Handoff {
    id: ConnectionId,
    connection: Connection,
    peer: SocketAddr,
    local_addr: SocketAddr,
    path_hint: PathKind,
    listener_id: String,
    tls_enabled: bool,
}

impl L7Handoff {
    /// Creates a new L7 handoff envelope from an accepted connection, listener ID, and TLS flag.
    pub fn new(
        connection: Connection,
        path_hint: PathKind,
        listener_id: impl Into<String>,
        tls_enabled: bool,
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
            tls_enabled,
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

    /// Returns the detected traffic protocol hint ([`PathKind::Tls`], [`PathKind::Http`], [`PathKind::Http1`], or [`PathKind::Http2`]).
    #[inline]
    pub const fn path_hint(&self) -> PathKind {
        self.path_hint
    }

    /// Returns `true` if this connection was classified or configured as HTTP/1.x.
    #[inline]
    pub const fn is_http1(&self) -> bool {
        self.path_hint.is_http1()
    }

    /// Returns `true` if this connection was classified or configured as HTTP/2.
    #[inline]
    pub const fn is_http2(&self) -> bool {
        self.path_hint.is_http2()
    }

    /// Returns `true` if this connection is any HTTP version.
    #[inline]
    pub const fn is_http(&self) -> bool {
        self.path_hint.is_http()
    }

    /// Returns `true` if this connection requires TLS termination.
    #[inline]
    pub const fn is_tls(&self) -> bool {
        self.tls_enabled || self.path_hint.is_tls()
    }

    /// Returns whether TLS is enabled on the ingress listener.
    #[inline]
    pub const fn tls_enabled(&self) -> bool {
        self.tls_enabled
    }

    /// Returns the declared listener identifier (from listeners.json).
    #[inline]
    pub fn listener_id(&self) -> &str {
        &self.listener_id
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

/// A classified UDP datagram and socket prepared for handoff to L7 protocol engines (HTTP/3, QUIC).
///
/// Holds the incoming [`Datagram`], the underlying shared [`UdpSocket`], listener ID,
/// TLS enabled flag, and path hint.
#[derive(Debug, Clone)]
pub struct UdpL7Handoff {
    datagram: crate::udp::datagram::Datagram,
    socket: std::sync::Arc<crate::udp::socket::UdpSocket>,
    path_hint: PathKind,
    listener_id: String,
    tls_enabled: bool,
}

impl UdpL7Handoff {
    /// Creates a new UDP L7 handoff envelope.
    pub fn new(
        datagram: crate::udp::datagram::Datagram,
        socket: std::sync::Arc<crate::udp::socket::UdpSocket>,
        path_hint: PathKind,
        listener_id: impl Into<String>,
        tls_enabled: bool,
    ) -> Self {
        Self {
            datagram,
            socket,
            path_hint,
            listener_id: listener_id.into(),
            tls_enabled,
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

    /// Returns the traffic protocol hint ([`PathKind::Http3`], [`PathKind::Quic`], or similar).
    #[inline]
    pub const fn path_hint(&self) -> PathKind {
        self.path_hint
    }

    /// Returns `true` if this datagram was received for HTTP/3 or QUIC.
    #[inline]
    pub const fn is_http3(&self) -> bool {
        self.path_hint.is_http3()
    }

    /// Returns `true` if this datagram requires TLS/QUIC encryption handling.
    #[inline]
    pub const fn is_tls(&self) -> bool {
        self.tls_enabled || self.path_hint.is_tls() || self.path_hint.is_http3()
    }

    /// Returns whether TLS is enabled on the ingress listener.
    #[inline]
    pub const fn tls_enabled(&self) -> bool {
        self.tls_enabled
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
