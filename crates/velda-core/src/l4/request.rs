//! Layer 4 request/connection model.
//!
//! L4 operates below the HTTP layer. It represents the network
//! connection and transport-level metadata without making any
//! assumptions about HTTP semantics.

use std::net::SocketAddr;

pub use crate::types::ConnectionId;

/// Supported L4 transport protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportProtocol {
    Tcp,
    Udp,
}

/// Information about the peer connected to Velda.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peer {
    pub address: SocketAddr,
}

/// L4 connection metadata.
///
/// This structure intentionally contains only transport-level
/// information. HTTP-specific data belongs to [`crate::L7Request`].
#[derive(Debug, Clone)]
pub struct L4Request {
    /// Unique identifier of the connection.
    pub connection_id: ConnectionId,

    /// Transport protocol used by the connection.
    pub protocol: TransportProtocol,

    /// Client address.
    pub peer: Peer,

    /// Local address accepted by Velda.
    pub local_addr: SocketAddr,

    /// Original destination, when available.
    ///
    /// This can be populated when Velda operates behind transparent
    /// proxying, TPROXY, or similar networking mechanisms.
    pub original_destination: Option<SocketAddr>,
}

impl L4Request {
    /// Creates a new L4 request context.
    pub fn new(
        connection_id: ConnectionId,
        protocol: TransportProtocol,
        peer: SocketAddr,
        local_addr: SocketAddr,
    ) -> Self {
        Self {
            connection_id,
            protocol,
            peer: Peer { address: peer },
            local_addr,
            original_destination: None,
        }
    }

    /// Sets the original destination address.
    pub fn with_original_destination(mut self, address: SocketAddr) -> Self {
        self.original_destination = Some(address);
        self
    }

    /// Returns the client IP address.
    pub fn client_ip(&self) -> std::net::IpAddr {
        self.peer.address.ip()
    }

    /// Returns the client port.
    pub fn client_port(&self) -> u16 {
        self.peer.address.port()
    }
}
