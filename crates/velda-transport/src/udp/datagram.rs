//! UDP datagram model.

use std::net::SocketAddr;
use velda_core::{ConnectionId, L4Request, TransportProtocol};

/// An individual UDP datagram with network endpoint metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Datagram {
    pub peer: SocketAddr,
    pub local_addr: SocketAddr,
    pub data: Vec<u8>,
}

impl Datagram {
    /// Creates a new UDP datagram.
    #[inline]
    pub fn new(peer: SocketAddr, local_addr: SocketAddr, data: Vec<u8>) -> Self {
        Self {
            peer,
            local_addr,
            data,
        }
    }

    /// Returns the remote peer address (sender or destination).
    #[inline]
    pub const fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// Returns the local socket address.
    #[inline]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns a slice over the datagram payload bytes.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Returns the length in bytes of the payload.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns whether the payload is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Consumes the datagram, returning the underlying payload vector.
    #[inline]
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }

    /// Converts this datagram into a [`L4Request`] metadata descriptor with [`TransportProtocol::Udp`].
    pub fn to_l4_request(&self, connection_id: ConnectionId) -> L4Request {
        L4Request::new(
            connection_id,
            TransportProtocol::Udp,
            self.peer,
            self.local_addr,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datagram_properties() {
        let peer: SocketAddr = "192.168.1.100:5353".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:53".parse().unwrap();
        let payload = b"dns-query-packet".to_vec();

        let dgram = Datagram::new(peer, local, payload.clone());
        assert_eq!(dgram.peer(), peer);
        assert_eq!(dgram.local_addr(), local);
        assert_eq!(dgram.data(), payload.as_slice());
        assert_eq!(dgram.len(), 16);
        assert!(!dgram.is_empty());

        let l4_req = dgram.to_l4_request(ConnectionId::new(99));
        assert_eq!(l4_req.connection_id, ConnectionId::new(99));
        assert_eq!(l4_req.protocol, TransportProtocol::Udp);
        assert_eq!(l4_req.peer.address, peer);
        assert_eq!(l4_req.local_addr, local);

        assert_eq!(dgram.into_data(), payload);
    }
}
