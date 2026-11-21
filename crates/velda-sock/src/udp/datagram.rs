//! UDP datagram model.

use velda_core::{L4Request, TransportProtocol};

/// An individual UDP datagram with raw integer network endpoint metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Datagram {
    pub peer_ip: u128,
    pub peer_port_be: u16,
    pub local_ip: u128,
    pub local_port_be: u16,
    pub data: bytes::Bytes,
}

impl Datagram {
    /// Creates a new UDP datagram.
    #[inline]
    pub fn new(
        peer_ip: u128,
        peer_port_be: u16,
        local_ip: u128,
        local_port_be: u16,
        data: impl Into<bytes::Bytes>,
    ) -> Self {
        Self {
            peer_ip,
            peer_port_be,
            local_ip,
            local_port_be,
            data: data.into(),
        }
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
        self.data.to_vec()
    }

    /// Consumes the datagram, returning the underlying [`bytes::Bytes`].
    #[inline]
    pub fn into_bytes(self) -> bytes::Bytes {
        self.data
    }

    /// Converts this datagram into a [`L4Request`] metadata descriptor with [`TransportProtocol::Udp`].
    pub fn to_l4_request(&self, connection_id: u64) -> L4Request {
        L4Request::new(
            connection_id,
            TransportProtocol::Udp,
            self.peer_ip,
            self.peer_port_be,
            self.local_ip,
            self.local_port_be,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datagram_properties() {
        let peer_ip = velda_core::lut::ip::v4_to_u128(0xC0A8_0164);
        let peer_port_be = 5353u16.to_be();
        let local_ip = velda_core::lut::ip::v4_to_u128(0x7F00_0001);
        let local_port_be = 53u16.to_be();
        let payload = b"dns-query-packet".to_vec();

        let dgram = Datagram::new(
            peer_ip,
            peer_port_be,
            local_ip,
            local_port_be,
            payload.clone(),
        );
        assert_eq!(dgram.peer_ip, peer_ip);
        assert_eq!(dgram.peer_port_be, peer_port_be);
        assert_eq!(dgram.local_ip, local_ip);
        assert_eq!(dgram.local_port_be, local_port_be);
        assert_eq!(dgram.data(), payload.as_slice());
        assert_eq!(dgram.len(), 16);
        assert!(!dgram.is_empty());

        let l4_req = dgram.to_l4_request(99);
        assert_eq!(l4_req.connection_id, 99);
        assert_eq!(l4_req.protocol, TransportProtocol::Udp);
        assert_eq!(l4_req.peer_ip, peer_ip);
        assert_eq!(l4_req.peer_port_be, peer_port_be);
        assert_eq!(l4_req.local_ip, local_ip);
        assert_eq!(l4_req.local_port_be, local_port_be);

        assert_eq!(dgram.into_data(), payload);
    }
}
