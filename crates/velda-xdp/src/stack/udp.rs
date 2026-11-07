//! # Zero-Copy UDP Framing & Datagram Delivery
//!
//! ## Ingress Delivery for HTTP/3 (QUIC) and gRPC-over-UDP
//!
//! Unlike TCP which requires stateful sequence reassembly in the network layer, UDP is
//! stateless at Layer 4:
//! - Handshakes, sequence verification, flow control, and TLS 1.3 packet encryption
//!   already execute in user space within the QUIC protocol engine ([`velda-http3`]).
//! - Inbound UDP frames arriving via `AF_XDP` are DMA-transferred directly into UMEM slabs.
//! - [`parse_inbound_datagram`] skips Layer 2/3/4 header offsets and wraps the slab in an
//!   [`XdpDatagram`] reference, allowing [`velda-http3`] to decrypt payloads in-place without
//!   heap allocations or memory copies.

use std::net::{IpAddr, SocketAddr};
use std::ops::{Deref, DerefMut};

use crate::error::{Result, XdpError};
use crate::pool::XdpSlabLease;

/// Standard 8-byte UDP header (RFC 768).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpHeader {
    pub src_port: u16,
    pub dst_port: u16,
    pub length: u16,
    pub checksum: u16,
}

/// Zero-copy UDP datagram backed by a UMEM slab.
///
/// Implements `Deref<Target = [u8]>` to allow downstream L7 protocol engines
/// to access packet bytes transparently without managing underlying slab offsets.
pub struct XdpDatagram {
    pub src: SocketAddr,
    pub dst: SocketAddr,
    pub lease: XdpSlabLease,
    pub payload_offset: usize,
    pub payload_len: usize,
}

impl XdpDatagram {
    /// Constructs a datagram wrapping a leased slab frame.
    pub fn new(
        src: SocketAddr,
        dst: SocketAddr,
        lease: XdpSlabLease,
        payload_offset: usize,
        payload_len: usize,
    ) -> Self {
        Self {
            src,
            dst,
            lease,
            payload_offset,
            payload_len,
        }
    }

    /// Accesses the UDP payload slice directly, bypassing L2/L3/L4 headers.
    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.lease.payload()[self.payload_offset..self.payload_offset + self.payload_len]
    }

    /// Mutably accesses the payload slice for in-place packet decryption.
    #[inline]
    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.lease.payload_mut()[self.payload_offset..self.payload_offset + self.payload_len]
    }
}

impl Deref for XdpDatagram {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.payload()
    }
}

impl DerefMut for XdpDatagram {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.payload_mut()
    }
}

/// Parses the 8-byte UDP header from an L3 payload slice.
pub fn parse_udp_header(data: &[u8]) -> Result<(UdpHeader, &[u8])> {
    if data.len() < 8 {
        return Err(XdpError::PacketTruncated {
            protocol: "UDP",
            len: data.len(),
            min_expected: 8,
        });
    }

    let src_port = u16::from_be_bytes([data[0], data[1]]);
    let dst_port = u16::from_be_bytes([data[2], data[3]]);
    let length = u16::from_be_bytes([data[4], data[5]]);
    let checksum = u16::from_be_bytes([data[6], data[7]]);

    if (length as usize) < 8 {
        return Err(XdpError::PacketTruncated {
            protocol: "UDP (Length field < 8 bytes)",
            len: length as usize,
            min_expected: 8,
        });
    }

    let header = UdpHeader {
        src_port,
        dst_port,
        length,
        checksum,
    };

    let payload_len = ((length as usize) - 8).min(data.len() - 8);
    Ok((header, &data[8..8 + payload_len]))
}

/// Identifies an inbound UDP frame and encapsulates it into an [`XdpDatagram`] without copying.
pub fn parse_inbound_datagram(
    src_ip: IpAddr,
    dst_ip: IpAddr,
    l3_offset: usize,
    mut lease: XdpSlabLease,
) -> Result<XdpDatagram> {
    let raw = lease.payload();
    if raw.len() < l3_offset + 8 {
        return Err(XdpError::PacketTruncated {
            protocol: "UDP Inbound",
            len: raw.len(),
            min_expected: l3_offset + 8,
        });
    }

    let udp_bytes = &raw[l3_offset..];
    let (header, _) = parse_udp_header(udp_bytes)?;

    let src = SocketAddr::new(src_ip, header.src_port);
    let dst = SocketAddr::new(dst_ip, header.dst_port);
    let payload_offset = l3_offset + 8;
    let payload_len = ((header.length as usize) - 8).min(raw.len() - payload_offset);

    lease.set_payload_len(payload_offset + payload_len);

    Ok(XdpDatagram::new(
        src,
        dst,
        lease,
        payload_offset,
        payload_len,
    ))
}
