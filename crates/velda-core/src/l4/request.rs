//! Layer 4 Request & Connection Metadata (`velda_core::l4::request`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     L4Request (TRANSPORT METADATA, PURE INTEGERS)                       │
//! ├──────────────────────┬──────────────────────────────────────────────────────────────────┤
//! │ connection_id: u64   │ Monotonic per-connection identifier                              │
//! │ protocol             │ TransportProtocol::{ Tcp, Udp }                                  │
//! │ peer_ip / port_be    │ u128 (IPv4-mapped ::ffff:a.b.c.d or IPv6) + u16 network order    │
//! │ local_ip / port_be   │ u128 accepted listener IP + u16 network order                    │
//! │ original_dst_ip/port │ u128 + u16 BE (SO_ORIGINAL_DST / TPROXY, 0 if unavailable)       │
//! └──────────────────────┴──────────────────────────────────────────────────────────────────┘
//! ```

/// Supported L4 transport protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportProtocol {
    Tcp,
    Udp,
}

/// L4 connection metadata stored as pure integer fields.
///
/// This structure intentionally contains only transport-level
/// information. HTTP-specific data belongs to [`crate::L7Request`].
#[derive(Debug, Clone)]
pub struct L4Request {
    /// Unique identifier of the connection.
    pub connection_id: u64,

    /// Transport protocol used by the connection.
    pub protocol: TransportProtocol,

    /// Client IP address in `u128` (IPv4-mapped `::ffff:a.b.c.d` or IPv6).
    pub peer_ip: u128,

    /// Client port in network byte order (big-endian).
    pub peer_port_be: u16,

    /// Local IP address accepted by Velda (`u128` IPv4-mapped or IPv6).
    pub local_ip: u128,

    /// Local port in network byte order (big-endian).
    pub local_port_be: u16,

    /// Original destination IP (`0` if unavailable).
    pub original_dst_ip: u128,

    /// Original destination port in network byte order (`0` if unavailable).
    pub original_dst_port_be: u16,
}

impl L4Request {
    /// Creates a new L4 request context from raw integer IP/port pairs.
    #[inline]
    pub const fn new(
        connection_id: u64,
        protocol: TransportProtocol,
        peer_ip: u128,
        peer_port_be: u16,
        local_ip: u128,
        local_port_be: u16,
    ) -> Self {
        Self {
            connection_id,
            protocol,
            peer_ip,
            peer_port_be,
            local_ip,
            local_port_be,
            original_dst_ip: 0,
            original_dst_port_be: 0,
        }
    }

    /// Sets the original destination address.
    #[inline]
    pub const fn with_original_destination(mut self, ip: u128, port_be: u16) -> Self {
        self.original_dst_ip = ip;
        self.original_dst_port_be = port_be;
        self
    }
}
