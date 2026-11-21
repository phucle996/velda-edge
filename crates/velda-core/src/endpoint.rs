//! Physical Backend Destination Endpoint (`velda_core::endpoint`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     ENDPOINT POD LAYOUT (24 BYTES, #[repr(C, align(8))])                │
//! ├──────────────────────┬──────────┬──────────┬──────────┬─────────────────────────────────┤
//! │ 0..16 (16B)          │ 16..20   │ 20..22   │ 22..23   │ 23..24                          │
//! │ ip: u128             │ weight   │ port_be  │ is_v6    │ _pad                            │
//! │ (IPv4-mapped / IPv6) │ (u32)    │ (u16 BE) │ (0 | 1)  │ (1B align)                      │
//! └──────────────────────┴──────────┴──────────┴──────────┴─────────────────────────────────┘
//!
//!   Invariant: `Endpoint` strictly denotes a physical backend destination target.
//!   Never use `Endpoint` for ingress listeners, client sockets, or HTTP routes.
//! ```

use std::fmt;

/// Represents a concrete backend destination target as a POD raw integer struct.
#[repr(C, align(8))]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct Endpoint {
    /// 16B: IPv4-mapped (`::ffff:a.b.c.d`) or IPv6 address in big-endian integer layout.
    pub ip: u128,
    /// 4B: Load balancing weight (minimum 1).
    pub weight: u32,
    /// 2B: Destination port in network byte order (big-endian).
    pub port_be: u16,
    /// 1B: Address family flag (`0` = IPv4, `1` = IPv6).
    pub is_v6: u8,
    /// 1B: Explicit alignment padding.
    pub _pad: u8,
}

// `u128` has 16-byte alignment on x86_64, so 24 bytes of fields round up to 32 bytes.
const _: () = assert!(std::mem::size_of::<Endpoint>() == 32);

impl Endpoint {
    /// Creates an IPv4 backend destination endpoint.
    #[inline]
    pub const fn from_v4(ip: [u8; 4], port: u16, weight: u32) -> Self {
        let ipv4_u32 = u32::from_be_bytes(ip);
        let mapped = crate::lut::ip::v4_to_u128(ipv4_u32);
        Self {
            ip: mapped,
            weight: if weight == 0 { 1 } else { weight },
            port_be: port.to_be(),
            is_v6: 0,
            _pad: 0,
        }
    }

    /// Creates an IPv6 backend destination endpoint.
    #[inline]
    pub const fn from_v6(ip: [u8; 16], port: u16, weight: u32) -> Self {
        Self {
            ip: u128::from_be_bytes(ip),
            weight: if weight == 0 { 1 } else { weight },
            port_be: port.to_be(),
            is_v6: 1,
            _pad: 0,
        }
    }

    /// Creates an `Endpoint` directly from raw `(ip, port_be, weight)` integers.
    #[inline]
    pub const fn from_raw(ip: u128, port_be: u16, weight: u32) -> Self {
        Self {
            ip,
            weight: if weight == 0 { 1 } else { weight },
            port_be,
            is_v6: if crate::lut::ip::is_v4_u128(ip) { 0 } else { 1 },
            _pad: 0,
        }
    }
}

impl fmt::Display for Endpoint {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (buf, len) = crate::lut::ip::format_ip_bytes(self.ip);
        let ip_str = std::str::from_utf8(&buf[..len]).unwrap_or("invalid-ip");
        let port = u16::from_be(self.port_be);
        if self.is_v6 != 0 {
            write!(f, "[{ip_str}]:{port}")
        } else {
            write!(f, "{ip_str}:{port}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_creation() {
        let ep = Endpoint::from_v4([10, 0, 0, 1], 8080, 10);
        assert_eq!(u16::from_be(ep.port_be), 8080);
        assert_eq!(ep.is_v6, 0);
        assert_eq!(ep.weight, 10);
        assert_eq!(ep.to_string(), "10.0.0.1:8080");

        let ep6 = Endpoint::from_v6(
            [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            9090,
            5,
        );
        assert_eq!(u16::from_be(ep6.port_be), 9090);
        assert_eq!(ep6.is_v6, 1);
        assert_eq!(ep6.weight, 5);
        assert_eq!(ep6.to_string(), "[2001:db8::1]:9090");

        let ep_raw = Endpoint::from_raw(ep.ip, ep.port_be, 7);
        assert_eq!(ep_raw.is_v6, 0);
        assert_eq!(ep_raw.to_string(), "10.0.0.1:8080");
    }

    #[test]
    fn test_endpoint_min_weight() {
        let ep = Endpoint::from_v4([10, 0, 0, 1], 8080, 0);
        assert_eq!(ep.weight, 1);
    }
}
