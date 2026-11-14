//! Canonical Vocabulary & Lossless Integer IP Codecs (`velda_observability::logs::types`).
//!
//! Re-exports canonical vocabulary from [`velda_core::lut`] and provides lossless IP codecs.

use std::net::{IpAddr, Ipv6Addr};

/// Encodes an [`IpAddr`] losslessly into a 128-bit integer using RFC 4291 IPv4-mapped encoding
/// (`0x0000_0000_0000_0000_0000_FFFF_<ipv4_u32>`) so both IPv4 and IPv6 occupy a single
/// integer field without an extra version discriminator flag.
#[inline(always)]
pub const fn encode_ip_u128(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(v4) => v4.to_ipv6_mapped().to_bits(),
        IpAddr::V6(v6) => v6.to_bits(),
    }
}

/// Restores the canonical [`IpAddr`] (`IpAddr::V4` or `IpAddr::V6`) from its RFC 4291 `u128` integer.
#[inline(always)]
pub const fn decode_ip_u128(bits: u128) -> IpAddr {
    Ipv6Addr::from_bits(bits).to_canonical()
}

/// Encodes an upstream backend [`IpAddr`] into a 32-bit integer (native IPv4 bits or FNV1a-32 of IPv6).
#[inline(always)]
pub const fn encode_upstream_ip_u32(ip: IpAddr) -> u32 {
    match ip {
        IpAddr::V4(v4) => v4.to_bits(),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_bits(),
            None => {
                let octets = v6.octets();
                let mut h: u32 = 0x811c_9dc5;
                let mut i = 0;
                while i < 16 {
                    h ^= octets[i] as u32;
                    h = h.wrapping_mul(0x0100_0193);
                    i += 1;
                }
                h
            }
        },
    }
}
