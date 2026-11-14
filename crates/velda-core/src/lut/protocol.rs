//! Canonical wire protocol variants and lookup tables (`velda-core::lut::protocol`).
//!
//! Encodes the physical transport and application wire protocol framing into a flat `u8`.
//!
//! Follows the architectural invariant of Velda Edge:
//! - Unifies downstream ingress protocols and upstream egress protocols under a single vocabulary.
//! - 100% dense (0..=6), resident in L1 Data Cache ($7 \times 16\text{B} = 112\text{B}$).

use serde::{Deserialize, Serialize};

/// Canonical wire protocol variant identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum WireProtocol {
    /// HTTP/1.1 text streaming protocol (RFC 9112).
    H1 = 0,
    /// HTTP/2 binary multiplexed framing protocol (RFC 9113).
    H2 = 1,
    /// HTTP/3 QUIC datagram packet-driven protocol (RFC 9114).
    H3 = 2,
    /// gRPC over HTTP/2 TCP streaming framing.
    GrpcTcp = 3,
    /// gRPC over HTTP/3 QUIC UDP datagram streaming.
    GrpcUdp = 4,
    /// Layer 4 raw TCP direct byte stream proxying.
    RawTcp = 5,
    /// Layer 4 raw UDP datagram forwarding.
    RawUdp = 6,
}

/// Static lookup table mapping each `WireProtocol` discriminant (0..6) directly to its
/// human-readable wire protocol name.
///
/// Resident in CPU L1 Data Cache ($7 \times 16\text{B} = 112\text{B}$).
pub static WIRE_PROTOCOL_NAMES: [&str; 7] = [
    "http/1.1", "http/2", "http/3", "grpc/tcp", "grpc/udp", "raw/tcp", "raw/udp",
];

/// Static lookup table mapping each `WireProtocol` discriminant (0..6) to its coarse
/// protocol family identifier (`"http"`, `"grpc"`, `"raw"`).
///
/// Enables log indexers (such as Loki) to group multi-stream logs in O(1) without branch prediction.
pub static WIRE_PROTOCOL_FAMILIES: [&str; 7] =
    ["http", "http", "http", "grpc", "grpc", "raw", "raw"];

impl WireProtocol {
    /// Total number of distinct wire protocol variants.
    pub const COUNT: usize = 7;

    /// Static table containing all wire protocol variants in contiguous discriminant order.
    pub const ALL: [Self; 7] = [
        Self::H1,
        Self::H2,
        Self::H3,
        Self::GrpcTcp,
        Self::GrpcUdp,
        Self::RawTcp,
        Self::RawUdp,
    ];

    /// Converts a raw `u8` discriminant into a typed [`WireProtocol`].
    ///
    /// Returns the matched variant if valid (0..=6), or [`WireProtocol::H1`] if out of bounds.
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        if (v as usize) < Self::COUNT {
            Self::ALL[v as usize]
        } else {
            Self::H1
        }
    }

    /// Converts a raw `u8` discriminant into `Some(WireProtocol)` if valid (0..=6), or `None` if out of bounds.
    #[inline(always)]
    pub const fn try_from_u8(v: u8) -> Option<Self> {
        if (v as usize) < Self::COUNT {
            Some(Self::ALL[v as usize])
        } else {
            None
        }
    }

    /// Resolves the canonical wire protocol string via O(1) direct array indexing.
    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        WIRE_PROTOCOL_NAMES[self as usize]
    }

    /// Resolves the coarse protocol family (`"http"`, `"grpc"`, `"raw"`) via O(1) direct array indexing.
    #[inline(always)]
    pub const fn family_str(self) -> &'static str {
        WIRE_PROTOCOL_FAMILIES[self as usize]
    }

    /// Returns `true` if this protocol belongs to the HTTP family (H1, H2, or H3).
    #[inline(always)]
    pub const fn is_http(self) -> bool {
        (self as u8) <= 2
    }

    /// Returns `true` if this protocol belongs to the gRPC family (gRPC TCP or gRPC UDP).
    #[inline(always)]
    pub const fn is_grpc(self) -> bool {
        let v = self as u8;
        v == 3 || v == 4
    }

    /// Returns `true` if this protocol belongs to the Raw L4 transport family (TCP or UDP).
    #[inline(always)]
    pub const fn is_raw(self) -> bool {
        (self as u8) >= 5
    }
}

const _: () = assert!(WireProtocol::COUNT == WIRE_PROTOCOL_NAMES.len());
const _: () = assert!(WireProtocol::COUNT == WIRE_PROTOCOL_FAMILIES.len());
const _: () = assert!(WireProtocol::COUNT == WireProtocol::ALL.len());
const _: () = assert!(std::mem::size_of::<WireProtocol>() == 1);

impl TryFrom<u8> for WireProtocol {
    type Error = u8;

    #[inline(always)]
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        Self::try_from_u8(v).ok_or(v)
    }
}

impl Default for WireProtocol {
    #[inline(always)]
    fn default() -> Self {
        Self::H1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_protocol_dense_roundtrip() {
        for i in 0..7u8 {
            let p = WireProtocol::from_u8(i);
            assert_eq!(p as u8, i);
            assert_eq!(WireProtocol::ALL[i as usize], p);
            assert_eq!(WIRE_PROTOCOL_NAMES[i as usize], p.as_str());
            assert_eq!(WireProtocol::try_from_u8(i), Some(p));
        }
        assert_eq!(WireProtocol::from_u8(7), WireProtocol::H1);
        assert_eq!(WireProtocol::from_u8(255), WireProtocol::H1);
        assert!(WireProtocol::try_from_u8(7).is_none());
        assert!(WireProtocol::try_from_u8(255).is_none());
    }

    #[test]
    fn test_wire_protocol_families() {
        assert_eq!(WireProtocol::H1.family_str(), "http");
        assert_eq!(WireProtocol::H2.family_str(), "http");
        assert_eq!(WireProtocol::H3.family_str(), "http");
        assert_eq!(WireProtocol::GrpcTcp.family_str(), "grpc");
        assert_eq!(WireProtocol::GrpcUdp.family_str(), "grpc");
        assert_eq!(WireProtocol::RawTcp.family_str(), "raw");
        assert_eq!(WireProtocol::RawUdp.family_str(), "raw");

        assert!(WireProtocol::H1.is_http());
        assert!(WireProtocol::GrpcTcp.is_grpc());
        assert!(WireProtocol::RawUdp.is_raw());
    }
}
