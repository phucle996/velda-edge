//! Canonical edge pipeline execution variants (`velda-core::lut::pipe`).
//!
//! Encodes the complete physical forwarding pipeline path (downstream generation, upstream generation,
//! and streaming pipe strategy) as a single flat `u8` discriminant.
//!
//! Follows the architectural invariant of Velda Edge:
//! - Frontline hot path workers emit a single compile-time integer constant (1 byte `mov`).
//! - Shadow workers and log shippers perform O(1) direct array indexing without branch prediction overhead.

use serde::{Deserialize, Serialize};

use super::protocol::WireProtocol;

/// Canonical pipeline execution variant identifier.
///
/// Exactly 64 discrete pipeline configurations spanning HTTP/1.1, HTTP/2, HTTP/3,
/// gRPC over TCP/UDP/RDMA, and Raw L4 TCP/UDP/RDMA.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum PipeKind {
    // -------------------------------------------------------------------------
    // 1. HTTP/1.1 Downstream (0..11)
    // -------------------------------------------------------------------------
    BufferedH1H1 = 0,
    ServerStreamH1H1 = 1,
    ClientStreamH1H1 = 2,
    DuplexH1H1 = 3,

    BufferedH1H2 = 4,
    ServerStreamH1H2 = 5,
    ClientStreamH1H2 = 6,
    DuplexH1H2 = 7,

    BufferedH1H3 = 8,
    ServerStreamH1H3 = 9,
    ClientStreamH1H3 = 10,
    DuplexH1H3 = 11,

    // -------------------------------------------------------------------------
    // 2. HTTP/2 Downstream (12..23)
    // -------------------------------------------------------------------------
    BufferedH2H1 = 12,
    ServerStreamH2H1 = 13,
    ClientStreamH2H1 = 14,
    DuplexH2H1 = 15,

    BufferedH2H2 = 16,
    ServerStreamH2H2 = 17,
    ClientStreamH2H2 = 18,
    DuplexH2H2 = 19,

    BufferedH2H3 = 20,
    ServerStreamH2H3 = 21,
    ClientStreamH2H3 = 22,
    DuplexH2H3 = 23,

    // -------------------------------------------------------------------------
    // 3. HTTP/3 Downstream (24..35)
    // -------------------------------------------------------------------------
    BufferedH3H1 = 24,
    ServerStreamH3H1 = 25,
    ClientStreamH3H1 = 26,
    DuplexH3H1 = 27,

    BufferedH3H2 = 28,
    ServerStreamH3H2 = 29,
    ClientStreamH3H2 = 30,
    DuplexH3H2 = 31,

    BufferedH3H3 = 32,
    ServerStreamH3H3 = 33,
    ClientStreamH3H3 = 34,
    DuplexH3H3 = 35,

    // -------------------------------------------------------------------------
    // 4. gRPC TCP Downstream (36..47)
    // -------------------------------------------------------------------------
    BufferedGrpcTcpTcp = 36,
    ServerStreamGrpcTcpTcp = 37,
    ClientStreamGrpcTcpTcp = 38,
    DuplexGrpcTcpTcp = 39,

    BufferedGrpcTcpUdp = 40,
    ServerStreamGrpcTcpUdp = 41,
    ClientStreamGrpcTcpUdp = 42,
    DuplexGrpcTcpUdp = 43,

    BufferedGrpcTcpRdma = 44,
    ServerStreamGrpcTcpRdma = 45,
    ClientStreamGrpcTcpRdma = 46,
    DuplexGrpcTcpRdma = 47,

    // -------------------------------------------------------------------------
    // 5. gRPC UDP Downstream (48..59)
    // -------------------------------------------------------------------------
    BufferedGrpcUdpTcp = 48,
    ServerStreamGrpcUdpTcp = 49,
    ClientStreamGrpcUdpTcp = 50,
    DuplexGrpcUdpTcp = 51,

    BufferedGrpcUdpUdp = 52,
    ServerStreamGrpcUdpUdp = 53,
    ClientStreamGrpcUdpUdp = 54,
    DuplexGrpcUdpUdp = 55,

    BufferedGrpcUdpRdma = 56,
    ServerStreamGrpcUdpRdma = 57,
    ClientStreamGrpcUdpRdma = 58,
    DuplexGrpcUdpRdma = 59,

    // -------------------------------------------------------------------------
    // 6. Raw L4 Transport (60..63)
    // -------------------------------------------------------------------------
    TcpTcp = 60,
    TcpRdma = 61,
    UdpUdp = 62,
    UdpRdma = 63,
}

/// Static lookup table mapping each `PipeKind` discriminant (0..63) directly to its
/// human-readable ASCII representation.
///
/// Resident in CPU L1 Data Cache ($64 \times 16\text{B} = 1,024\text{B}$).
/// Enables zero-branch, zero-allocation log and metric serialization.
pub static PIPE_NAMES: [&str; 64] = [
    // 0..11: H1 Downstream
    "buffered_h1_h1",
    "server_stream_h1_h1",
    "client_stream_h1_h1",
    "duplex_h1_h1",
    "buffered_h1_h2",
    "server_stream_h1_h2",
    "client_stream_h1_h2",
    "duplex_h1_h2",
    "buffered_h1_h3",
    "server_stream_h1_h3",
    "client_stream_h1_h3",
    "duplex_h1_h3",
    // 12..23: H2 Downstream
    "buffered_h2_h1",
    "server_stream_h2_h1",
    "client_stream_h2_h1",
    "duplex_h2_h1",
    "buffered_h2_h2",
    "server_stream_h2_h2",
    "client_stream_h2_h2",
    "duplex_h2_h2",
    "buffered_h2_h3",
    "server_stream_h2_h3",
    "client_stream_h2_h3",
    "duplex_h2_h3",
    // 24..35: H3 Downstream
    "buffered_h3_h1",
    "server_stream_h3_h1",
    "client_stream_h3_h1",
    "duplex_h3_h1",
    "buffered_h3_h2",
    "server_stream_h3_h2",
    "client_stream_h3_h2",
    "duplex_h3_h2",
    "buffered_h3_h3",
    "server_stream_h3_h3",
    "client_stream_h3_h3",
    "duplex_h3_h3",
    // 36..47: gRPC TCP Downstream
    "buffered_grpc_tcp_tcp",
    "server_stream_grpc_tcp_tcp",
    "client_stream_grpc_tcp_tcp",
    "duplex_grpc_tcp_tcp",
    "buffered_grpc_tcp_udp",
    "server_stream_grpc_tcp_udp",
    "client_stream_grpc_tcp_udp",
    "duplex_grpc_tcp_udp",
    "buffered_grpc_tcp_rdma",
    "server_stream_grpc_tcp_rdma",
    "client_stream_grpc_tcp_rdma",
    "duplex_grpc_tcp_rdma",
    // 48..59: gRPC UDP Downstream
    "buffered_grpc_udp_tcp",
    "server_stream_grpc_udp_tcp",
    "client_stream_grpc_udp_tcp",
    "duplex_grpc_udp_tcp",
    "buffered_grpc_udp_udp",
    "server_stream_grpc_udp_udp",
    "client_stream_grpc_udp_udp",
    "duplex_grpc_udp_udp",
    "buffered_grpc_udp_rdma",
    "server_stream_grpc_udp_rdma",
    "client_stream_grpc_udp_rdma",
    "duplex_grpc_udp_rdma",
    // 60..63: Raw L4 Transport
    "tcp_tcp",
    "tcp_rdma",
    "udp_udp",
    "udp_rdma",
];

impl PipeKind {
    /// Number of distinct pipeline execution variants.
    pub const COUNT: usize = 64;

    /// Static lookup table of all 64 discrete pipeline configurations in discriminant order.
    pub const ALL: [Self; 64] = [
        // 0..11: H1 Downstream
        Self::BufferedH1H1,
        Self::ServerStreamH1H1,
        Self::ClientStreamH1H1,
        Self::DuplexH1H1,
        Self::BufferedH1H2,
        Self::ServerStreamH1H2,
        Self::ClientStreamH1H2,
        Self::DuplexH1H2,
        Self::BufferedH1H3,
        Self::ServerStreamH1H3,
        Self::ClientStreamH1H3,
        Self::DuplexH1H3,
        // 12..23: H2 Downstream
        Self::BufferedH2H1,
        Self::ServerStreamH2H1,
        Self::ClientStreamH2H1,
        Self::DuplexH2H1,
        Self::BufferedH2H2,
        Self::ServerStreamH2H2,
        Self::ClientStreamH2H2,
        Self::DuplexH2H2,
        Self::BufferedH2H3,
        Self::ServerStreamH2H3,
        Self::ClientStreamH2H3,
        Self::DuplexH2H3,
        // 24..35: H3 Downstream
        Self::BufferedH3H1,
        Self::ServerStreamH3H1,
        Self::ClientStreamH3H1,
        Self::DuplexH3H1,
        Self::BufferedH3H2,
        Self::ServerStreamH3H2,
        Self::ClientStreamH3H2,
        Self::DuplexH3H2,
        Self::BufferedH3H3,
        Self::ServerStreamH3H3,
        Self::ClientStreamH3H3,
        Self::DuplexH3H3,
        // 36..47: gRPC TCP Downstream
        Self::BufferedGrpcTcpTcp,
        Self::ServerStreamGrpcTcpTcp,
        Self::ClientStreamGrpcTcpTcp,
        Self::DuplexGrpcTcpTcp,
        Self::BufferedGrpcTcpUdp,
        Self::ServerStreamGrpcTcpUdp,
        Self::ClientStreamGrpcTcpUdp,
        Self::DuplexGrpcTcpUdp,
        Self::BufferedGrpcTcpRdma,
        Self::ServerStreamGrpcTcpRdma,
        Self::ClientStreamGrpcTcpRdma,
        Self::DuplexGrpcTcpRdma,
        // 48..59: gRPC UDP Downstream
        Self::BufferedGrpcUdpTcp,
        Self::ServerStreamGrpcUdpTcp,
        Self::ClientStreamGrpcUdpTcp,
        Self::DuplexGrpcUdpTcp,
        Self::BufferedGrpcUdpUdp,
        Self::ServerStreamGrpcUdpUdp,
        Self::ClientStreamGrpcUdpUdp,
        Self::DuplexGrpcUdpUdp,
        Self::BufferedGrpcUdpRdma,
        Self::ServerStreamGrpcUdpRdma,
        Self::ClientStreamGrpcUdpRdma,
        Self::DuplexGrpcUdpRdma,
        // 60..63: Raw L4 Transport
        Self::TcpTcp,
        Self::TcpRdma,
        Self::UdpUdp,
        Self::UdpRdma,
    ];

    /// Resolves the static human-readable pipe identifier via O(1) table lookup.
    ///
    /// Since `PipeKind` has exactly 64 contiguous discriminants (0..=63),
    /// this is a pure direct array index with zero branches and zero heap allocations.
    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        PIPE_NAMES[self as usize]
    }

    /// Converts a raw `u8` discriminant into a typed [`PipeKind`] if valid (0..=63).
    ///
    /// Returns `Some(PipeKind)` for valid discriminants, or `None` if out of bounds.
    /// Pure safe Rust, zero `unsafe`, zero artificial fallbacks.
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Option<Self> {
        if (v as usize) < Self::COUNT {
            Some(Self::ALL[v as usize])
        } else {
            None
        }
    }

    /// Returns `true` if this pipeline serves HTTP downstream traffic (H1, H2, or H3).
    #[inline(always)]
    pub const fn is_http(self) -> bool {
        (self as u8) < 36
    }

    /// Returns `true` if this pipeline serves gRPC downstream traffic (over TCP or UDP).
    #[inline(always)]
    pub const fn is_grpc(self) -> bool {
        let v = self as u8;
        v >= 36 && v < 60
    }

    /// Returns `true` if this pipeline serves Raw L4 transport traffic (TCP or UDP).
    #[inline(always)]
    pub const fn is_raw(self) -> bool {
        (self as u8) >= 60
    }

    /// Returns the downstream wire protocol variant.
    #[inline(always)]
    pub const fn downstream_protocol(self) -> WireProtocol {
        match self as u8 {
            0..=11 => WireProtocol::H1,
            12..=23 => WireProtocol::H2,
            24..=35 => WireProtocol::H3,
            36..=47 => WireProtocol::GrpcTcp,
            48..=59 => WireProtocol::GrpcUdp,
            60 | 61 => WireProtocol::RawTcp,
            62 | 63 => WireProtocol::RawUdp,
            _ => WireProtocol::H1,
        }
    }

    /// Returns the upstream wire protocol variant.
    #[inline(always)]
    pub const fn upstream_protocol(self) -> WireProtocol {
        match self as u8 {
            // H1 downstream
            0..=3 => WireProtocol::H1,
            4..=7 => WireProtocol::H2,
            8..=11 => WireProtocol::H3,
            // H2 downstream
            12..=15 => WireProtocol::H1,
            16..=19 => WireProtocol::H2,
            20..=23 => WireProtocol::H3,
            // H3 downstream
            24..=27 => WireProtocol::H1,
            28..=31 => WireProtocol::H2,
            32..=35 => WireProtocol::H3,
            // Grpc TCP downstream
            36..=39 => WireProtocol::GrpcTcp,
            40..=43 => WireProtocol::GrpcUdp,
            44..=47 => WireProtocol::GrpcTcp, // GrpcRdma transport
            // Grpc UDP downstream
            48..=51 => WireProtocol::GrpcTcp,
            52..=55 => WireProtocol::GrpcUdp,
            56..=59 => WireProtocol::GrpcTcp,
            // Raw L4
            60 | 61 => WireProtocol::RawTcp,
            62 | 63 => WireProtocol::RawUdp,
            _ => WireProtocol::H1,
        }
    }

    /// Returns the downstream protocol discriminant as raw `u8` (0: H1, 1: H2, 2: H3, 3: GrpcTcp, 4: GrpcUdp, 5: RawTcp, 6: RawUdp).
    #[inline(always)]
    pub const fn downstream_protocol_u8(self) -> u8 {
        self.downstream_protocol() as u8
    }

    /// Returns the upstream protocol discriminant as raw `u8` (0: H1, 1: H2, 2: H3, 3: GrpcTcp, 4: GrpcUdp, 5: RawTcp, 6: RawUdp).
    #[inline(always)]
    pub const fn upstream_protocol_u8(self) -> u8 {
        self.upstream_protocol() as u8
    }

    /// Returns the streaming pipe strategy (0: Buffered, 1: ServerStream, 2: ClientStream, 3: Duplex).
    #[inline(always)]
    pub const fn pipe_strategy_u8(self) -> u8 {
        let v = self as u8;
        if v < 60 {
            v % 4
        } else {
            0 // Raw L4 maps to Buffered/Direct
        }
    }

    /// Resolves the canonical [`PipeKind`] from 3 orthogonal dimensions
    /// (`downstream_proto: u8`, `upstream_proto: u8`, `pipe_strategy: u8`).
    pub const fn from_protocols(
        downstream_proto: u8,
        upstream_proto: u8,
        pipe_strategy: u8,
    ) -> Self {
        let strat = pipe_strategy & 0x03; // clamp 0..3
        let id = match downstream_proto {
            // H1 downstream
            0 => match upstream_proto {
                0 => strat,
                1 => 4 + strat,
                2 => 8 + strat,
                _ => strat,
            },
            // H2 downstream
            1 => match upstream_proto {
                0 => 12 + strat,
                1 => 16 + strat,
                2 => 20 + strat,
                _ => 16 + strat,
            },
            // H3 downstream
            2 => match upstream_proto {
                0 => 24 + strat,
                1 => 28 + strat,
                2 => 32 + strat,
                _ => 32 + strat,
            },
            // gRPC TCP downstream
            3 => match upstream_proto {
                3 => 36 + strat,
                4 => 40 + strat,
                _ => 44 + strat,
            },
            // gRPC UDP downstream
            4 => match upstream_proto {
                3 => 48 + strat,
                4 => 52 + strat,
                _ => 56 + strat,
            },
            // Raw TCP downstream
            5 => match upstream_proto {
                5 => 60,
                _ => 61,
            },
            // Raw UDP downstream
            6 => match upstream_proto {
                6 => 62,
                _ => 63,
            },
            _ => 0,
        };
        Self::ALL[id as usize]
    }
}

const _: () = assert!(PipeKind::COUNT == PIPE_NAMES.len());
const _: () = assert!(PipeKind::COUNT == PipeKind::ALL.len());
const _: () = assert!(std::mem::size_of::<PipeKind>() == 1);

impl TryFrom<u8> for PipeKind {
    type Error = u8;

    #[inline(always)]
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match Self::from_u8(v) {
            Some(kind) => Ok(kind),
            None => Err(v),
        }
    }
}

impl Default for PipeKind {
    #[inline(always)]
    fn default() -> Self {
        Self::BufferedH1H1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipe_kind_dense_roundtrip() {
        for i in 0..64u8 {
            let p = PipeKind::from_u8(i).expect("all 0..64 must be valid");
            assert_eq!(p as u8, i);
            assert_eq!(PipeKind::ALL[i as usize], p);
            assert_eq!(PIPE_NAMES[i as usize], p.as_str());
        }
        assert!(PipeKind::from_u8(64).is_none());
        assert!(PipeKind::from_u8(255).is_none());
    }

    #[test]
    fn test_protocols() {
        assert_eq!(
            PipeKind::BufferedH1H1.downstream_protocol(),
            WireProtocol::H1
        );
        assert_eq!(PipeKind::BufferedH1H1.upstream_protocol(), WireProtocol::H1);
        assert_eq!(
            PipeKind::BufferedH2H3.downstream_protocol(),
            WireProtocol::H2
        );
        assert_eq!(PipeKind::BufferedH2H3.upstream_protocol(), WireProtocol::H3);
        assert_eq!(PipeKind::TcpTcp.downstream_protocol(), WireProtocol::RawTcp);
        assert_eq!(PipeKind::TcpTcp.upstream_protocol(), WireProtocol::RawTcp);
    }
}
