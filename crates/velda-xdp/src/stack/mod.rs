//! # Userspace Network Protocol Stack (L2 / L3 / UDP / TCP)
//!
//! When running in [`XdpEngineKind::AfXdp`] mode, raw Ethernet frames arrive directly
//! into [`XdpSlabArena`] slabs from the NIC. This module provides zero-copy packet
//! decoding, framing, and single-core connection state machines for L2, L3, UDP, and TCP.

pub mod frame;
pub mod tcp;
pub mod udp;

pub use frame::{
    ETHER_TYPE_ARP, ETHER_TYPE_IPV4, ETHER_TYPE_IPV6, EthernetFrame, IP_PROTO_TCP, IP_PROTO_UDP,
    Ipv4Header, Ipv6Header, L3Packet, compute_internet_checksum, parse_ethernet_frame,
    parse_l3_packet,
};
pub use tcp::{
    TcpConnection, TcpConnectionKey, TcpConnectionTable, TcpHeader, TcpSegment, TcpState,
};
pub use udp::{UdpHeader, XdpDatagram};
