//! # Zero-Copy L2 Ethernet & L3 IPv4/IPv6 Parsing
//!
//! ## In-Place Frame Processing & Datacenter Trunking
//!
//! ### 802.1Q VLAN Tagging in Datacenter Environments
//! In Kubernetes SR-IOV and trunked switch environments, inbound wire packets often include
//! an 802.1Q tag (`0x8100`, 4 bytes) between the MAC addresses and EtherType.
//! Standard 14-byte parsers misinterpret `0x8100` as the EtherType, failing IPv4/IPv6 identification.
//! [`parse_ethernet_frame`] inspects tag bytes and offsets the L3 boundary to byte 18 when present.
//!
//! ### RFC 1071 Checksum Verification
//! IPv4 and TCP/UDP header checksums are computed via the standard 16-bit one's complement sum.
//! [`compute_internet_checksum`] iterates across slices directly within UMEM slab memory
//! without allocating intermediate arrays on the heap.

use crate::error::{Result, XdpError};
use std::net::{Ipv4Addr, Ipv6Addr};

pub const ETHER_TYPE_IPV4: u16 = 0x0800;
pub const ETHER_TYPE_ARP: u16 = 0x0806;
pub const ETHER_TYPE_8021Q: u16 = 0x8100;
pub const ETHER_TYPE_IPV6: u16 = 0x86DD;

pub const IP_PROTO_TCP: u8 = 6;
pub const IP_PROTO_UDP: u8 = 17;

/// Ethernet frame header parsed from slab memory (14 bytes standard, 18 bytes with 802.1Q tag).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EthernetFrame {
    pub dst_mac: [u8; 6],
    pub src_mac: [u8; 6],
    pub ether_type: u16,
    pub header_len: usize,
}

/// Standard 20-byte IPv4 header parsed from slab memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ipv4Header {
    pub src_ip: Ipv4Addr,
    pub dst_ip: Ipv4Addr,
    pub protocol: u8,
    pub total_len: u16,
    pub header_len: usize,
}

/// Standard 40-byte IPv6 header parsed from slab memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ipv6Header {
    pub src_ip: Ipv6Addr,
    pub dst_ip: Ipv6Addr,
    pub next_header: u8,
    pub payload_len: u16,
    pub header_len: usize,
}

/// Classified Layer 3 packet referencing the underlying payload slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum L3Packet<'a> {
    Ipv4 {
        header: Ipv4Header,
        payload: &'a [u8],
    },
    Ipv6 {
        header: Ipv6Header,
        payload: &'a [u8],
    },
    Arp,
    Other(u16),
}

/// Parses an Ethernet frame from raw bytes received via NIC DMA.
pub fn parse_ethernet_frame(data: &[u8]) -> Result<(EthernetFrame, &[u8])> {
    if data.len() < 14 {
        return Err(XdpError::PacketTruncated {
            protocol: "Ethernet",
            len: data.len(),
            min_expected: 14,
        });
    }

    let mut dst_mac = [0u8; 6];
    let mut src_mac = [0u8; 6];
    dst_mac.copy_from_slice(&data[0..6]);
    src_mac.copy_from_slice(&data[6..12]);

    let mut ether_type = u16::from_be_bytes([data[12], data[13]]);
    let mut header_len = 14;

    // Handle 802.1Q VLAN Tagging (4 additional bytes)
    if ether_type == ETHER_TYPE_8021Q {
        if data.len() < 18 {
            return Err(XdpError::PacketTruncated {
                protocol: "Ethernet (802.1Q)",
                len: data.len(),
                min_expected: 18,
            });
        }
        ether_type = u16::from_be_bytes([data[16], data[17]]);
        header_len = 18;
    }

    let frame = EthernetFrame {
        dst_mac,
        src_mac,
        ether_type,
        header_len,
    };

    Ok((frame, &data[header_len..]))
}

/// Parses an IPv4 header and validates version bits.
pub fn parse_ipv4_header(data: &[u8]) -> Result<(Ipv4Header, &[u8])> {
    if data.len() < 20 {
        return Err(XdpError::PacketTruncated {
            protocol: "IPv4",
            len: data.len(),
            min_expected: 20,
        });
    }

    let version_ihl = data[0];
    let version = version_ihl >> 4;
    if version != 4 {
        return Err(XdpError::ChecksumMismatch {
            protocol: "IPv4 (Version is not 4)",
        });
    }

    // IHL (Internet Header Length) indicates header length in 32-bit words (minimum 5, or 20 bytes)
    let ihl = (version_ihl & 0x0F) as usize;
    let header_len = ihl * 4;
    if header_len < 20 || data.len() < header_len {
        return Err(XdpError::PacketTruncated {
            protocol: "IPv4 Header",
            len: data.len(),
            min_expected: header_len,
        });
    }

    let total_len = u16::from_be_bytes([data[2], data[3]]);
    let protocol = data[9];
    let src_ip = Ipv4Addr::new(data[12], data[13], data[14], data[15]);
    let dst_ip = Ipv4Addr::new(data[16], data[17], data[18], data[19]);

    let header = Ipv4Header {
        src_ip,
        dst_ip,
        protocol,
        total_len,
        header_len,
    };

    let payload_len = (total_len as usize)
        .saturating_sub(header_len)
        .min(data.len() - header_len);
    Ok((header, &data[header_len..header_len + payload_len]))
}

/// Parses a fixed 40-byte IPv6 header.
pub fn parse_ipv6_header(data: &[u8]) -> Result<(Ipv6Header, &[u8])> {
    if data.len() < 40 {
        return Err(XdpError::PacketTruncated {
            protocol: "IPv6",
            len: data.len(),
            min_expected: 40,
        });
    }

    let version = data[0] >> 4;
    if version != 6 {
        return Err(XdpError::ChecksumMismatch {
            protocol: "IPv6 (Version is not 6)",
        });
    }

    let payload_len = u16::from_be_bytes([data[4], data[5]]);
    let next_header = data[6];

    let mut src_bytes = [0u8; 16];
    let mut dst_bytes = [0u8; 16];
    src_bytes.copy_from_slice(&data[8..24]);
    dst_bytes.copy_from_slice(&data[24..40]);

    let header = Ipv6Header {
        src_ip: Ipv6Addr::from(src_bytes),
        dst_ip: Ipv6Addr::from(dst_bytes),
        next_header,
        payload_len,
        header_len: 40,
    };

    let payload_slice_len = (payload_len as usize).min(data.len() - 40);
    Ok((header, &data[40..40 + payload_slice_len]))
}

/// Classifies a Layer 3 packet by EtherType and exposes the remaining payload slice.
pub fn parse_l3_packet<'a>(ether_type: u16, data: &'a [u8]) -> Result<L3Packet<'a>> {
    match ether_type {
        ETHER_TYPE_IPV4 => {
            let (header, payload) = parse_ipv4_header(data)?;
            Ok(L3Packet::Ipv4 { header, payload })
        }
        ETHER_TYPE_IPV6 => {
            let (header, payload) = parse_ipv6_header(data)?;
            Ok(L3Packet::Ipv6 { header, payload })
        }
        ETHER_TYPE_ARP => Ok(L3Packet::Arp),
        other => Ok(L3Packet::Other(other)),
    }
}

/// Computes the standard RFC 1071 one's complement Internet checksum across byte slices.
pub fn compute_internet_checksum(data: &[u8]) -> u16 {
    let mut sum = 0u32;
    let (chunks, remainder) = data.as_chunks::<2>();

    for chunk in chunks {
        let word = u16::from_be_bytes(*chunk);
        sum = sum.wrapping_add(word as u32);
    }

    if !remainder.is_empty() {
        let word = (remainder[0] as u32) << 8;
        sum = sum.wrapping_add(word);
    }

    while (sum >> 16) != 0 {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }

    !(sum as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ethernet_and_ipv4_parsing() {
        let mut packet = vec![0u8; 34];
        packet[0..6].copy_from_slice(&[0x00, 0x11, 0x22, 0x33, 0x44, 0x55]);
        packet[6..12].copy_from_slice(&[0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB]);
        packet[12..14].copy_from_slice(&[0x08, 0x00]); // EtherType IPv4

        packet[14] = 0x45; // Version 4, IHL 5
        packet[16..18].copy_from_slice(&[0x00, 0x14]); // Total Length: 20 bytes
        packet[23] = 6; // Protocol TCP
        packet[26..30].copy_from_slice(&[192, 168, 1, 10]);
        packet[30..34].copy_from_slice(&[10, 0, 0, 1]);

        let (eth, eth_payload) = parse_ethernet_frame(&packet).expect("Ethernet parsing succeeds");
        assert_eq!(eth.ether_type, ETHER_TYPE_IPV4);
        assert_eq!(eth.header_len, 14);

        let l3 = parse_l3_packet(eth.ether_type, eth_payload).expect("L3 parsing succeeds");
        match l3 {
            L3Packet::Ipv4 { header, payload } => {
                assert_eq!(header.protocol, IP_PROTO_TCP);
                assert_eq!(header.src_ip, Ipv4Addr::new(192, 168, 1, 10));
                assert_eq!(header.dst_ip, Ipv4Addr::new(10, 0, 0, 1));
                assert_eq!(payload.len(), 0);
            }
            _ => panic!("Expected IPv4 packet"),
        }
    }
}
