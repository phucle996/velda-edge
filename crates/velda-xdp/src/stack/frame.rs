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

// ============================================================================
// RFC 1071 Hardware-Accelerated 4-Tier Checksum Hierarchy
// ============================================================================

/// Function pointer signature for RFC 1071 Internet Checksum kernels.
pub type ChecksumFn = fn(&[u8]) -> u16;

#[inline(always)]
fn finalize_checksum(mut sum: u64) -> u16 {
    while (sum >> 16) != 0 {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }
    !(sum as u16)
}

// ----------------------------------------------------------------------------
// Tier 1: 256-bit AVX2 Checksum Kernel (32 bytes per iteration)
// ----------------------------------------------------------------------------
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn checksum_avx2_256_inner(data: &[u8]) -> u16 {
    use std::arch::x86_64::*;
    let len = data.len();
    let mut offset = 0;

    let bswap_mask = _mm256_setr_epi8(
        1, 0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14, 1, 0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10,
        13, 12, 15, 14,
    );
    let zero = _mm256_setzero_si256();
    let mut acc = _mm256_setzero_si256();

    while offset + 32 <= len {
        unsafe {
            let v = _mm256_loadu_si256(data.as_ptr().add(offset) as *const __m256i);
            let v_be = _mm256_shuffle_epi8(v, bswap_mask);
            let lo = _mm256_unpacklo_epi16(v_be, zero);
            let hi = _mm256_unpackhi_epi16(v_be, zero);
            acc = _mm256_add_epi32(acc, lo);
            acc = _mm256_add_epi32(acc, hi);
        }
        offset += 32;
    }

    let mut lanes = [0u32; 8];
    unsafe {
        _mm256_storeu_si256(lanes.as_mut_ptr() as *mut __m256i, acc);
    }
    let mut sum: u64 = lanes.iter().map(|&x| x as u64).sum();

    while offset + 2 <= len {
        let word = u16::from_be_bytes([data[offset], data[offset + 1]]);
        sum = sum.wrapping_add(word as u64);
        offset += 2;
    }
    if offset < len {
        let word = (data[offset] as u64) << 8;
        sum = sum.wrapping_add(word);
    }

    finalize_checksum(sum)
}

/// Tier 1 (256-bit AVX2): Computes RFC 1071 checksum 32 bytes per iteration.
#[cfg(target_arch = "x86_64")]
pub fn checksum_avx2_256(data: &[u8]) -> u16 {
    if is_x86_feature_detected!("avx2") {
        unsafe { checksum_avx2_256_inner(data) }
    } else {
        checksum_sse2_128(data)
    }
}

// ----------------------------------------------------------------------------
// Tier 2: 128-bit SSE2 / ARM NEON Checksum Kernel (16 bytes per iteration)
// ----------------------------------------------------------------------------
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn checksum_sse2_128_inner(data: &[u8]) -> u16 {
    use std::arch::x86_64::*;
    let len = data.len();
    let mut offset = 0;
    let zero = _mm_setzero_si128();
    let mut acc = _mm_setzero_si128();

    while offset + 16 <= len {
        unsafe {
            let v = _mm_loadu_si128(data.as_ptr().add(offset) as *const __m128i);
            let evens = _mm_and_si128(v, _mm_set1_epi16(0x00FF));
            let odds = _mm_srli_epi16(v, 8);
            let v_be = _mm_or_si128(_mm_slli_epi16(evens, 8), odds);

            let lo = _mm_unpacklo_epi16(v_be, zero);
            let hi = _mm_unpackhi_epi16(v_be, zero);
            acc = _mm_add_epi32(acc, lo);
            acc = _mm_add_epi32(acc, hi);
        }
        offset += 16;
    }

    let mut lanes = [0u32; 4];
    unsafe {
        _mm_storeu_si128(lanes.as_mut_ptr() as *mut __m128i, acc);
    }
    let mut sum: u64 = lanes.iter().map(|&x| x as u64).sum();

    while offset + 2 <= len {
        let word = u16::from_be_bytes([data[offset], data[offset + 1]]);
        sum = sum.wrapping_add(word as u64);
        offset += 2;
    }
    if offset < len {
        let word = (data[offset] as u64) << 8;
        sum = sum.wrapping_add(word);
    }

    finalize_checksum(sum)
}

/// Tier 2 (128-bit SSE2 on x86_64): Computes RFC 1071 checksum 16 bytes per iteration.
#[cfg(target_arch = "x86_64")]
pub fn checksum_sse2_128(data: &[u8]) -> u16 {
    unsafe { checksum_sse2_128_inner(data) }
}

/// Tier 2 (128-bit ARM NEON on aarch64): Computes RFC 1071 checksum 16 bytes per iteration.
#[cfg(target_arch = "aarch64")]
pub fn checksum_neon_128(data: &[u8]) -> u16 {
    use core::arch::aarch64::*;
    let len = data.len();
    let mut offset = 0;
    let mut acc = unsafe { vdupq_n_u32(0) };

    while offset + 16 <= len {
        unsafe {
            let v = vld1q_u8(data.as_ptr().add(offset));
            let v_be = vrev16q_u8(v);
            let v16 = vreinterpretq_u16_u8(v_be);
            let lo = vmovl_u16(vget_low_u16(v16));
            let hi = vmovl_u16(vget_high_u16(v16));
            acc = vaddq_u32(acc, vaddq_u32(lo, hi));
        }
        offset += 16;
    }

    let mut lanes = [0u32; 4];
    unsafe {
        vst1q_u32(lanes.as_mut_ptr(), acc);
    }
    let mut sum: u64 = lanes.iter().map(|&x| x as u64).sum();

    while offset + 2 <= len {
        let word = u16::from_be_bytes([data[offset], data[offset + 1]]);
        sum = sum.wrapping_add(word as u64);
        offset += 2;
    }
    if offset < len {
        let word = (data[offset] as u64) << 8;
        sum = sum.wrapping_add(word);
    }

    finalize_checksum(sum)
}

// ----------------------------------------------------------------------------
// Tier 3: 64-bit Dual-SWAR Checksum Kernel (8 bytes per iteration)
// ----------------------------------------------------------------------------
/// Tier 3 (64-bit Dual-SWAR): Loads 8-byte chunks and accumulates into a 64-bit sum with carry folding.
pub fn checksum_swar_64(data: &[u8]) -> u16 {
    let len = data.len();
    let mut offset = 0;
    let mut sum: u64 = 0;

    while offset + 8 <= len {
        let chunk: [u8; 8] = data[offset..offset + 8].try_into().unwrap();
        let w = u64::from_be_bytes(chunk);
        let (new_sum, overflow) = sum.overflowing_add(w);
        sum = new_sum.wrapping_add(overflow as u64);
        offset += 8;
    }

    while offset + 2 <= len {
        let word = u16::from_be_bytes([data[offset], data[offset + 1]]);
        let (new_sum, overflow) = sum.overflowing_add(word as u64);
        sum = new_sum.wrapping_add(overflow as u64);
        offset += 2;
    }

    if offset < len {
        let word = (data[offset] as u64) << 8;
        let (new_sum, overflow) = sum.overflowing_add(word);
        sum = new_sum.wrapping_add(overflow as u64);
    }

    finalize_checksum(sum)
}

/// Computes the standard RFC 1071 one's complement Internet checksum across byte slices.
#[inline]
pub fn compute_internet_checksum(data: &[u8]) -> u16 {
    #[cfg(target_arch = "x86_64")]
    {
        checksum_avx2_256(data)
    }
    #[cfg(target_arch = "aarch64")]
    {
        checksum_neon_128(data)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        checksum_swar_64(data)
    }
}

/// Hardware-accelerated RFC 1071 one's complement Internet checksum function pointer.
///
/// Kept for crate API compatibility; internally delegates directly to inlined `compute_internet_checksum`.
pub static CHECKSUM_FN: std::sync::LazyLock<ChecksumFn> =
    std::sync::LazyLock::new(|| compute_internet_checksum);

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

    #[test]
    fn test_checksum_hierarchy_edge_cases_and_consistency() {
        let edge_lengths = [0, 1, 2, 3, 14, 20, 34, 1500];
        for &len in &edge_lengths {
            let buffer: Vec<u8> = (0..len).map(|i| ((i * 37 + 13) & 0xFF) as u8).collect();
            let c_swar64 = checksum_swar_64(&buffer);

            #[cfg(target_arch = "x86_64")]
            {
                let c_sse2 = checksum_sse2_128(&buffer);
                assert_eq!(
                    c_swar64, c_sse2,
                    "Mismatch between SWAR-64 and SSE2 at length {len}"
                );
                let c_avx2 = checksum_avx2_256(&buffer);
                assert_eq!(
                    c_sse2, c_avx2,
                    "Mismatch between SSE2 and AVX2 at length {len}"
                );
            }

            #[cfg(target_arch = "aarch64")]
            {
                let c_neon = checksum_neon_128(&buffer);
                assert_eq!(
                    c_swar64, c_neon,
                    "Mismatch between SWAR-64 and NEON at length {len}"
                );
            }

            assert_eq!(
                c_swar64,
                compute_internet_checksum(&buffer),
                "CHECKSUM_FN mismatch at length {len}"
            );
        }
    }

    #[test]
    fn test_checksum_ipv4_header_known_vector() {
        // Standard IPv4 20-byte header:
        // Version 4, IHL 5, DSCP 0, Total Length 20 (0x0014)
        // Identification 0x0000, Flags/Frag 0x4000 (DF), TTL 64 (0x40), Protocol TCP (6)
        // Header checksum 0x0000 (initially)
        // Src IP: 192.168.1.1 (0xC0, 0xA8, 0x01, 0x01)
        // Dst IP: 192.168.1.2 (0xC0, 0xA8, 0x01, 0x02)
        let mut ip_header = [
            0x45, 0x00, 0x00, 0x14, 0x00, 0x00, 0x40, 0x00, 0x40, 0x06, 0x00, 0x00, 192, 168, 1, 1,
            192, 168, 1, 2,
        ];
        let csum = compute_internet_checksum(&ip_header);
        assert_ne!(csum, 0);

        // Writing the calculated checksum back into header should result in checksum == 0
        ip_header[10..12].copy_from_slice(&csum.to_be_bytes());
        let verified = compute_internet_checksum(&ip_header);
        assert_eq!(
            verified, 0,
            "Checksum verification of valid header must equal 0"
        );
    }

    #[test]
    fn test_checksum_random_buffer_permutations() {
        // Deterministic pseudo-random generation across odd/even sizes
        let mut lcg: u64 = 0xDEADBEEFCAFEBABE;
        for size in [
            5, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 512, 1024, 2048,
        ] {
            let mut data = vec![0u8; size];
            for byte in data.iter_mut() {
                lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1);
                *byte = (lcg >> 33) as u8;
            }

            let s64 = checksum_swar_64(&data);

            #[cfg(target_arch = "x86_64")]
            {
                let sse = checksum_sse2_128(&data);
                assert_eq!(s64, sse, "SWAR-64 vs SSE2 mismatch for size {size}");
                let avx = checksum_avx2_256(&data);
                assert_eq!(sse, avx, "SSE2 vs AVX2 mismatch for size {size}");
            }
        }
    }
}
