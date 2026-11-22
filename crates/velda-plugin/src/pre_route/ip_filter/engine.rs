//! Contiguous Flat Memory CIDR Matching Engine.
//!
//! Organizes rules into two distinct contiguous partitions:
//! 1. **Exact IP Partition** (`/32` IPv4, `/128` IPv6): Evaluated first using exact equality (`==`).
//! 2. **Subnet Range Partition** (`/1..=/31` IPv4, `/1..=/127` IPv6): Evaluated using 64-bit SWAR vector arithmetic.
//!
//! ```text
//!  Contiguous Flat Memory CIDR Matching Engine Layout:
//!
//!  +─────────────────────────────────────────────────────────────────────────+
//!  | Partition 1: Exact Matches (/32 IPv4, /128 IPv6)                        |
//!  |                                                                         |
//!  |  exact_v4: [ 10.0.0.1, 10.0.0.5, 192.168.1.1 ]  (Sorted u32)           |
//!  |  exact_v6: [ 2001:db8::1, ... ]                  (Sorted u128)          |
//!  |                                                                         |
//!  |  - Checked first via equality `==` or binary search (1 cycle / O(logN)) |
//!  |  - Bypasses subnet mask memory loads entirely for single-IP lists       |
//!  +─────────────────────────────────────────────────────────────────────────+
//!                                     │
//!                              (if not matched)
//!                                     ▼
//!  +─────────────────────────────────────────────────────────────────────────+
//!  | Partition 2: Subnet Ranges (/1..=/31 IPv4, /1..=/127 IPv6)              |
//!  |                                                                         |
//!  |  subnet_v4_nets:  [ net0, net1, net2, net3, ... ] (Contiguous Vec<u32>) |
//!  |  subnet_v4_masks: [ mask0, mask1, mask2, ... ]   (Contiguous Vec<u32>) |
//!  |                                                                         |
//!  |  64-bit SWAR (2 rules/iter):                                            |
//!  |    ip64 = (ip << 32) | ip;                                              |
//!  |    diff = (ip64 & m64) ^ n64;                                           |
//!  |    if (diff as u32 == 0) || (diff >> 32 == 0) => MATCH!                 |
//!  |                                                                         |
//!  |  - Handles non-byte-aligned CIDRs (/20, /23) without state explosion    |
//!  +─────────────────────────────────────────────────────────────────────────+
//! ```

use super::config::IpNetwork;
use super::stream::ParsedIp;

/// Evaluates 2 IPv4 CIDR rules per cycle using 64-bit SWAR registers.
#[inline]
pub fn match_cidr_v4_64(ip: u32, nets: &[u32], masks: &[u32]) -> bool {
    let (n_chunks, n_rem) = nets.as_chunks::<2>();
    let (m_chunks, m_rem) = masks.as_chunks::<2>();
    let ip64 = ((ip as u64) << 32) | (ip as u64);

    for (n_pair, m_pair) in n_chunks.iter().zip(m_chunks.iter()) {
        let m64 = ((m_pair[1] as u64) << 32) | (m_pair[0] as u64);
        let n64 = ((n_pair[1] as u64) << 32) | (n_pair[0] as u64);
        let diff = (ip64 & m64) ^ n64;
        if (diff as u32) == 0 || (diff >> 32) == 0 {
            return true;
        }
    }

    if !n_rem.is_empty() && !m_rem.is_empty() {
        return (ip & m_rem[0]) == n_rem[0];
    }
    false
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn match_cidr_v4_sse2(ip: u32, nets: &[u32], masks: &[u32]) -> bool {
    use std::arch::x86_64::*;

    let (n_chunks, n_rem) = nets.as_chunks::<4>();
    let (m_chunks, m_rem) = masks.as_chunks::<4>();
    let v_ip = _mm_set1_epi32(ip as i32);

    for (n_chunk, m_chunk) in n_chunks.iter().zip(m_chunks.iter()) {
        unsafe {
            let v_mask = _mm_loadu_si128(m_chunk.as_ptr() as *const __m128i);
            let v_net = _mm_loadu_si128(n_chunk.as_ptr() as *const __m128i);
            let v_masked = _mm_and_si128(v_ip, v_mask);
            let v_cmp = _mm_cmpeq_epi32(v_masked, v_net);
            if _mm_movemask_epi8(v_cmp) != 0 {
                return true;
            }
        }
    }

    match_cidr_v4_64(ip, n_rem, m_rem)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2")]
unsafe fn match_cidr_v4_avx2(ip: u32, nets: &[u32], masks: &[u32]) -> bool {
    use std::arch::x86_64::*;

    let (n_chunks, n_rem) = nets.as_chunks::<8>();
    let (m_chunks, m_rem) = masks.as_chunks::<8>();
    let v_ip = _mm256_set1_epi32(ip as i32);

    for (n_chunk, m_chunk) in n_chunks.iter().zip(m_chunks.iter()) {
        unsafe {
            let v_mask = _mm256_loadu_si256(m_chunk.as_ptr() as *const __m256i);
            let v_net = _mm256_loadu_si256(n_chunk.as_ptr() as *const __m256i);
            let v_masked = _mm256_and_si256(v_ip, v_mask);
            let v_cmp = _mm256_cmpeq_epi32(v_masked, v_net);
            if _mm256_movemask_epi8(v_cmp) != 0 {
                return true;
            }
        }
    }

    if n_rem.len() >= 4 && m_rem.len() >= 4 {
        unsafe { match_cidr_v4_sse2(ip, n_rem, m_rem) }
    } else {
        match_cidr_v4_64(ip, n_rem, m_rem)
    }
}

/// Evaluates IPv4 CIDR rules using a hierarchical compile-time register tier:
/// 1. AVX2 256-bit (8 rules/cycle) if target_feature = "avx2".
/// 2. SSE2 128-bit (4 rules/cycle) on baseline x86_64.
/// 3. 64-bit SWAR (2 rules/cycle) on all 64-bit architectures (ARM64, etc.).
/// 4. Scalar tail (1 rule).
#[inline]
pub fn match_cidr_v4(ip: u32, nets: &[u32], masks: &[u32]) -> bool {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        if nets.len() >= 8 && masks.len() >= 8 {
            return unsafe { match_cidr_v4_avx2(ip, nets, masks) };
        }
        if nets.len() >= 4 && masks.len() >= 4 {
            return unsafe { match_cidr_v4_sse2(ip, nets, masks) };
        }
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    {
        if nets.len() >= 4 && masks.len() >= 4 {
            return unsafe { match_cidr_v4_sse2(ip, nets, masks) };
        }
    }
    match_cidr_v4_64(ip, nets, masks)
}

/// Compacts, deduplicates, and sorts IPv4 subnets by prefix length ascending (broadest first).
///
/// Automatically eliminates redundant child subnets subsumed by an earlier broader subnet.
pub fn compact_and_sort_v4(rules: &[(u32, u32)]) -> Vec<(u32, u32)> {
    if rules.is_empty() {
        return Vec::new();
    }
    let mut deduplicated: Vec<(u32, u32)> = rules.to_vec();
    deduplicated.sort_unstable_by_key(|&(net, mask)| (mask.count_ones(), net));
    deduplicated.dedup();

    let mut compacted: Vec<(u32, u32)> = Vec::with_capacity(deduplicated.len());
    for &(net, mask) in &deduplicated {
        let is_subsumed = compacted
            .iter()
            .any(|&(parent_net, parent_mask)| (net & parent_mask) == parent_net);
        if !is_subsumed {
            compacted.push((net, mask));
        }
    }
    compacted
}

/// Compacts, deduplicates, and sorts IPv6 subnets by prefix length ascending (broadest first).
pub fn compact_and_sort_v6(rules: &[(u128, u128)]) -> Vec<(u128, u128)> {
    if rules.is_empty() {
        return Vec::new();
    }
    let mut deduplicated: Vec<(u128, u128)> = rules.to_vec();
    deduplicated.sort_unstable_by_key(|&(net, mask)| (mask.count_ones(), net));
    deduplicated.dedup();

    let mut compacted: Vec<(u128, u128)> = Vec::with_capacity(deduplicated.len());
    for &(net, mask) in &deduplicated {
        let is_subsumed = compacted
            .iter()
            .any(|&(parent_net, parent_mask)| (net & parent_mask) == parent_net);
        if !is_subsumed {
            compacted.push((net, mask));
        }
    }
    compacted
}

/// Pre-compiled contiguous flat memory matching engine for IP access rules.
#[derive(Debug, Clone, Default)]
pub struct IpFilterEngine {
    // Partition 1: Exact matches (/32 IPv4, /128 IPv6)
    pub exact_v4: Vec<u32>,
    pub exact_v6: Vec<u128>,

    // Partition 2: Subnet ranges (/1..=/31 IPv4, /1..=/127 IPv6)
    pub subnet_v4_nets: Vec<u32>,
    pub subnet_v4_masks: Vec<u32>,
    pub subnet_v6: Vec<(u128, u128)>,
}

impl IpFilterEngine {
    /// Compiles a slice of [`IpNetwork`] rules into contiguous flat partitions.
    pub fn from_cidrs(cidrs: &[IpNetwork]) -> Self {
        let mut exact_v4 = Vec::new();
        let mut exact_v6 = Vec::new();
        let mut raw_subnet_v4 = Vec::new();
        let mut raw_subnet_v6 = Vec::new();

        for cidr in cidrs {
            match *cidr {
                IpNetwork::V4 { net, mask } => {
                    if mask == !0u32 {
                        exact_v4.push(net);
                    } else {
                        raw_subnet_v4.push((net, mask));
                    }
                }
                IpNetwork::V6 { net, mask } => {
                    if mask == !0u128 {
                        exact_v6.push(net);
                    } else {
                        raw_subnet_v6.push((net, mask));
                    }
                }
            }
        }

        exact_v4.sort_unstable();
        exact_v4.dedup();

        exact_v6.sort_unstable();
        exact_v6.dedup();

        let compacted_v4 = compact_and_sort_v4(&raw_subnet_v4);
        let compacted_v6 = compact_and_sort_v6(&raw_subnet_v6);

        let (subnet_v4_nets, subnet_v4_masks) = compacted_v4.into_iter().unzip();

        Self {
            exact_v4,
            exact_v6,
            subnet_v4_nets,
            subnet_v4_masks,
            subnet_v6: compacted_v6,
        }
    }

    /// Returns true if this engine contains zero rules.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.exact_v4.is_empty()
            && self.subnet_v4_nets.is_empty()
            && self.exact_v6.is_empty()
            && self.subnet_v6.is_empty()
    }

    /// Matches an IPv4 address (`u32`) against this engine.
    #[inline]
    pub fn matches_v4(&self, ip: u32) -> bool {
        // 1. Fast-path Exact IP check (/32)
        if !self.exact_v4.is_empty() {
            if self.exact_v4.len() <= 16 {
                if self.exact_v4.contains(&ip) {
                    return true;
                }
            } else if self.exact_v4.binary_search(&ip).is_ok() {
                return true;
            }
        }

        // 2. Subnet range check using AVX2 SIMD / 64-bit SWAR
        if !self.subnet_v4_nets.is_empty() {
            return match_cidr_v4(ip, &self.subnet_v4_nets, &self.subnet_v4_masks);
        }

        false
    }

    /// Matches an IPv6 address (`u128`) against this engine.
    #[inline]
    pub fn matches_v6(&self, ip: u128) -> bool {
        // 1. Fast-path Exact IP check (/128)
        if !self.exact_v6.is_empty() {
            if self.exact_v6.len() <= 16 {
                if self.exact_v6.contains(&ip) {
                    return true;
                }
            } else if self.exact_v6.binary_search(&ip).is_ok() {
                return true;
            }
        }

        // 2. Subnet range check
        for &(net, mask) in &self.subnet_v6 {
            if (ip & mask) == net {
                return true;
            }
        }

        false
    }

    /// Matches a [`ParsedIp`] (IPv4 or IPv6) against this engine.
    #[inline(always)]
    pub fn matches_ip(&self, ip: ParsedIp) -> bool {
        match ip {
            ParsedIp::V4(bits) => self.matches_v4(bits),
            ParsedIp::V6(bits) => self.matches_v6(bits),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_and_sort_v4() {
        let n1 = IpNetwork::from_cidr("10.0.0.0/8").unwrap();
        let n2 = IpNetwork::from_cidr("10.1.0.0/16").unwrap();
        let n3 = IpNetwork::from_cidr("10.1.2.3/32").unwrap();
        let n4 = IpNetwork::from_cidr("192.168.1.0/24").unwrap();

        let rules = vec![
            match n3 {
                IpNetwork::V4 { net, mask } => (net, mask),
                _ => unreachable!(),
            },
            match n2 {
                IpNetwork::V4 { net, mask } => (net, mask),
                _ => unreachable!(),
            },
            match n1 {
                IpNetwork::V4 { net, mask } => (net, mask),
                _ => unreachable!(),
            },
            match n4 {
                IpNetwork::V4 { net, mask } => (net, mask),
                _ => unreachable!(),
            },
        ];

        let compacted = compact_and_sort_v4(&rules);
        assert_eq!(compacted.len(), 2);
        // Broadest first (10.0.0.0/8 has mask prefix 8, 192.168.1.0/24 has prefix 24)
        assert_eq!(compacted[0].1.count_ones(), 8);
        assert_eq!(compacted[1].1.count_ones(), 24);
    }

    #[test]
    fn test_engine_partitioning_and_matching() {
        let cidrs = vec![
            IpNetwork::from_cidr("10.0.0.1").unwrap(),       // exact v4
            IpNetwork::from_cidr("192.168.1.0/24").unwrap(), // subnet v4
            IpNetwork::from_cidr("2001:db8::1").unwrap(),    // exact v6
            IpNetwork::from_cidr("2001:db8:a00::/40").unwrap(), // subnet v6
        ];
        let engine = IpFilterEngine::from_cidrs(&cidrs);
        assert_eq!(engine.exact_v4.len(), 1);
        assert_eq!(engine.subnet_v4_nets.len(), 1);
        assert_eq!(engine.exact_v6.len(), 1);
        assert_eq!(engine.subnet_v6.len(), 1);

        assert!(engine.matches_v4(0x0A00_0001)); // 10.0.0.1
        assert!(engine.matches_v4(0xC0A8_012A)); // 192.168.1.42
        assert!(!engine.matches_v4(0x0808_0808)); // 8.8.8.8

        assert!(engine.matches_v6(0x2001_0DB8_0000_0000_0000_0000_0000_0001));
        assert!(engine.matches_v6(0x2001_0DB8_0A12_0000_0000_0000_0000_0001));
        assert!(!engine.matches_v6(0x2001_0DB9_0000_0000_0000_0000_0000_0001));
    }
}
