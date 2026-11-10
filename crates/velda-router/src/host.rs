//! Host and authority matching utilities for Layer 7 routers.
//!
//! Provides ultra-low-latency host cleaning, reverse block-by-block domain matching,
//! and 1-cycle suffix wildcard verification using Length-Guarded Dual-Chunk SWAR,
//! 64-bit register bitmasking, and zero heap allocations.
//!
//! # Microarchitectural Rationale
//! - **Reverse Block-by-Block Host Matching (`ReverseHostLabels`)**: Hostnames are tokenized
//!   right-to-left (TLD -> SLD -> Subdomain). Each label is encoded into a 64-bit integer
//!   register (`u64`) with case-insensitive bitmasking (`& 0xDFDF_DFDF_DFDF_DFDF`).
//! - **1-Cycle Wildcard Suffix Matching (`CompiledHost::Wildcard`)**: RFC 6125 wildcards (`*.example.com`)
//!   match rightmost suffix blocks `[u64("com"), u64("example")]` simultaneously using Dual-64 SWAR
//!   or 128-bit vector registers. Suffix equality immediately validates the route without scanning
//!   arbitrary leftmost subdomains.
//! - **Register-Adaptive Hardware Multi-Versioning**: Hardware capabilities probe CPU topology
//!   once at boot via [`global_hardware_topology()`], resolving function pointers down to 32-bit
//!   hardware (`Bits64`, `Bits32`) with zero branching on the request serving hot path.
//! - **SWAR Colon Scanning (`swar_find_colon`)**: Scans 8 bytes per iteration for port colons
//!   using 64-bit bitwise magic arithmetic, stripping ports in 1-2 CPU cycles without branch loops.

use velda_core::hardware::{RegisterWidth, global_hardware_topology};

// ============================================================================
// 1. Reverse Block-by-Block Host Matching (Right-to-Left Label Matching)
// ============================================================================

/// Encoded representation of a single DNS domain label (up to 8 bytes in 1 register).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HostLabelBlock {
    /// Case-insensitively uppercase-masked 64-bit integer value of the label.
    pub word: u64,
    /// Byte length of the label.
    pub len: u8,
}

impl HostLabelBlock {
    /// Encodes a raw label slice into a case-insensitive 64-bit [`HostLabelBlock`].
    #[inline(always)]
    pub fn encode(b: &[u8]) -> Self {
        let len = b.len();
        if len == 0 {
            return Self { word: 0, len: 0 };
        }
        let word = match len {
            1 => (b[0] & 0xDF) as u64,
            2 => {
                let w = u16::from_le_bytes([b[0], b[1]]) as u64;
                w & 0xDFDF
            }
            3 => {
                let w = (b[0] as u64) | ((b[1] as u64) << 8) | ((b[2] as u64) << 16);
                w & 0x00DF_DFDF
            }
            4 => {
                let w = u32::from_le_bytes(b[..4].try_into().unwrap()) as u64;
                w & 0xDFDF_DFDF
            }
            5 => {
                let lo = u32::from_le_bytes(b[..4].try_into().unwrap()) as u64;
                let hi = b[4] as u64;
                (lo & 0xDFDF_DFDF) | ((hi & 0xDF) << 32)
            }
            6 => {
                let lo = u32::from_le_bytes(b[..4].try_into().unwrap()) as u64;
                let hi = u16::from_le_bytes(b[4..6].try_into().unwrap()) as u64;
                (lo & 0xDFDF_DFDF) | ((hi & 0xDFDF) << 32)
            }
            7 => {
                let lo = u32::from_le_bytes(b[..4].try_into().unwrap()) as u64;
                let hi = (b[4] as u64) | ((b[5] as u64) << 8) | ((b[6] as u64) << 16);
                (lo & 0xDFDF_DFDF) | ((hi & 0x00DF_DFDF) << 32)
            }
            8 => {
                let w = u64::from_le_bytes(b[..8].try_into().unwrap());
                w & 0xDFDF_DFDF_DFDF_DFDF_u64
            }
            _ => {
                // For long labels (> 8 bytes): load first 8 bytes and fold remaining bytes
                let w = u64::from_le_bytes(b[..8].try_into().unwrap()) & 0xDFDF_DFDF_DFDF_DFDF_u64;
                let mut acc = w;
                for &byte in &b[8..] {
                    acc = acc.rotate_left(5) ^ ((byte & 0xDF) as u64);
                }
                acc
            }
        };
        Self {
            word,
            len: len as u8,
        }
    }
}

/// Tokenized reverse host labels stored on the stack (TLD at index 0, SLD at index 1, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReverseHostLabels {
    pub blocks: [HostLabelBlock; 8],
    pub count: u8,
    pub is_ip_or_unbracketed: bool,
}

impl ReverseHostLabels {
    /// Tokenizes an incoming host string from right to left into pre-encoded 64-bit blocks.
    #[inline]
    pub fn parse(host: &str) -> Self {
        let clean = clean_host(host);
        let bytes = clean.as_bytes();
        let len = bytes.len();

        if len == 0 {
            return Self::default();
        }

        // Unbracketed IPv6 (contains colons) or single word (e.g. localhost)
        if clean.contains(':') || !clean.contains('.') {
            let mut labels = Self {
                blocks: [HostLabelBlock::default(); 8],
                count: 1,
                is_ip_or_unbracketed: true,
            };
            labels.blocks[0] = HostLabelBlock::encode(bytes);
            return labels;
        }

        let mut labels = Self {
            blocks: [HostLabelBlock::default(); 8],
            count: 0,
            is_ip_or_unbracketed: false,
        };

        // Scan backwards: rightmost label (TLD) -> SLD -> Subdomain
        let mut end = len;
        let mut i = len;

        while i > 0 && (labels.count as usize) < labels.blocks.len() {
            i -= 1;
            if bytes[i] == b'.' {
                let seg = &bytes[i + 1..end];
                if !seg.is_empty() {
                    labels.blocks[labels.count as usize] = HostLabelBlock::encode(seg);
                    labels.count += 1;
                }
                end = i;
            }
        }

        if end > 0 && (labels.count as usize) < labels.blocks.len() {
            let seg = &bytes[..end];
            if !seg.is_empty() {
                labels.blocks[labels.count as usize] = HostLabelBlock::encode(seg);
                labels.count += 1;
            }
        }

        labels
    }
}

/// Signature for comparing two 64-bit label words simultaneously.
pub type CompareLabelsDualFn = fn(e0: u64, e1: u64, a0: u64, a1: u64) -> bool;

/// Pre-resolved static function pointer for Dual-64 label comparison.
pub static COMPARE_LABELS_DUAL_FN: std::sync::LazyLock<CompareLabelsDualFn> =
    std::sync::LazyLock::new(|| {
        let caps = global_hardware_topology().cpu.register_capabilities;
        match caps.select_for_workload(RegisterWidth::Bits64) {
            RegisterWidth::Bits64 | RegisterWidth::Bits128 | RegisterWidth::Bits256 => {
                compare_labels_dual_64
            }
            RegisterWidth::Bits32 => compare_labels_quad_32,
        }
    });

/// Compares two label words simultaneously using Dual-64 SWAR on 64-bit/vector hardware.
#[inline(always)]
pub fn compare_labels_dual_64(e0: u64, e1: u64, a0: u64, a1: u64) -> bool {
    ((e0 ^ a0) | (e1 ^ a1)) == 0
}

/// Compares two label words across four 32-bit registers for 32-bit hardware compatibility.
#[inline(always)]
pub fn compare_labels_quad_32(e0: u64, e1: u64, a0: u64, a1: u64) -> bool {
    let e0_lo = e0 as u32;
    let e0_hi = (e0 >> 32) as u32;
    let a0_lo = a0 as u32;
    let a0_hi = (a0 >> 32) as u32;

    let e1_lo = e1 as u32;
    let e1_hi = (e1 >> 32) as u32;
    let a1_lo = a1 as u32;
    let a1_hi = (a1 >> 32) as u32;

    ((e0_lo ^ a0_lo) | (e0_hi ^ a0_hi) | (e1_lo ^ a1_lo) | (e1_hi ^ a1_hi)) == 0
}

/// Compares two label words simultaneously with zero indirect call overhead.
///
/// Inlines directly at caller sites, compiling down to 3 instructions (`xor`, `xor`, `or`)
/// on 64-bit targets with zero stack allocations and zero function call penalty.
#[inline(always)]
pub fn compare_labels_dual(e0: u64, e1: u64, a0: u64, a1: u64) -> bool {
    #[cfg(target_pointer_width = "64")]
    {
        compare_labels_dual_64(e0, e1, a0, a1)
    }
    #[cfg(not(target_pointer_width = "64"))]
    {
        compare_labels_quad_32(e0, e1, a0, a1)
    }
}

/// Pre-compiled, zero-allocation host routing rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledHost {
    /// Matches any host (`"*"` or `None`).
    Any,
    /// Exact domain match evaluated right-to-left.
    Exact(ReverseHostLabels),
    /// RFC 6125 wildcard matching suffix labels from right to left.
    Wildcard {
        suffix: ReverseHostLabels,
        suffix_count: u8,
    },
}

impl CompiledHost {
    /// Compiles an optional host pattern into a [`CompiledHost`].
    pub fn compile(host: Option<&str>) -> Self {
        let Some(host) = host else {
            return Self::Any;
        };
        if host == "*" || host.is_empty() {
            return Self::Any;
        }

        let clean = clean_host(host);
        if let Some(suffix) = clean.strip_prefix("*.") {
            let parsed_suffix = ReverseHostLabels::parse(suffix);
            Self::Wildcard {
                suffix_count: parsed_suffix.count,
                suffix: parsed_suffix,
            }
        } else {
            Self::Exact(ReverseHostLabels::parse(clean))
        }
    }

    /// Matches this compiled host against pre-tokenized request host labels in 1-2 CPU cycles.
    #[inline(always)]
    pub fn matches(&self, actual: &ReverseHostLabels) -> bool {
        match self {
            Self::Any => true,
            Self::Wildcard {
                suffix,
                suffix_count,
            } => {
                // RFC 6125: Actual host must have at least one label to the left of suffix
                if actual.count <= *suffix_count || actual.is_ip_or_unbracketed {
                    return false;
                }

                // Standard 2-label suffix (e.g. "*.example.com" -> ["com", "example"])
                if *suffix_count == 2 {
                    let words_match = compare_labels_dual(
                        suffix.blocks[0].word,
                        suffix.blocks[1].word,
                        actual.blocks[0].word,
                        actual.blocks[1].word,
                    );
                    let lens_match = (suffix.blocks[0].len == actual.blocks[0].len)
                        && (suffix.blocks[1].len == actual.blocks[1].len);
                    return words_match && lens_match;
                }

                // General case for N suffix labels
                let sc = *suffix_count as usize;
                for i in 0..sc {
                    if suffix.blocks[i].word != actual.blocks[i].word
                        || suffix.blocks[i].len != actual.blocks[i].len
                    {
                        return false;
                    }
                }
                true
            }
            Self::Exact(expected) => {
                if expected.count != actual.count {
                    return false;
                }

                // Standard 2-label domain (e.g. "example.com" -> ["com", "example"])
                if expected.count == 2 {
                    let words_match = compare_labels_dual(
                        expected.blocks[0].word,
                        expected.blocks[1].word,
                        actual.blocks[0].word,
                        actual.blocks[1].word,
                    );
                    let lens_match = (expected.blocks[0].len == actual.blocks[0].len)
                        && (expected.blocks[1].len == actual.blocks[1].len);
                    return words_match && lens_match;
                }

                // Standard 3-label domain (e.g. "api.example.com" -> ["com", "example", "api"])
                if expected.count == 3 {
                    let suffix_words_match = compare_labels_dual(
                        expected.blocks[0].word,
                        expected.blocks[1].word,
                        actual.blocks[0].word,
                        actual.blocks[1].word,
                    );
                    let suffix_lens_match = (expected.blocks[0].len == actual.blocks[0].len)
                        && (expected.blocks[1].len == actual.blocks[1].len);
                    if !suffix_words_match || !suffix_lens_match {
                        return false;
                    }
                    return expected.blocks[2].word == actual.blocks[2].word
                        && expected.blocks[2].len == actual.blocks[2].len;
                }

                // General case for N labels
                let count = expected.count as usize;
                for i in 0..count {
                    if expected.blocks[i].word != actual.blocks[i].word
                        || expected.blocks[i].len != actual.blocks[i].len
                    {
                        return false;
                    }
                }
                true
            }
        }
    }
}

// ============================================================================
// 2. Port Stripping & Host Cleaning Utilities
// ============================================================================

/// Scans for the ASCII colon (`':'`, 0x3A) using 64-bit and 32-bit SWAR chunking
/// with hardware TZCNT zero-byte isolation.
#[inline]
pub fn swar_find_colon(bytes: &[u8]) -> Option<usize> {
    let len = bytes.len();
    let mut i = 0;

    // 64-bit SWAR chunk scan (8 bytes per iteration)
    while i + 8 <= len {
        let w = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        let x = w ^ 0x3A3A_3A3A_3A3A_3A3A_u64;
        let has_zero = (x.wrapping_sub(0x0101_0101_0101_0101_u64)) & !x & 0x8080_8080_8080_8080_u64;
        if has_zero != 0 {
            return Some(i + (has_zero.trailing_zeros() / 8) as usize);
        }
        i += 8;
    }

    // 32-bit SWAR chunk scan (4 bytes)
    if i + 4 <= len {
        let w = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap());
        let x = w ^ 0x3A3A_3A3A_u32;
        let has_zero = (x.wrapping_sub(0x0101_0101_u32)) & !x & 0x8080_8080_u32;
        if has_zero != 0 {
            return Some(i + (has_zero.trailing_zeros() / 8) as usize);
        }
        i += 4;
    }

    // Scalar tail for remainder (0..3 bytes)
    while i < len {
        if bytes[i] == b':' {
            return Some(i);
        }
        i += 1;
    }

    None
}

/// Extracts the hostname without port or IPv6 enclosing brackets.
///
/// Uses SWAR chunked scans to locate port colons in 1-2 CPU cycles.
///
/// Handles:
/// - `"example.com:8080"` -> `"example.com"`
/// - `"example.com"` -> `"example.com"`
/// - `"[::1]:8080"` -> `"::1"`
/// - `"[::1]"` -> `"::1"`
/// - `"[2001:db8::1]:443"` -> `"2001:db8::1"`
#[inline]
pub fn clean_host(host: &str) -> &str {
    if host.starts_with('[') {
        if let Some(close_bracket) = host.find(']') {
            &host[1..close_bracket]
        } else {
            host
        }
    } else {
        let bytes = host.as_bytes();
        if let Some(first_colon) = swar_find_colon(bytes) {
            // Unbracketed IPv6 has more than 1 colon (e.g. "::1" or "2001:db8::1")
            if swar_find_colon(&bytes[first_colon + 1..]).is_some() {
                host
            } else {
                &host[..first_colon]
            }
        } else {
            host
        }
    }
}

/// Compares two ASCII domain byte slices case-insensitively using Dual 64-bit SWAR registers
/// (16 bytes per cycle across ALU Port 0 & ALU Port 1).
#[inline]
pub fn eq_ignore_ascii_case_dual64_swar(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let len = a.len();
    if len == 0 {
        return true;
    }

    let mut i = 0;
    let dual_len = len & !15;

    // Dual 64-bit SWAR: process 16 bytes per cycle across ALU Port 0 & ALU Port 1
    while i < dual_len {
        let wa0 = u64::from_ne_bytes(a[i..i + 8].try_into().unwrap());
        let wb0 = u64::from_ne_bytes(b[i..i + 8].try_into().unwrap());
        let wa1 = u64::from_ne_bytes(a[i + 8..i + 16].try_into().unwrap());
        let wb1 = u64::from_ne_bytes(b[i + 8..i + 16].try_into().unwrap());

        let d0 = wa0 ^ wb0;
        let d1 = wa1 ^ wb1;

        if (d0 | d1) != 0 {
            // Fast rejection: if any non-case bits differ, early exit
            if ((d0 | d1) & 0xDFDF_DFDF_DFDF_DFDF_u64) != 0 {
                return false;
            }
            // Differing bytes must be ASCII letters ('a'-'z' / 'A'-'Z')
            if !validate_ascii_letter_diff_64(wa0, d0) || !validate_ascii_letter_diff_64(wa1, d1) {
                return false;
            }
        }
        i += 16;
    }

    // 64-bit SWAR single chunk: 8 bytes
    if i + 8 <= len {
        let wa = u64::from_ne_bytes(a[i..i + 8].try_into().unwrap());
        let wb = u64::from_ne_bytes(b[i..i + 8].try_into().unwrap());
        let d = wa ^ wb;
        if d != 0 {
            if (d & 0xDFDF_DFDF_DFDF_DFDF_u64) != 0 {
                return false;
            }
            if !validate_ascii_letter_diff_64(wa, d) {
                return false;
            }
        }
        i += 8;
    }

    // 32-bit SWAR chunk: 4 bytes
    if i + 4 <= len {
        let wa = u32::from_ne_bytes(a[i..i + 4].try_into().unwrap());
        let wb = u32::from_ne_bytes(b[i..i + 4].try_into().unwrap());
        let d = wa ^ wb;
        if d != 0 {
            if (d & 0xDFDF_DFDF_u32) != 0 {
                return false;
            }
            let mut mask = d;
            while mask != 0 {
                let byte_idx = (mask.trailing_zeros() / 8) as usize;
                let byte_val = ((wa >> (byte_idx * 8)) & 0xFF) as u8;
                let lower = byte_val | 0x20;
                if !lower.is_ascii_lowercase() {
                    return false;
                }
                mask &= !(0xFF_u32 << (byte_idx * 8));
            }
        }
        i += 4;
    }

    // Scalar tail: 0..3 remaining bytes
    while i < len {
        if !a[i].eq_ignore_ascii_case(&b[i]) {
            return false;
        }
        i += 1;
    }

    true
}

#[inline(always)]
fn validate_ascii_letter_diff_64(word: u64, diff: u64) -> bool {
    if diff == 0 {
        return true;
    }
    let mut mask = diff;
    while mask != 0 {
        let byte_idx = (mask.trailing_zeros() / 8) as usize;
        let byte_val = ((word >> (byte_idx * 8)) & 0xFF) as u8;
        let lower = byte_val | 0x20;
        if !lower.is_ascii_lowercase() {
            return false;
        }
        mask &= !(0xFF_u64 << (byte_idx * 8));
    }
    true
}

/// Returns the specificity rank for a host pattern to enforce deterministic precedence:
/// - 2: Exact host (e.g. `"api.example.com"`)
/// - 1: Suffix wildcard (e.g. `"*.example.com"`)
/// - 0: Catch-all wildcard (`"*"` or `None`)
#[inline]
pub fn host_specificity(host: Option<&str>) -> u8 {
    match host {
        Some("*") | None => 0,
        Some(h) if clean_host(h).starts_with("*.") => 1,
        Some(_) => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_host() {
        assert_eq!(clean_host("example.com"), "example.com");
        assert_eq!(clean_host("example.com:8080"), "example.com");
        assert_eq!(clean_host("[::1]"), "::1");
        assert_eq!(clean_host("[::1]:8080"), "::1");
        assert_eq!(clean_host("[2001:db8::1]:443"), "2001:db8::1");
    }

    #[test]
    fn test_reverse_host_labels_parsing() {
        let rev = ReverseHostLabels::parse("api.velda.io");
        assert_eq!(rev.count, 3);
        assert_eq!(rev.blocks[0], HostLabelBlock::encode(b"io"));
        assert_eq!(rev.blocks[1], HostLabelBlock::encode(b"velda"));
        assert_eq!(rev.blocks[2], HostLabelBlock::encode(b"api"));

        let rev_port = ReverseHostLabels::parse("sub.api.example.com:8080");
        assert_eq!(rev_port.count, 4);
        assert_eq!(rev_port.blocks[0], HostLabelBlock::encode(b"com"));
        assert_eq!(rev_port.blocks[1], HostLabelBlock::encode(b"example"));
        assert_eq!(rev_port.blocks[2], HostLabelBlock::encode(b"api"));
        assert_eq!(rev_port.blocks[3], HostLabelBlock::encode(b"sub"));
    }

    #[test]
    fn test_compiled_host_matching() {
        let any = CompiledHost::compile(None);
        let any_star = CompiledHost::compile(Some("*"));
        let exact = CompiledHost::compile(Some("api.example.com"));
        let wildcard = CompiledHost::compile(Some("*.example.com"));

        let req_api = ReverseHostLabels::parse("api.example.com:8080");
        let req_sub = ReverseHostLabels::parse("sub.api.example.com");
        let req_root = ReverseHostLabels::parse("example.com");
        let req_other = ReverseHostLabels::parse("api.other.com");

        assert!(any.matches(&req_api));
        assert!(any_star.matches(&req_api));

        assert!(exact.matches(&req_api));
        assert!(!exact.matches(&req_sub));
        assert!(!exact.matches(&req_root));
        assert!(!exact.matches(&req_other));

        assert!(wildcard.matches(&req_api));
        assert!(wildcard.matches(&req_sub));
        assert!(!wildcard.matches(&req_root)); // RFC 6125: wildcard must have >= 1 label to the left
        assert!(!wildcard.matches(&req_other));

        assert_eq!(host_specificity(Some("api.velda.io")), 2);
        assert_eq!(host_specificity(Some("*.velda.io")), 1);
        assert_eq!(host_specificity(Some("*")), 0);
        assert_eq!(host_specificity(None), 0);
    }
}
