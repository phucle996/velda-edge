//! CIDR Bitwise Mask Matching, Subnet Aggregation, and Client IP Address Extraction.
//!
//! Provides zero-allocation, bitwise IP subnet checking for IPv4 and IPv6:
//! `(ip_bits & mask) == net_bits`.
//!
//! # Edge Security Invariants:
//! - **IPv4-Mapped / Translated IPv6 Canonicalization**: Dual-stack (`::ffff:a.b.c.d`), SIIT (`::ffff:0:a.b.c.d`),
//!   NAT64 (`64:ff9b::a.b.c.d`), and deprecated IPv4-compatible (`::a.b.c.d`) addresses are automatically
//!   canonicalized to native IPv4 (`u32`) to prevent CIDR bypass.
//! - **Octal / Leading-Zero IPv4 Defense (CVE-2021-28918)**: Strict decimal parser rejects leading zeros
//!   (e.g. `010.0.0.1`, `0177.0.0.1`) to prevent octal-vs-decimal parser differential bypasses.
//! - **Multi-Hop XFF & Multi-Header Anti-Smuggling**: Inspects and validates every hop across all header
//!   instances on the stack (`ExtractedIps`), rejecting malformed tokens, comma smuggling in `X-Real-IP`,
//!   cross-header fallback poisoning, and excessive hop floods (> 16 hops).
//! - **Strict Port & OWS Validation**: Validates optional ports (`1..=65535`) and restricts whitespace
//!   strictly to RFC 9110 OWS (`SP` / `HTAB`), rejecting control characters (`\r`, `\n`, `\x0c`).
//! - **Subnet Compaction & Deduplication**: Sorts CIDRs by prefix length ascending (broadest first) and
//!   eliminates redundant subsumed rules.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use http::header::HeaderName;
use velda_core::L7Request;

pub const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
pub const X_REAL_IP: HeaderName = HeaderName::from_static("x-real-ip");

/// Maximum number of `X-Forwarded-For` hops permitted before rejecting as a hop-flood attack.
pub const MAX_XFF_HOPS: usize = 16;

/// Maximum number of duplicate `X-Forwarded-For` header lines permitted.
pub const MAX_XFF_HEADERS: usize = 4;

/// Parsed client IP representation in native integer bit formats for single-cycle bitwise matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedIp {
    V4(u32),
    V6(u128),
}

impl ParsedIp {
    /// Converts the parsed bit representation into a standard [`IpAddr`].
    #[inline]
    pub fn to_ip_addr(self) -> IpAddr {
        match self {
            Self::V4(bits) => IpAddr::V4(Ipv4Addr::from_bits(bits)),
            Self::V6(bits) => IpAddr::V6(Ipv6Addr::from_bits(bits)),
        }
    }
}

/// Zero-allocation stack container holding all validated IP hops extracted from request headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtractedIps {
    pub ips: [ParsedIp; MAX_XFF_HOPS],
    pub len: usize,
}

impl ExtractedIps {
    #[inline(always)]
    pub fn as_slice(&self) -> &[ParsedIp] {
        &self.ips[..self.len]
    }
}

/// Status of IP extraction from request forwarding headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpExtractionError {
    /// Forwarding header is completely absent from the request.
    Missing,
    /// Forwarding header is present but contains invalid, unparseable, or malformed IP syntax.
    Malformed,
}

/// Preferred source for determining the downstream client's IP address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientIpSource {
    /// Inspects the `X-Forwarded-For` header first, falling back to `X-Real-IP` only if absent.
    #[default]
    XForwardedFor,
    /// Inspects the `X-Real-IP` header first, falling back to `X-Forwarded-For` only if absent.
    XRealIp,
}

impl ClientIpSource {
    /// Extracts and validates all downstream client IP hops into a stack-allocated [`ExtractedIps`].
    ///
    /// # Anti-Evasion Contract
    /// Never falls back to secondary headers if the primary header is present but `Malformed`.
    #[inline]
    pub fn extract_all_parsed_ips(
        &self,
        req: &L7Request,
    ) -> Result<ExtractedIps, IpExtractionError> {
        match self {
            Self::XForwardedFor => match extract_xff_all_parsed(req) {
                Ok(ips) => Ok(ips),
                Err(IpExtractionError::Missing) => extract_real_ip_all_parsed(req),
                Err(IpExtractionError::Malformed) => Err(IpExtractionError::Malformed),
            },
            Self::XRealIp => match extract_real_ip_all_parsed(req) {
                Ok(ips) => Ok(ips),
                Err(IpExtractionError::Missing) => extract_xff_all_parsed(req),
                Err(IpExtractionError::Malformed) => Err(IpExtractionError::Malformed),
            },
        }
    }

    /// Extracts and parses the primary downstream client IP into high-performance [`ParsedIp`].
    #[inline]
    pub fn extract_parsed_ip(&self, req: &L7Request) -> Result<ParsedIp, IpExtractionError> {
        self.extract_all_parsed_ips(req).map(|ext| ext.ips[0])
    }

    /// Extracts the candidate downstream client IP address as standard [`IpAddr`].
    #[inline]
    pub fn extract_client_ip(&self, req: &L7Request) -> Option<IpAddr> {
        self.extract_parsed_ip(req).ok().map(|p| p.to_ip_addr())
    }
}

#[inline]
fn extract_xff_all_parsed(req: &L7Request) -> Result<ExtractedIps, IpExtractionError> {
    let mut iter = req.headers.get_all(&X_FORWARDED_FOR).iter();
    let Some(first_hdr) = iter.next() else {
        return Err(IpExtractionError::Missing);
    };

    let mut ips = [ParsedIp::V4(0); MAX_XFF_HOPS];
    let mut count = 0usize;
    let mut hdr_count = 0usize;

    let mut current_hdr = Some(first_hdr);
    while let Some(val) = current_hdr {
        hdr_count += 1;
        if hdr_count > MAX_XFF_HEADERS {
            return Err(IpExtractionError::Malformed);
        }
        let bytes = val.as_bytes();
        if bytes.is_empty() {
            return Err(IpExtractionError::Malformed);
        }
        for token in bytes.split(|&b| b == b',') {
            if count >= MAX_XFF_HOPS {
                return Err(IpExtractionError::Malformed);
            }
            let parsed = parse_ip_bytes(token).ok_or(IpExtractionError::Malformed)?;
            ips[count] = parsed;
            count += 1;
        }
        current_hdr = iter.next();
    }

    if count == 0 {
        return Err(IpExtractionError::Malformed);
    }

    Ok(ExtractedIps { ips, len: count })
}

#[inline]
fn extract_real_ip_all_parsed(req: &L7Request) -> Result<ExtractedIps, IpExtractionError> {
    let mut iter = req.headers.get_all(&X_REAL_IP).iter();
    let Some(val) = iter.next() else {
        return Err(IpExtractionError::Missing);
    };
    // Singleton header invariant: multiple X-Real-IP headers are rejected as header smuggling
    if iter.next().is_some() {
        return Err(IpExtractionError::Malformed);
    }
    let bytes = val.as_bytes();
    // X-Real-IP must not contain comma-separated lists
    if bytes.is_empty() || bytes.contains(&b',') {
        return Err(IpExtractionError::Malformed);
    }
    let parsed = parse_ip_bytes(bytes).ok_or(IpExtractionError::Malformed)?;
    let mut ips = [ParsedIp::V4(0); MAX_XFF_HOPS];
    ips[0] = parsed;
    Ok(ExtractedIps { ips, len: 1 })
}

#[inline(always)]
fn is_http_ows(b: u8) -> bool {
    b == b' ' || b == b'\t'
}

#[inline]
fn is_valid_port_bytes(port: &[u8]) -> bool {
    if port.is_empty() || port.len() > 5 || port[0] == b'0' {
        return false;
    }
    let mut val: u32 = 0;
    for &b in port {
        if !b.is_ascii_digit() {
            return false;
        }
        val = val * 10 + (b - b'0') as u32;
    }
    (1..=65535).contains(&val)
}

/// High-speed byte-level IP parser handling IPv4, IPv6, optional ports, quotes, and dual-stack canonicalization.
#[inline]
pub fn parse_ip_bytes(mut bytes: &[u8]) -> Option<ParsedIp> {
    // 1. Trim strict RFC 9110 OWS (SP / HTAB only; rejects \r, \n, \x0c control chars)
    while let Some((&first, rest)) = bytes.split_first() {
        if is_http_ows(first) {
            bytes = rest;
        } else {
            break;
        }
    }
    while let Some((&last, rest)) = bytes.split_last() {
        if is_http_ows(last) {
            bytes = rest;
        } else {
            break;
        }
    }
    if bytes.is_empty() {
        return None;
    }

    // 2. Strip a single pair of surrounding double quotes if present (e.g. "192.168.1.1")
    if bytes.len() >= 2 && bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"' {
        bytes = &bytes[1..bytes.len() - 1];
        // Do not allow nested quotes or inner whitespace padding inside quotes
        if bytes.is_empty() || bytes.contains(&b'"') {
            return None;
        }
    }

    // 3. Bracketed IPv6 with optional validated port: `[2001:db8::1]:8080` or `[2001:db8::1]`
    if bytes[0] == b'[' {
        let close_pos = bytes.iter().position(|&b| b == b']')?;
        let ip_slice = &bytes[1..close_pos];
        let remainder = &bytes[close_pos + 1..];
        if !remainder.is_empty() && (remainder[0] != b':' || !is_valid_port_bytes(&remainder[1..]))
        {
            return None;
        }
        return parse_ipv6_str_canonical(ip_slice);
    }

    // 4. Check for IPv4 with optional validated port: e.g. `192.168.1.1:8080`
    let colon_count = bytes.iter().filter(|&&b| b == b':').count();
    let has_dots = bytes.contains(&b'.');

    if colon_count == 1 && has_dots {
        let colon_pos = bytes.iter().position(|&b| b == b':')?;
        let ip_slice = &bytes[..colon_pos];
        let port_slice = &bytes[colon_pos + 1..];
        if !is_valid_port_bytes(port_slice) {
            return None;
        }
        return parse_ipv4_bytes(ip_slice).map(ParsedIp::V4);
    }

    // 5. Fast-path IPv4 parse
    if colon_count == 0
        && has_dots
        && let Some(bits) = parse_ipv4_bytes(bytes)
    {
        return Some(ParsedIp::V4(bits));
    }

    // 6. IPv6 (plain or IPv4-mapped / translated)
    parse_ipv6_str_canonical(bytes)
}

/// Zero-allocation, single-cycle decimal octet parser directly computing IPv4 `u32` bits.
///
/// Strictly rejects leading zeros (e.g. `010.0.0.1`) to prevent octal parser differential attacks (CVE-2021-28918).
#[inline]
pub fn parse_ipv4_bytes(bytes: &[u8]) -> Option<u32> {
    if bytes.is_empty() || bytes.len() > 15 {
        return None;
    }
    let mut octets = [0u32; 4];
    let mut octet_idx = 0;
    let mut current_val: u32 = 0;
    let mut digit_count: u8 = 0;

    for &b in bytes {
        if b == b'.' {
            if digit_count == 0 || octet_idx >= 3 || current_val > 255 {
                return None;
            }
            octets[octet_idx] = current_val;
            octet_idx += 1;
            current_val = 0;
            digit_count = 0;
        } else if b.is_ascii_digit() {
            // Reject octal / leading-zero notation (e.g. "01", "00", "010")
            if digit_count > 0 && current_val == 0 {
                return None;
            }
            let digit = (b - b'0') as u32;
            current_val = current_val * 10 + digit;
            if current_val > 255 {
                return None;
            }
            digit_count += 1;
        } else {
            return None;
        }
    }
    if digit_count == 0 || octet_idx != 3 || current_val > 255 {
        return None;
    }
    octets[3] = current_val;
    Some((octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3])
}

/// Extracts an embedded IPv4 address from IPv4-mapped, SIIT-translated, NAT64, or deprecated IPv4-compatible IPv6 addresses.
#[inline]
pub fn extract_embedded_ipv4(v6: Ipv6Addr, raw_str_has_dots: bool) -> Option<Ipv4Addr> {
    let segs = v6.segments();
    // 1. Standard IPv4-mapped IPv6: ::ffff:a.b.c.d (0:0:0:0:0:ffff:hi:lo)
    // 2. SIIT IPv4-translated IPv6: ::ffff:0:a.b.c.d (0:0:0:0:ffff:0:hi:lo)
    // 3. Well-Known NAT64 prefix:   64:ff9b::a.b.c.d (0x0064:0xff9b:0:0:0:0:hi:lo)
    if (segs[0] == 0
        && segs[1] == 0
        && segs[2] == 0
        && segs[3] == 0
        && segs[4] == 0
        && segs[5] == 0xffff)
        || (segs[0] == 0
            && segs[1] == 0
            && segs[2] == 0
            && segs[3] == 0
            && segs[4] == 0xffff
            && segs[5] == 0)
        || (segs[0] == 0x0064
            && segs[1] == 0xff9b
            && segs[2] == 0
            && segs[3] == 0
            && segs[4] == 0
            && segs[5] == 0)
    {
        let bits = ((segs[6] as u32) << 16) | (segs[7] as u32);
        return Some(Ipv4Addr::from_bits(bits));
    }
    // 4. Deprecated RFC 4291 IPv4-compatible syntax (`::a.b.c.d` / `0:0:0:0:0:0:hi:lo`),
    //    excluding standard IPv6 unspecified `::` (0) and loopback `::1` (1) unless written with dots.
    if segs[0] == 0 && segs[1] == 0 && segs[2] == 0 && segs[3] == 0 && segs[4] == 0 && segs[5] == 0
    {
        let bits = ((segs[6] as u32) << 16) | (segs[7] as u32);
        if raw_str_has_dots || bits > 1 {
            return Some(Ipv4Addr::from_bits(bits));
        }
    }
    None
}

#[inline]
fn parse_ipv6_str_canonical(bytes: &[u8]) -> Option<ParsedIp> {
    if bytes.is_empty() || bytes.len() > 45 {
        return None;
    }
    let has_dots = bytes.contains(&b'.');
    // If an IPv6 string uses dotted-quad notation (e.g. `::ffff:010.0.0.1`), validate the trailing IPv4 part
    if has_dots {
        let last_colon = bytes.iter().rposition(|&b| b == b':')?;
        parse_ipv4_bytes(&bytes[last_colon + 1..])?;
    }
    let s = std::str::from_utf8(bytes).ok()?;
    let v6: Ipv6Addr = s.parse().ok()?;
    if let Some(v4) = extract_embedded_ipv4(v6, has_dots) {
        Some(ParsedIp::V4(v4.to_bits()))
    } else {
        Some(ParsedIp::V6(v6.to_bits()))
    }
}

/// Strongly typed, zero-allocation CIDR network prefix matcher for IPv4 and IPv6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpNetwork {
    V4 { net: u32, mask: u32 },
    V6 { net: u128, mask: u128 },
}

impl IpNetwork {
    /// Parses an exact IP address or CIDR notation (e.g. `10.0.0.0/8`, `192.168.1.5`, `2001:db8::/32`, `::ffff:192.168.1.0/120`).
    pub fn from_cidr(s: &str) -> Result<Self, String> {
        let mut s = s.trim();
        if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
            s = s[1..s.len() - 1].trim();
        }
        if s.is_empty() {
            return Err("CIDR expression cannot be empty".to_string());
        }

        if let Some((ip_str, prefix_str)) = s.split_once('/') {
            let ip_str = ip_str.trim();
            let prefix_str = prefix_str.trim();
            if prefix_str.len() > 1 && prefix_str.starts_with('0') {
                return Err(format!(
                    "Invalid CIDR prefix length '{prefix_str}': leading zeros are not permitted"
                ));
            }
            let prefix: u8 = prefix_str
                .parse()
                .map_err(|e| format!("Invalid CIDR prefix length '{prefix_str}': {e}"))?;

            let ip: IpAddr = ip_str
                .parse()
                .map_err(|e| format!("Invalid IP address '{ip_str}': {e}"))?;

            match ip {
                IpAddr::V4(v4) => {
                    if prefix > 32 {
                        return Err(format!("IPv4 prefix cannot exceed 32, got {prefix}"));
                    }
                    let mask = if prefix == 0 {
                        0u32
                    } else {
                        !0u32 << (32 - prefix)
                    };
                    let net = v4.to_bits() & mask;
                    Ok(Self::V4 { net, mask })
                }
                IpAddr::V6(v6) => {
                    if let Some(v4) = extract_embedded_ipv4(v6, ip_str.contains('.')) {
                        if prefix < 96 {
                            return Err(format!(
                                "IPv4-embedded IPv6 prefix must be >= 96, got {prefix}"
                            ));
                        }
                        let v4_prefix = prefix - 96;
                        if v4_prefix > 32 {
                            return Err(format!("IPv4 prefix cannot exceed 32, got {v4_prefix}"));
                        }
                        let mask = if v4_prefix == 0 {
                            0u32
                        } else {
                            !0u32 << (32 - v4_prefix)
                        };
                        let net = v4.to_bits() & mask;
                        Ok(Self::V4 { net, mask })
                    } else {
                        if prefix > 128 {
                            return Err(format!("IPv6 prefix cannot exceed 128, got {prefix}"));
                        }
                        let mask = if prefix == 0 {
                            0u128
                        } else {
                            !0u128 << (128 - prefix)
                        };
                        let net = v6.to_bits() & mask;
                        Ok(Self::V6 { net, mask })
                    }
                }
            }
        } else {
            // Bare IP without slash (/32 or /128)
            let ip: IpAddr = s
                .parse()
                .map_err(|e| format!("Invalid IP address '{s}': {e}"))?;
            match ip {
                IpAddr::V4(v4) => Ok(Self::V4 {
                    net: v4.to_bits(),
                    mask: !0u32,
                }),
                IpAddr::V6(v6) => {
                    if let Some(v4) = extract_embedded_ipv4(v6, s.contains('.')) {
                        Ok(Self::V4 {
                            net: v4.to_bits(),
                            mask: !0u32,
                        })
                    } else {
                        Ok(Self::V6 {
                            net: v6.to_bits(),
                            mask: !0u128,
                        })
                    }
                }
            }
        }
    }

    /// Tests whether an [`IpAddr`] falls within this network prefix.
    #[inline(always)]
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self, ip) {
            (Self::V4 { net, mask }, IpAddr::V4(v4)) => (v4.to_bits() & mask) == *net,
            (Self::V6 { net, mask }, IpAddr::V6(v6)) => {
                if extract_embedded_ipv4(v6, false).is_some() {
                    false
                } else {
                    (v6.to_bits() & mask) == *net
                }
            }
            (Self::V4 { net, mask }, IpAddr::V6(v6)) => {
                if let Some(v4) = extract_embedded_ipv4(v6, false) {
                    (v4.to_bits() & mask) == *net
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Tests whether a [`ParsedIp`] falls within this network prefix.
    #[inline(always)]
    pub fn contains_parsed(&self, ip: ParsedIp) -> bool {
        match (self, ip) {
            (Self::V4 { net, mask }, ParsedIp::V4(bits)) => (bits & mask) == *net,
            (Self::V6 { net, mask }, ParsedIp::V6(bits)) => (bits & mask) == *net,
            _ => false,
        }
    }

    /// Returns the prefix length in bits.
    #[inline]
    pub fn prefix_len(&self) -> u8 {
        match self {
            Self::V4 { mask, .. } => mask.count_ones() as u8,
            Self::V6 { mask, .. } => mask.count_ones() as u8,
        }
    }

    /// Formats the CIDR into its canonical string representation.
    pub fn to_canonical_string(&self) -> String {
        match self {
            Self::V4 { net, mask } => {
                let ip = Ipv4Addr::from_bits(*net);
                let prefix = mask.count_ones();
                format!("{ip}/{prefix}")
            }
            Self::V6 { net, mask } => {
                let ip = Ipv6Addr::from_bits(*net);
                let prefix = mask.count_ones();
                format!("{ip}/{prefix}")
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipv4_cidr_matching() {
        let net = IpNetwork::from_cidr("192.168.1.0/24").unwrap();
        assert!(net.contains("192.168.1.1".parse().unwrap()));
        assert!(net.contains("192.168.1.254".parse().unwrap()));
        assert!(!net.contains("192.168.2.1".parse().unwrap()));
        assert!(!net.contains("10.0.0.1".parse().unwrap()));
    }

    #[test]
    fn test_ipv6_cidr_matching() {
        let net = IpNetwork::from_cidr("2001:db8::/32").unwrap();
        assert!(net.contains("2001:db8:1234::1".parse().unwrap()));
        assert!(net.contains("2001:db8:ffff:ffff::1".parse().unwrap()));
        assert!(!net.contains("2001:db9::1".parse().unwrap()));
        assert!(!net.contains("::1".parse().unwrap()));
    }

    #[test]
    fn test_ipv4_mapped_and_translated_ipv6_matching() {
        let net = IpNetwork::from_cidr("192.168.1.0/24").unwrap();
        assert!(net.contains("::ffff:192.168.1.50".parse().unwrap()));
        assert!(net.contains("::ffff:0:192.168.1.50".parse().unwrap()));
        assert!(net.contains("64:ff9b::192.168.1.50".parse().unwrap()));

        let net_from_mapped = IpNetwork::from_cidr("::ffff:192.168.1.0/120").unwrap();
        assert!(net_from_mapped.contains("192.168.1.50".parse().unwrap()));
        assert_eq!(net_from_mapped, net);
    }

    #[test]
    fn test_parse_ip_bytes_and_adversarial_inputs() {
        assert_eq!(
            parse_ip_bytes(b"192.168.1.1").unwrap(),
            ParsedIp::V4(Ipv4Addr::new(192, 168, 1, 1).to_bits())
        );
        assert_eq!(
            parse_ip_bytes(b"192.168.1.1:8080").unwrap(),
            ParsedIp::V4(Ipv4Addr::new(192, 168, 1, 1).to_bits())
        );
        assert_eq!(
            parse_ip_bytes(b"\"192.168.1.1\"").unwrap(),
            ParsedIp::V4(Ipv4Addr::new(192, 168, 1, 1).to_bits())
        );
        assert_eq!(
            parse_ip_bytes(b"[2001:db8::1]:8080").unwrap(),
            ParsedIp::V6("2001:db8::1".parse::<Ipv6Addr>().unwrap().to_bits())
        );
        assert_eq!(
            parse_ip_bytes(b"::ffff:10.0.0.1").unwrap(),
            ParsedIp::V4(Ipv4Addr::new(10, 0, 0, 1).to_bits())
        );
        assert_eq!(
            parse_ip_bytes(b"64:ff9b::10.0.0.1").unwrap(),
            ParsedIp::V4(Ipv4Addr::new(10, 0, 0, 1).to_bits())
        );
        assert_eq!(
            parse_ip_bytes(b"::10.0.0.1").unwrap(),
            ParsedIp::V4(Ipv4Addr::new(10, 0, 0, 1).to_bits())
        );
        assert_eq!(
            parse_ip_bytes(b"::1").unwrap(),
            ParsedIp::V6(Ipv6Addr::LOCALHOST.to_bits())
        );

        // CVE-2021-28918 Octal / Leading-Zero IPv4 Evasion must be rejected
        assert!(parse_ip_bytes(b"010.0.0.1").is_none());
        assert!(parse_ip_bytes(b"0177.0.0.1").is_none());
        assert!(parse_ip_bytes(b"192.168.001.1").is_none());
        assert!(parse_ip_bytes(b"::ffff:010.0.0.1").is_none());

        // Invalid Port Smuggling must be rejected
        assert!(parse_ip_bytes(b"10.0.0.1:0").is_none());
        assert!(parse_ip_bytes(b"10.0.0.1:65536").is_none());
        assert!(parse_ip_bytes(b"10.0.0.1:080").is_none());
        assert!(parse_ip_bytes(b"10.0.0.1:80@203.0.113.5").is_none());
        assert!(parse_ip_bytes(b"[2001:db8::1]:99999").is_none());
        assert!(parse_ip_bytes(b"[2001:db8::1]garbage").is_none());

        // Control character smuggling (\r, \n, \x0c) must be rejected
        assert!(parse_ip_bytes(b"\x0c10.0.0.1").is_none());
        assert!(parse_ip_bytes(b"\r\n10.0.0.1").is_none());
        assert!(parse_ip_bytes(b"\"\"10.0.0.1\"\"").is_none());
    }

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
}
