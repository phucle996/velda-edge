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

use http::header::HeaderName;
use velda_core::L7Request;
pub use velda_core::lut::ip::parse_ipv4_bytes;

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
    /// Converts the parsed bit representation into an RFC 4291 `u128` integer.
    #[inline]
    pub const fn to_u128(self) -> u128 {
        match self {
            Self::V4(bits) => velda_core::lut::ip::encode_ipv4_u128(bits),
            Self::V6(bits) => bits,
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

    /// Extracts the candidate downstream client IP address as an RFC 4291 `u128` integer.
    #[inline]
    pub fn extract_client_ip(&self, req: &L7Request) -> Option<u128> {
        self.extract_parsed_ip(req).ok().map(|p| p.to_u128())
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

/// Extracts an embedded IPv4 `u32` from IPv4-mapped, SIIT-translated, NAT64, or deprecated IPv4-compatible IPv6 `u128` bits.
#[inline]
pub const fn extract_embedded_ipv4(bits: u128, raw_str_has_dots: bool) -> Option<u32> {
    let upper_96 = bits >> 32;
    let lower_32 = bits as u32;
    // 1. Standard IPv4-mapped IPv6: ::ffff:a.b.c.d (0:0:0:0:0:ffff:hi:lo)
    // 2. SIIT IPv4-translated IPv6: ::ffff:0:a.b.c.d (0:0:0:0:ffff:0:hi:lo)
    // 3. Well-Known NAT64 prefix:   64:ff9b::a.b.c.d (0x0064:0xff9b:0:0:0:0:hi:lo)
    if upper_96 == 0x0000_FFFF
        || upper_96 == 0xFFFF_0000
        || upper_96 == 0x0064_FF9B_0000_0000_0000_0000
    {
        return Some(lower_32);
    }
    // 4. Deprecated RFC 4291 IPv4-compatible syntax (`::a.b.c.d` / `0:0:0:0:0:0:hi:lo`),
    //    excluding standard IPv6 unspecified `::` (0) and loopback `::1` (1) unless written with dots.
    if upper_96 == 0 && (raw_str_has_dots || lower_32 > 1) {
        return Some(lower_32);
    }
    None
}

#[inline]
fn parse_ipv6_str_canonical(bytes: &[u8]) -> Option<ParsedIp> {
    if bytes.is_empty() || bytes.len() > 45 {
        return None;
    }
    let has_dots = bytes.contains(&b'.');
    let bits = velda_core::lut::ip::parse_ipv6_u128(bytes)?;
    if let Some(v4) = extract_embedded_ipv4(bits, has_dots) {
        Some(ParsedIp::V4(v4))
    } else {
        Some(ParsedIp::V6(bits))
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

            if let Some(v4_bits) = parse_ipv4_bytes(ip_str.as_bytes()) {
                if prefix > 32 {
                    return Err(format!("IPv4 prefix cannot exceed 32, got {prefix}"));
                }
                let mask = if prefix == 0 {
                    0u32
                } else {
                    !0u32 << (32 - prefix)
                };
                let net = v4_bits & mask;
                Ok(Self::V4 { net, mask })
            } else if let Some(v6_bits) = velda_core::lut::ip::parse_ipv6_u128(ip_str.as_bytes()) {
                if let Some(v4_bits) = extract_embedded_ipv4(v6_bits, ip_str.contains('.')) {
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
                    let net = v4_bits & mask;
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
                    let net = v6_bits & mask;
                    Ok(Self::V6 { net, mask })
                }
            } else {
                Err(format!("Invalid IP address '{ip_str}'"))
            }
        } else if let Some(v4_bits) = parse_ipv4_bytes(s.as_bytes()) {
            Ok(Self::V4 {
                net: v4_bits,
                mask: !0u32,
            })
        } else if let Some(v6_bits) = velda_core::lut::ip::parse_ipv6_u128(s.as_bytes()) {
            if let Some(v4_bits) = extract_embedded_ipv4(v6_bits, s.contains('.')) {
                Ok(Self::V4 {
                    net: v4_bits,
                    mask: !0u32,
                })
            } else {
                Ok(Self::V6 {
                    net: v6_bits,
                    mask: !0u128,
                })
            }
        } else {
            Err(format!("Invalid IP address '{s}'"))
        }
    }

    /// Tests whether an RFC 4291 `u128` IP address falls within this network prefix.
    #[inline(always)]
    pub fn contains(&self, ip: u128) -> bool {
        if let Some(v4_bits) = extract_embedded_ipv4(ip, false) {
            match self {
                Self::V4 { net, mask } => (v4_bits & mask) == *net,
                Self::V6 { .. } => false,
            }
        } else {
            match self {
                Self::V6 { net, mask } => (ip & mask) == *net,
                Self::V4 { .. } => false,
            }
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
                let (buf, len) = velda_core::lut::ip::format_ipv4_bytes(*net);
                let ip = std::str::from_utf8(&buf[..len]).unwrap_or("0.0.0.0");
                let prefix = mask.count_ones();
                format!("{ip}/{prefix}")
            }
            Self::V6 { net, mask } => {
                let (buf, len) = velda_core::lut::ip::format_ipv6_bytes(*net);
                let ip = std::str::from_utf8(&buf[..len]).unwrap_or("::");
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
        assert!(net.contains(velda_core::lut::ip::parse_ip_u128(b"192.168.1.1").unwrap()));
        assert!(net.contains(velda_core::lut::ip::parse_ip_u128(b"192.168.1.254").unwrap()));
        assert!(!net.contains(velda_core::lut::ip::parse_ip_u128(b"192.168.2.1").unwrap()));
        assert!(!net.contains(velda_core::lut::ip::parse_ip_u128(b"10.0.0.1").unwrap()));
    }

    #[test]
    fn test_ipv6_cidr_matching() {
        let net = IpNetwork::from_cidr("2001:db8::/32").unwrap();
        assert!(net.contains(velda_core::lut::ip::parse_ip_u128(b"2001:db8:1234::1").unwrap()));
        assert!(
            net.contains(velda_core::lut::ip::parse_ip_u128(b"2001:db8:ffff:ffff::1").unwrap())
        );
        assert!(!net.contains(velda_core::lut::ip::parse_ip_u128(b"2001:db9::1").unwrap()));
        assert!(!net.contains(1));
    }

    #[test]
    fn test_ipv4_mapped_and_translated_ipv6_matching() {
        let net = IpNetwork::from_cidr("192.168.1.0/24").unwrap();
        assert!(net.contains(velda_core::lut::ip::parse_ip_u128(b"::ffff:192.168.1.50").unwrap()));
        assert!(
            net.contains(velda_core::lut::ip::parse_ip_u128(b"::ffff:0:192.168.1.50").unwrap())
        );
        assert!(
            net.contains(velda_core::lut::ip::parse_ip_u128(b"64:ff9b::192.168.1.50").unwrap())
        );

        let net_from_mapped = IpNetwork::from_cidr("::ffff:192.168.1.0/120").unwrap();
        assert!(
            net_from_mapped.contains(velda_core::lut::ip::parse_ip_u128(b"192.168.1.50").unwrap())
        );
        assert_eq!(net_from_mapped, net);
    }

    #[test]
    fn test_parse_ip_bytes_and_adversarial_inputs() {
        assert_eq!(
            parse_ip_bytes(b"192.168.1.1").unwrap(),
            ParsedIp::V4(0xC0A8_0101)
        );
        assert_eq!(
            parse_ip_bytes(b"192.168.1.1:8080").unwrap(),
            ParsedIp::V4(0xC0A8_0101)
        );
        assert_eq!(
            parse_ip_bytes(b"\"192.168.1.1\"").unwrap(),
            ParsedIp::V4(0xC0A8_0101)
        );
        assert_eq!(
            parse_ip_bytes(b"[2001:db8::1]:8080").unwrap(),
            ParsedIp::V6(0x2001_0DB8_0000_0000_0000_0000_0000_0001)
        );
        assert_eq!(
            parse_ip_bytes(b"::ffff:10.0.0.1").unwrap(),
            ParsedIp::V4(0x0A00_0001)
        );
        assert_eq!(
            parse_ip_bytes(b"64:ff9b::10.0.0.1").unwrap(),
            ParsedIp::V4(0x0A00_0001)
        );
        assert_eq!(
            parse_ip_bytes(b"::10.0.0.1").unwrap(),
            ParsedIp::V4(0x0A00_0001)
        );
        assert_eq!(parse_ip_bytes(b"::1").unwrap(), ParsedIp::V6(1));

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
