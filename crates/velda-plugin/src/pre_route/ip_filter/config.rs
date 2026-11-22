//! Configuration models and domain representations for `ip_filter`.
//!
//! ```text
//!  IP / CIDR Domain Representation & Canonicalization:
//!
//!  [Raw Input: CIDR / IP String]
//!               │
//!               ▼
//!  +─────────────────────────────────────────────────────────────+
//!  | Canonicalization & Transition Address Translation           |
//!  |                                                             |
//!  |  IPv4 Dotted Decimal ("192.168.1.0/24") ─► V4 { net, mask } |
//!  |                                                             |
//!  |  Dual-Stack IPv4-Mapped (::ffff:10.0.0.1)                   |
//!  |  SIIT IPv4-Translated   (::ffff:0:10.0.0.1) ─► V4 { net }   |
//!  |  NAT64 Well-Known       (64:ff9b::10.0.0.1) (Embedded V4)   |
//!  |  Deprecated Compat      (::10.0.0.1)                        |
//!  |                                                             |
//!  |  Native IPv6            (2001:db8::/32)    ─► V6 { net, mask }
//!  +─────────────────────────────────────────────────────────────+
//!               │
//!               ▼
//!  IpFilterConfig { mode, allow_cidrs, deny_cidrs, client_ip_source }
//! ```

use velda_core::lut::ip::parse_ipv4_bytes;

use super::stream::ParsedIp;

/// Operational filtering mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpFilterMode {
    /// Only permitted IPs can access (Whitelist).
    AllowList,
    /// Specified IPs are blocked (Blacklist).
    DenyList,
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

/// Strongly typed, zero-allocation CIDR network prefix representation for IPv4 and IPv6.
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

    /// Returns true if this rule matches a single specific IP (/32 for v4, /128 for v6).
    #[inline]
    pub fn is_exact_ip(&self) -> bool {
        match self {
            Self::V4 { mask, .. } => *mask == !0u32,
            Self::V6 { mask, .. } => *mask == !0u128,
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

/// Comprehensive configuration for IP and CIDR access control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpFilterConfig {
    pub mode: IpFilterMode,
    pub allow_cidrs: Vec<IpNetwork>,
    pub deny_cidrs: Vec<IpNetwork>,
    pub client_ip_source: ClientIpSource,
}

impl Default for IpFilterConfig {
    fn default() -> Self {
        Self {
            mode: IpFilterMode::DenyList,
            allow_cidrs: Vec::new(),
            deny_cidrs: Vec::new(),
            client_ip_source: ClientIpSource::default(),
        }
    }
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

        assert!(net.contains_parsed(ParsedIp::V4(0xC0A8_0101)));
        assert!(!net.contains_parsed(ParsedIp::V4(0xC0A8_0201)));
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
}
