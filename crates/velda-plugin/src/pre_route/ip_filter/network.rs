//! CIDR Bitwise Mask Matching and Client IP Address Extraction.
//!
//! Provides zero-allocation, bitwise IP subnet checking for IPv4 and IPv6:
//! `(ip_bits & mask) == net_bits`.

use std::net::IpAddr;

use velda_core::L7Request;

/// Preferred source for determining the downstream client's IP address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientIpSource {
    /// Inspects the `X-Forwarded-For` header first (first client IP), falling back to `X-Real-IP`.
    #[default]
    XForwardedFor,
    /// Inspects the `X-Real-IP` header first, falling back to `X-Forwarded-For`.
    XRealIp,
}

impl ClientIpSource {
    /// Extracts the candidate downstream client IP address from request headers.
    #[inline]
    pub fn extract_client_ip(&self, req: &L7Request) -> Option<IpAddr> {
        match self {
            Self::XForwardedFor => extract_xff(req).or_else(|| extract_real_ip(req)),
            Self::XRealIp => extract_real_ip(req).or_else(|| extract_xff(req)),
        }
    }
}

#[inline]
fn extract_xff(req: &L7Request) -> Option<IpAddr> {
    req.headers
        .get("x-forwarded-for")
        .and_then(|val| val.to_str().ok())
        .and_then(|raw| raw.split(',').next())
        .and_then(|first| first.trim().parse::<IpAddr>().ok())
}

#[inline]
fn extract_real_ip(req: &L7Request) -> Option<IpAddr> {
    req.headers
        .get("x-real-ip")
        .and_then(|val| val.to_str().ok())
        .and_then(|raw| raw.trim().parse::<IpAddr>().ok())
}

/// Strongly typed, zero-allocation CIDR network prefix matcher for IPv4 and IPv6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpNetwork {
    V4 { net: u32, mask: u32 },
    V6 { net: u128, mask: u128 },
}

impl IpNetwork {
    /// Parses an exact IP address or CIDR notation (e.g. `10.0.0.0/8`, `192.168.1.5`, `2001:db8::/32`).
    pub fn from_cidr(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if let Some((ip_str, prefix_str)) = s.split_once('/') {
            let ip: IpAddr = ip_str
                .trim()
                .parse()
                .map_err(|e| format!("Invalid IP address '{ip_str}': {e}"))?;
            let prefix: u8 = prefix_str
                .trim()
                .parse()
                .map_err(|e| format!("Invalid CIDR prefix '{prefix_str}': {e}"))?;

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
        } else {
            // Single exact IP address
            let ip: IpAddr = s
                .parse()
                .map_err(|e| format!("Invalid IP address '{s}': {e}"))?;
            match ip {
                IpAddr::V4(v4) => Ok(Self::V4 {
                    net: v4.to_bits(),
                    mask: !0u32,
                }),
                IpAddr::V6(v6) => Ok(Self::V6 {
                    net: v6.to_bits(),
                    mask: !0u128,
                }),
            }
        }
    }

    /// Tests whether an [`IpAddr`] falls within this network prefix.
    ///
    /// # Performance Contract
    /// Executes in 1-2 CPU clock cycles using pure bitwise operations:
    /// `(ip_bits & mask) == net_bits`. **Zero heap allocations**.
    #[inline(always)]
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self, ip) {
            (Self::V4 { net, mask }, IpAddr::V4(v4)) => (v4.to_bits() & mask) == *net,
            (Self::V6 { net, mask }, IpAddr::V6(v6)) => (v6.to_bits() & mask) == *net,
            _ => false, // IPv4 network cannot match IPv6 address and vice versa
        }
    }
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
    }
}
