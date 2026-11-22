//! Streaming Zero-Allocation Client IP Parser & Evaluator.
//!
//! Enforces zero-trust multi-hop evaluation directly over header byte buffers on CPU registers,
//! eliminating intermediate stack allocations and `memcpy` overhead.
//!
//! ```text
//!  [L7Request Headers: X-Forwarded-For / X-Real-IP]
//!                      │
//!                      ▼
//!   +───────────────────────────────────────────────────────────+
//!   | Stream & Evaluate Directly on CPU Registers (Zero-Copy)   |
//!   |                                                           |
//!   |  for token in header_bytes.split(b',') {                  |
//!   |     parse_ip_bytes(token) ──► ParsedIp (u32/u128 on reg)  |
//!   |                                      │                    |
//!   |            ┌─────────────────────────┴────────────────┐   |
//!   |            ▼                                          ▼   |
//!   |       [AllowList]                                [DenyList]|
//!   |  deny.matches_ip? ─► REJECT 403           allow.matches_ip|
//!   |  unallowed |= !allow.matches_ip           ├─► Excused     |
//!   |                                           └─► deny.match? |
//!   |                                                └─► REJECT |
//!   |  }                                                        |
//!   |                                                           |
//!   |  - Zero stack buffer [ParsedIp; 16] (Saved 512B on stack) |
//!   |  - Zero memcpy calls                                      |
//!   |  - Anti-evasion: RFC 9110 OWS, octal reject, port check   |
//!   +───────────────────────────────────────────────────────────+
//!                      │
//!                      ▼
//!         Action::Continue / Action::Reject
//! ```

use http::header::HeaderName;
use velda_core::{Action, L7Request, RejectReason};

use super::config::{ClientIpSource, IpFilterMode, extract_embedded_ipv4};
use super::engine::IpFilterEngine;
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
    #[inline(always)]
    pub const fn to_u128(self) -> u128 {
        match self {
            Self::V4(bits) => velda_core::lut::ip::encode_ipv4_u128(bits),
            Self::V6(bits) => bits,
        }
    }
}

/// Internal status of header extraction during streaming evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamStatus {
    Continue,
    Reject,
    HeaderMissing,
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

#[inline]
fn eval_real_ip(
    req: &L7Request,
    mode: IpFilterMode,
    allow: &IpFilterEngine,
    deny: &IpFilterEngine,
) -> StreamStatus {
    let mut iter = req.headers.get_all(&X_REAL_IP).iter();
    let Some(val) = iter.next() else {
        return StreamStatus::HeaderMissing;
    };
    // Singleton header invariant: multiple X-Real-IP headers are rejected as header smuggling
    if iter.next().is_some() {
        return StreamStatus::Reject;
    }
    let bytes = val.as_bytes();
    // X-Real-IP must not contain comma-separated lists
    if bytes.is_empty() || bytes.contains(&b',') {
        return StreamStatus::Reject;
    }
    let Some(hop_ip) = parse_ip_bytes(bytes) else {
        return StreamStatus::Reject;
    };

    match mode {
        IpFilterMode::AllowList => {
            if !deny.is_empty() && deny.matches_ip(hop_ip) {
                return StreamStatus::Reject;
            }
            if !allow.matches_ip(hop_ip) {
                return StreamStatus::Reject;
            }
            StreamStatus::Continue
        }
        IpFilterMode::DenyList => {
            let excused = !allow.is_empty() && allow.matches_ip(hop_ip);
            if !excused && deny.matches_ip(hop_ip) {
                return StreamStatus::Reject;
            }
            StreamStatus::Continue
        }
    }
}

#[inline]
fn eval_xff(
    req: &L7Request,
    mode: IpFilterMode,
    allow: &IpFilterEngine,
    deny: &IpFilterEngine,
) -> StreamStatus {
    let mut iter = req.headers.get_all(&X_FORWARDED_FOR).iter();
    let Some(first_hdr) = iter.next() else {
        return StreamStatus::HeaderMissing;
    };

    let mut hop_count = 0usize;
    let mut hdr_count = 0usize;
    let mut unallowed = false;
    let has_deny = !deny.is_empty();
    let has_allow = !allow.is_empty();

    let mut current_hdr = Some(first_hdr);
    while let Some(val) = current_hdr {
        hdr_count += 1;
        if hdr_count > MAX_XFF_HEADERS {
            return StreamStatus::Reject;
        }
        let bytes = val.as_bytes();
        if bytes.is_empty() {
            return StreamStatus::Reject;
        }
        for token in bytes.split(|&b| b == b',') {
            if hop_count >= MAX_XFF_HOPS {
                return StreamStatus::Reject;
            }
            let Some(hop_ip) = parse_ip_bytes(token) else {
                return StreamStatus::Reject;
            };
            hop_count += 1;

            match mode {
                IpFilterMode::AllowList => {
                    if has_deny && deny.matches_ip(hop_ip) {
                        return StreamStatus::Reject;
                    }
                    unallowed |= !allow.matches_ip(hop_ip);
                }
                IpFilterMode::DenyList => {
                    let excused = has_allow && allow.matches_ip(hop_ip);
                    if !excused && deny.matches_ip(hop_ip) {
                        return StreamStatus::Reject;
                    }
                }
            }
        }
        current_hdr = iter.next();
    }

    if hop_count == 0 {
        return StreamStatus::Reject;
    }

    if mode == IpFilterMode::AllowList && unallowed {
        return StreamStatus::Reject;
    }

    StreamStatus::Continue
}

/// Evaluates downstream request client IP chain against configured CIDR rules on the fly.
///
/// Executes with zero heap allocations and zero stack buffers by streaming byte tokens
/// directly into CPU registers.
#[inline]
pub fn evaluate_stream(
    req: &L7Request,
    source: ClientIpSource,
    mode: IpFilterMode,
    allow: &IpFilterEngine,
    deny: &IpFilterEngine,
) -> Action {
    let status = match source {
        ClientIpSource::XForwardedFor => match eval_xff(req, mode, allow, deny) {
            StreamStatus::HeaderMissing => eval_real_ip(req, mode, allow, deny),
            other => other,
        },
        ClientIpSource::XRealIp => match eval_real_ip(req, mode, allow, deny) {
            StreamStatus::HeaderMissing => eval_xff(req, mode, allow, deny),
            other => other,
        },
    };

    match status {
        StreamStatus::Continue => Action::Continue,
        StreamStatus::Reject => Action::Reject(RejectReason::IpBlocked),
        StreamStatus::HeaderMissing => match mode {
            IpFilterMode::AllowList => Action::Reject(RejectReason::IpBlocked),
            IpFilterMode::DenyList => Action::Continue,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
