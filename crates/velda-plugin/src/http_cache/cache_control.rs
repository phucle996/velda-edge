//! RFC 9111 HTTP Cache-Control Directive Single-Pass Bitmask Tokenizer.
//!
//! Zero-allocation, bounds-safe byte slice scanning for HTTP `Cache-Control` headers.
//!
//! ```text
//!   header_bytes.split(|&b| b == b',')
//!     ├─ no `=`  ──► no-store | private | public | no-cache (OR into flags: u8)
//!     └─ has `=` ──► split_once(b'=')
//!                      ├─ "s-maxage" ──► flags |= CC_FLAG_S_MAXAGE, s_maxage_secs = parse_u32
//!                      └─ "max-age"  ──► flags |= CC_FLAG_MAX_AGE,  max_age_secs = parse_u32
//! ```

use std::time::Duration;

/// Single-pass bitmask result for Cache-Control directives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheControlFast {
    pub flags: u8,
    pub max_age_secs: Option<u32>,
    pub s_maxage_secs: Option<u32>,
}

pub const CC_FLAG_NO_STORE: u8 = 1 << 0;
pub const CC_FLAG_PRIVATE: u8 = 1 << 1;
pub const CC_FLAG_PUBLIC: u8 = 1 << 2;
pub const CC_FLAG_S_MAXAGE: u8 = 1 << 3;
pub const CC_FLAG_MAX_AGE: u8 = 1 << 4;
pub const CC_FLAG_NO_CACHE: u8 = 1 << 5;

impl CacheControlFast {
    #[inline(always)]
    pub fn is_no_store_or_private(&self) -> bool {
        (self.flags & (CC_FLAG_NO_STORE | CC_FLAG_PRIVATE)) != 0
    }

    #[inline(always)]
    pub fn is_no_cache_or_no_store(&self) -> bool {
        (self.flags & (CC_FLAG_NO_CACHE | CC_FLAG_NO_STORE)) != 0
    }

    #[inline(always)]
    pub fn is_public_or_s_maxage(&self) -> bool {
        (self.flags & (CC_FLAG_PUBLIC | CC_FLAG_S_MAXAGE)) != 0
    }

    #[inline(always)]
    pub fn effective_ttl_secs(&self) -> Option<u32> {
        self.s_maxage_secs.or(self.max_age_secs)
    }
}

#[inline(always)]
fn trim_ows(mut bytes: &[u8]) -> &[u8] {
    while let Some((&first, rest)) = bytes.split_first() {
        if first == b' ' || first == b'\t' {
            bytes = rest;
        } else {
            break;
        }
    }
    while let Some((&last, rest)) = bytes.split_last() {
        if last == b' ' || last == b'\t' {
            bytes = rest;
        } else {
            break;
        }
    }
    bytes
}

#[inline(always)]
fn trim_quotes_and_ows(mut bytes: &[u8]) -> &[u8] {
    while let Some((&first, rest)) = bytes.split_first() {
        if first == b' ' || first == b'\t' || first == b'"' {
            bytes = rest;
        } else {
            break;
        }
    }
    while let Some((&last, rest)) = bytes.split_last() {
        if last == b' ' || last == b'\t' || last == b'"' {
            bytes = rest;
        } else {
            break;
        }
    }
    bytes
}

#[inline(always)]
fn parse_u32_ascii(bytes: &[u8]) -> Option<u32> {
    let digits = trim_quotes_and_ows(bytes);
    if digits.is_empty() {
        return None;
    }
    let mut acc = 0u32;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add((b - b'0') as u32)?;
    }
    Some(acc)
}

/// Parses a `Cache-Control` header byte slice in a single pass with zero heap allocation or panics.
pub fn parse_cache_control_fast(header_bytes: &[u8]) -> CacheControlFast {
    let mut flags = 0u8;
    let mut max_age_secs = None;
    let mut s_maxage_secs = None;

    for raw_token in header_bytes.split(|&b| b == b',') {
        let token = trim_ows(raw_token);
        if token.is_empty() {
            continue;
        }

        if let Some(eq_pos) = token.iter().position(|&b| b == b'=') {
            let directive = trim_ows(token.get(..eq_pos).unwrap_or(&[]));
            let value = trim_ows(token.get(eq_pos + 1..).unwrap_or(&[]));

            if directive.eq_ignore_ascii_case(b"s-maxage") {
                flags |= CC_FLAG_S_MAXAGE;
                if let Some(secs) = parse_u32_ascii(value) {
                    s_maxage_secs = Some(secs);
                }
            } else if directive.eq_ignore_ascii_case(b"max-age") {
                flags |= CC_FLAG_MAX_AGE;
                if let Some(secs) = parse_u32_ascii(value) {
                    max_age_secs = Some(secs);
                }
            }
        } else if token.eq_ignore_ascii_case(b"no-store") {
            flags |= CC_FLAG_NO_STORE;
        } else if token.eq_ignore_ascii_case(b"private") {
            flags |= CC_FLAG_PRIVATE;
        } else if token.eq_ignore_ascii_case(b"public") {
            flags |= CC_FLAG_PUBLIC;
        } else if token.eq_ignore_ascii_case(b"no-cache") {
            flags |= CC_FLAG_NO_CACHE;
        }
    }

    CacheControlFast {
        flags,
        max_age_secs,
        s_maxage_secs,
    }
}

/// Parses the effective TTL from an RFC 9111 `Cache-Control` header string.
pub fn parse_cache_control_ttl(cache_control: &str) -> Option<Duration> {
    let fast = parse_cache_control_fast(cache_control.as_bytes());
    fast.effective_ttl_secs()
        .map(|s| Duration::from_secs(s as u64))
}
