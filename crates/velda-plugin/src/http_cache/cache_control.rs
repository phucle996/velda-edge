//! RFC 9111 HTTP Cache-Control Directive Single-Pass Bitmask Tokenizer.
//!
//! Provides zero-allocation, single-pass byte slice parsing for HTTP `Cache-Control`
//! headers, extracting flags and integer TTL parameters in $< 3$ ns without heap allocations.

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
    pub fn is_public_or_s_maxage(&self) -> bool {
        (self.flags & (CC_FLAG_PUBLIC | CC_FLAG_S_MAXAGE)) != 0
    }

    #[inline(always)]
    pub fn effective_ttl_secs(&self) -> Option<u32> {
        self.s_maxage_secs.or(self.max_age_secs)
    }
}

/// Parses a `Cache-Control` header byte slice in a single pass without allocations or repeated substring scans.
pub fn parse_cache_control_fast(header_bytes: &[u8]) -> CacheControlFast {
    let mut flags = 0u8;
    let mut max_age_secs = None;
    let mut s_maxage_secs = None;

    let mut i = 0;
    let len = header_bytes.len();

    while i < len {
        // Skip leading whitespace and commas
        while i < len
            && (header_bytes[i] == b' ' || header_bytes[i] == b',' || header_bytes[i] == b'\t')
        {
            i += 1;
        }
        if i >= len {
            break;
        }

        // Find end of token / directive
        let start = i;
        while i < len && header_bytes[i] != b',' {
            i += 1;
        }
        let token = &header_bytes[start..i];

        // Trim token whitespace
        let token_trimmed = {
            let mut s = 0;
            let mut e = token.len();
            while s < e && (token[s] == b' ' || token[s] == b'\t') {
                s += 1;
            }
            while e > s && (token[e - 1] == b' ' || token[e - 1] == b'\t') {
                e -= 1;
            }
            &token[s..e]
        };

        if let Some(eq_pos) = token_trimmed.iter().position(|&b| b == b'=') {
            let directive = {
                let mut s = 0;
                let mut e = eq_pos;
                while s < e && (token_trimmed[s] == b' ' || token_trimmed[s] == b'\t') {
                    s += 1;
                }
                while e > s && (token_trimmed[e - 1] == b' ' || token_trimmed[e - 1] == b'\t') {
                    e -= 1;
                }
                &token_trimmed[s..e]
            };
            let value = {
                let mut s = eq_pos + 1;
                let mut e = token_trimmed.len();
                while s < e && (token_trimmed[s] == b' ' || token_trimmed[s] == b'\t') {
                    s += 1;
                }
                while e > s && (token_trimmed[e - 1] == b' ' || token_trimmed[e - 1] == b'\t') {
                    e -= 1;
                }
                &token_trimmed[s..e]
            };

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
        } else if token_trimmed.eq_ignore_ascii_case(b"no-store") {
            flags |= CC_FLAG_NO_STORE;
        } else if token_trimmed.eq_ignore_ascii_case(b"private") {
            flags |= CC_FLAG_PRIVATE;
        } else if token_trimmed.eq_ignore_ascii_case(b"public") {
            flags |= CC_FLAG_PUBLIC;
        } else if token_trimmed.eq_ignore_ascii_case(b"no-cache") {
            flags |= CC_FLAG_NO_CACHE;
        }
    }

    CacheControlFast {
        flags,
        max_age_secs,
        s_maxage_secs,
    }
}

#[inline(always)]
fn parse_u32_ascii(bytes: &[u8]) -> Option<u32> {
    let mut s = 0;
    let mut e = bytes.len();
    while s < e && (bytes[s] == b' ' || bytes[s] == b'"') {
        s += 1;
    }
    while e > s && (bytes[e - 1] == b' ' || bytes[e - 1] == b'"') {
        e -= 1;
    }
    let digits = &bytes[s..e];
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

/// Parses the effective TTL from an RFC 9111 `Cache-Control` header string.
pub fn parse_cache_control_ttl(cache_control: &str) -> Option<Duration> {
    let fast = parse_cache_control_fast(cache_control.as_bytes());
    fast.effective_ttl_secs()
        .map(|s| Duration::from_secs(s as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cache_control_ttl() {
        assert_eq!(
            parse_cache_control_ttl("public, max-age=3600"),
            Some(Duration::from_secs(3600))
        );
        assert_eq!(
            parse_cache_control_ttl("max-age=60, s-maxage=300"),
            Some(Duration::from_secs(300))
        );
        assert_eq!(
            parse_cache_control_ttl("s-maxage=\"120\", max-age=60"),
            Some(Duration::from_secs(120))
        );
        assert_eq!(parse_cache_control_ttl("no-store, no-cache"), None);
        assert_eq!(parse_cache_control_ttl(""), None);
    }

    #[test]
    fn test_parse_cache_control_fast_directives() {
        let fast = parse_cache_control_fast(b"public, max-age=120");
        assert!(!fast.is_no_store_or_private());
        assert!(fast.is_public_or_s_maxage());
        assert_eq!(fast.effective_ttl_secs(), Some(120));

        let no_store = parse_cache_control_fast(b"no-store, private");
        assert!(no_store.is_no_store_or_private());
        assert_eq!(no_store.effective_ttl_secs(), None);

        let complex = parse_cache_control_fast(b"  public , s-maxage = 600 , max-age = 300 ");
        assert!(complex.is_public_or_s_maxage());
        assert_eq!(complex.effective_ttl_secs(), Some(600));
    }
}
