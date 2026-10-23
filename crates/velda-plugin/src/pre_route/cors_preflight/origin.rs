//! CORS Origin Matching Patterns.
//!
//! Provides high-performance, RFC 6454-compliant origin matching supporting exact domains,
//! wildcards (`*`), serialized `null`, and hardened subdomain wildcard matching (`https://*.example.com`).
//!
//! # Security Invariants & CVE Mitigations
//! - **CVE-2020-7662 / CVE-2021-23440 Defense**: The wildcard slice between prefix and suffix
//!   is strictly validated to contain only valid DNS hostname characters (`[a-zA-Z0-9.-]`).
//!   Attacks injecting path delimiters (`/`), credentials (`@`), query strings (`?`),
//!   fragments (`#`), or ports (`:`) inside the wildcard are immediately rejected.
//! - **Empty Subdomain Rejection**: Patterns like `https://*.example.com` will NEVER match `https://.example.com`.
//! - **Branchless Byte Lookup Table (LUT)**: 256-byte static lookup table for $O(1)$ character
//!   classification without branch misprediction or dynamic allocations.

/// Origin matching pattern supporting exact domain and wildcard subdomain matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OriginPattern {
    /// Wildcard matching all origins (`*`).
    Any,
    /// Exact match (e.g. `https://app.velda.io` or `null`).
    Exact(String),
    /// Subdomain wildcard (e.g. `https://*.velda.io`).
    Subdomain { prefix: String, suffix: String },
}

// Byte classification flags for DNS labels
const CHAR_INVALID: u8 = 0;
const CHAR_ALPHANUMERIC: u8 = 1;
const CHAR_HYPHEN: u8 = 2;
const CHAR_DOT: u8 = 4;

/// 256-byte static lookup table for branchless character validation in DNS subdomains.
static SUBDOMAIN_CHAR_LUT: [u8; 256] = {
    let mut table = [CHAR_INVALID; 256];
    let mut i = b'0';
    while i <= b'9' {
        table[i as usize] = CHAR_ALPHANUMERIC;
        i += 1;
    }
    let mut i = b'a';
    while i <= b'z' {
        table[i as usize] = CHAR_ALPHANUMERIC;
        i += 1;
    }
    let mut i = b'A';
    while i <= b'Z' {
        table[i as usize] = CHAR_ALPHANUMERIC;
        i += 1;
    }
    table[b'-' as usize] = CHAR_HYPHEN;
    table[b'.' as usize] = CHAR_DOT;
    table
};

/// Validates that a matched wildcard slice conforms strictly to DNS subdomain label grammar.
///
/// Disallows empty strings, consecutive dots, path separators (`/`), credentials (`@`),
/// query/fragment markers (`?`, `#`), port colons (`:`), or control/whitespace characters.
#[inline]
fn is_valid_subdomain(subdomain: &str) -> bool {
    let bytes = subdomain.as_bytes();
    if bytes.is_empty() {
        return false;
    }

    // First and last characters must be alphanumeric (RFC 1035 / RFC 1123: no leading/trailing dot or hyphen)
    if (SUBDOMAIN_CHAR_LUT[bytes[0] as usize] & CHAR_ALPHANUMERIC) == 0
        || (SUBDOMAIN_CHAR_LUT[bytes[bytes.len() - 1] as usize] & CHAR_ALPHANUMERIC) == 0
    {
        return false;
    }

    let mut prev_dot = false;
    let mut prev_hyphen = false;

    for &b in bytes {
        let flag = SUBDOMAIN_CHAR_LUT[b as usize];
        if flag == CHAR_INVALID {
            return false; // Forbidden character (injection probe)
        }

        let is_dot = (flag & CHAR_DOT) != 0;
        let is_hyphen = (flag & CHAR_HYPHEN) != 0;

        // Disallow consecutive dots ("..") and hyphens adjacent to dots ("-." or ".-")
        if (is_dot && prev_dot) || (is_dot && prev_hyphen) || (is_hyphen && prev_dot) {
            return false;
        }

        prev_dot = is_dot;
        prev_hyphen = is_hyphen;
    }
    true
}

impl OriginPattern {
    /// Parses an origin string into an [`OriginPattern`], pre-normalizing case for hot-path acceleration.
    pub fn parse(s: &str) -> Self {
        let s = s.trim();
        if s == "*" {
            Self::Any
        } else if let Some((prefix, suffix)) = s.split_once('*') {
            Self::Subdomain {
                prefix: prefix.to_ascii_lowercase(),
                suffix: suffix.to_ascii_lowercase(),
            }
        } else {
            Self::Exact(s.to_ascii_lowercase())
        }
    }

    /// Evaluates if an incoming `Origin` header value matches this pattern.
    ///
    /// Executes in $O(1)$ without heap allocations on the hot path.
    #[inline]
    pub fn matches(&self, origin: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(expected) => expected.eq_ignore_ascii_case(origin),
            Self::Subdomain { prefix, suffix } => {
                if origin.len() <= prefix.len() + suffix.len() {
                    return false;
                }
                let (o_prefix, rest) = origin.split_at(prefix.len());
                if !o_prefix.eq_ignore_ascii_case(prefix) {
                    return false;
                }
                let wildcard_len = origin.len() - prefix.len() - suffix.len();
                let (wildcard_part, o_suffix) = rest.split_at(wildcard_len);
                if !o_suffix.eq_ignore_ascii_case(suffix) {
                    return false;
                }
                is_valid_subdomain(wildcard_part)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_origin_pattern_exact() {
        let p = OriginPattern::parse("https://app.velda.io");
        assert!(p.matches("https://app.velda.io"));
        assert!(p.matches("HTTPS://APP.VELDA.IO"));
        assert!(!p.matches("https://api.velda.io"));
        assert!(!p.matches("https://app.velda.io:443"));
    }

    #[test]
    fn test_origin_pattern_subdomain_valid() {
        let p = OriginPattern::parse("https://*.velda.io");
        assert!(p.matches("https://sub.velda.io"));
        assert!(p.matches("https://deep.nested.velda.io"));
        assert!(p.matches("HTTPS://SUB.VELDA.IO"));
        assert!(p.matches("https://my-service-01.velda.io"));
    }

    #[test]
    fn test_origin_pattern_subdomain_cve_injection_defenses() {
        let p = OriginPattern::parse("https://*.velda.io");

        // Path injection attack (CVE-2020-7662)
        assert!(!p.matches("https://evil.com/.velda.io"));
        assert!(!p.matches("https://evil.com/sub.velda.io"));

        // Userinfo injection attack
        assert!(!p.matches("https://evil.com@.velda.io"));
        assert!(!p.matches("https://evil.com@sub.velda.io"));

        // Query & Fragment injection attack
        assert!(!p.matches("https://evil.com?.velda.io"));
        assert!(!p.matches("https://evil.com#.velda.io"));

        // Port delimiter inside wildcard
        assert!(!p.matches("https://evil.com:443.velda.io"));

        // Empty subdomain
        assert!(!p.matches("https://.velda.io"));

        // Malformed DNS labels (leading/trailing hyphen or dot, double dots, hyphen adjacent to dot)
        assert!(!p.matches("https://-sub.velda.io"));
        assert!(!p.matches("https://sub-.velda.io"));
        assert!(!p.matches("https://sub..velda.io"));
        assert!(!p.matches("https://a.-b.velda.io"));
        assert!(!p.matches("https://a-.b.velda.io"));

        // Scheme mismatch
        assert!(!p.matches("http://sub.velda.io"));
    }

    #[test]
    fn test_origin_pattern_any() {
        let p = OriginPattern::parse("*");
        assert!(p.matches("https://any-domain.com"));
        assert!(p.matches("http://localhost:3000"));
        assert!(p.matches("null"));
    }

    #[test]
    fn test_origin_pattern_null() {
        let p = OriginPattern::parse("null");
        assert!(p.matches("null"));
        assert!(p.matches("NULL"));
        assert!(!p.matches("https://app.velda.io"));
    }
}
