//! Self-Contained CORS Origin Matcher & Token Validator (`post_response::cors::origin`).
//!
//! ```text
//!   Incoming `Origin` bytes (`&[u8]`)
//!     │
//!     ├── CorsOriginPattern::Any ──────────────────────────────────► true
//!     ├── CorsOriginPattern::Exact(expected) ──────────────────────► eq_ignore_ascii_case
//!     └── CorsOriginPattern::Subdomain { prefix, suffix }
//!           ├─ prefix match & suffix match on &[u8] (no UTF-8 split panic)
//!           └─ 256-byte `SUBDOMAIN_CHAR_LUT`: rejects '/', '@', '?', '#', ':', '..', '.-', '-.'
//! ```
//!
//! Completely isolated inside `post_response::cors` with zero cross-phase coupling.

use crate::error::PluginError;

/// Compiled CORS origin matching pattern (`*`, exact origin / `null`, or `https://*.domain`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorsOriginPattern {
    /// Wildcard matching all origins (`*`).
    Any,
    /// Exact origin match normalized to ASCII lowercase (e.g. `https://app.velda.io` or `null`).
    Exact(Vec<u8>),
    /// Subdomain wildcard (e.g. `https://*.velda.io`) split around `*`.
    Subdomain { prefix: Vec<u8>, suffix: Vec<u8> },
}

const CHAR_INVALID: u8 = 0;
const CHAR_ALPHANUMERIC: u8 = 1;
const CHAR_HYPHEN: u8 = 2;
const CHAR_DOT: u8 = 4;

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

/// Validates DNS label grammar strictly on byte slices (`[a-zA-Z0-9.-]`).
#[inline]
fn is_valid_subdomain_bytes(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }
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
            return false;
        }
        let is_dot = (flag & CHAR_DOT) != 0;
        let is_hyphen = (flag & CHAR_HYPHEN) != 0;
        if (is_dot && prev_dot) || (is_dot && prev_hyphen) || (is_hyphen && prev_dot) {
            return false;
        }
        prev_dot = is_dot;
        prev_hyphen = is_hyphen;
    }
    true
}

impl CorsOriginPattern {
    /// Parses a trimmed origin string into a byte-oriented [`CorsOriginPattern`].
    pub fn parse(s: &str) -> Self {
        let trimmed = s.trim();
        if trimmed == "*" {
            Self::Any
        } else if let Some((prefix, suffix)) = trimmed.split_once('*') {
            Self::Subdomain {
                prefix: prefix.as_bytes().to_ascii_lowercase(),
                suffix: suffix.as_bytes().to_ascii_lowercase(),
            }
        } else {
            Self::Exact(trimmed.as_bytes().to_ascii_lowercase())
        }
    }

    /// Returns `true` if this pattern represents the permissive `*` wildcard or `null` literal.
    #[inline(always)]
    pub fn is_wildcard_or_null(&self) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(bytes) => bytes.eq_ignore_ascii_case(b"null"),
            Self::Subdomain { .. } => false,
        }
    }

    /// Evaluates an incoming `Origin` header byte slice in O(1) without heap allocation.
    #[inline]
    pub fn matches_bytes(&self, origin: &[u8]) -> bool {
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
                is_valid_subdomain_bytes(wildcard_part)
            }
        }
    }
}

/// Validates RFC 6454 origin pattern syntax and Fetch credential invariants.
pub fn validate_origin_pattern(
    pattern: &str,
    allow_credentials: bool,
    plugin_id: &str,
) -> Result<(), PluginError> {
    let trimmed = pattern.trim();
    if trimmed.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': origin pattern cannot be empty"
        )));
    }

    if trimmed == "*" {
        if allow_credentials {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': allow_credentials cannot be true when allow_origins contains '*' (RFC 6454 / Fetch Spec violation)"
            )));
        }
        return Ok(());
    }

    if trimmed.eq_ignore_ascii_case("null") {
        if allow_credentials {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': allow_credentials cannot be true when allow_origins contains 'null' (security risk: credential exposure to sandboxed contexts)"
            )));
        }
        return Ok(());
    }

    let after_scheme = if let Some(rest) = trimmed.strip_prefix("https://") {
        rest
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        rest
    } else {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': origin '{trimmed}' must start with 'http://' or 'https://'"
        )));
    };

    if after_scheme.is_empty() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': origin '{trimmed}' has empty authority"
        )));
    }

    if after_scheme.contains('/')
        || after_scheme.contains('@')
        || after_scheme.contains('?')
        || after_scheme.contains('#')
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': origin '{trimmed}' must not contain path, userinfo, query, or fragment"
        )));
    }

    if after_scheme.contains('*') {
        if !after_scheme.starts_with("*.") {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': wildcard origin '{trimmed}' must use '*.domain' syntax immediately after scheme (e.g. 'https://*.example.com')"
            )));
        }
        let host_rest = &after_scheme[2..];
        if host_rest.is_empty() || host_rest.contains('*') || host_rest.starts_with('.') {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': invalid wildcard origin pattern '{trimmed}'"
            )));
        }
    }

    Ok(())
}

/// Validates RFC 9110 header field-name grammar (`1*tchar` or `*`).
pub fn is_valid_header_name(h: &str) -> bool {
    let trimmed = h.trim();
    if trimmed == "*" {
        return true;
    }
    if trimmed.is_empty() {
        return false;
    }
    trimmed.bytes().all(|b| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subdomain_injection_defenses() {
        let p = CorsOriginPattern::parse("https://*.velda.io");
        assert!(p.matches_bytes(b"https://sub.velda.io"));
        assert!(p.matches_bytes(b"HTTPS://SUB.VELDA.IO"));
        assert!(!p.matches_bytes(b"https://evil.com/.velda.io"));
        assert!(!p.matches_bytes(b"https://evil.com@sub.velda.io"));
        assert!(!p.matches_bytes(b"https://evil.com:443.velda.io"));
        assert!(!p.matches_bytes(b"https://.velda.io"));
        assert!(!p.matches_bytes(b"https://sub..velda.io"));
        // Multi-byte UTF-8 slice safety
        assert!(!p.matches_bytes("https://🔥.velda.io".as_bytes()));
    }
}
