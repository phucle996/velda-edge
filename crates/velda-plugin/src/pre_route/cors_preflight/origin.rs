//! CORS Origin Matching Patterns.
//!
//! Supports exact domain, wildcard (`*`), and subdomain wildcard matching (`https://*.example.com`).

/// Origin matching pattern supporting exact domain and wildcard subdomain matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OriginPattern {
    /// Wildcard matching all origins (`*`).
    Any,
    /// Exact match (e.g. `https://app.velda.io`).
    Exact(String),
    /// Subdomain wildcard (e.g. `https://*.velda.io`).
    Subdomain { prefix: String, suffix: String },
}

impl OriginPattern {
    /// Parses an origin string into an [`OriginPattern`].
    pub fn parse(s: &str) -> Self {
        let s = s.trim();
        if s == "*" {
            Self::Any
        } else if let Some((prefix, suffix)) = s.split_once('*') {
            Self::Subdomain {
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            }
        } else {
            Self::Exact(s.to_string())
        }
    }

    /// Evaluates if an incoming `Origin` matches this pattern.
    #[inline]
    pub fn matches(&self, origin: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(expected) => expected.eq_ignore_ascii_case(origin),
            Self::Subdomain { prefix, suffix } => {
                origin.len() >= prefix.len() + suffix.len()
                    && origin.starts_with(prefix)
                    && origin.ends_with(suffix)
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
    }

    #[test]
    fn test_origin_pattern_subdomain() {
        let p = OriginPattern::parse("https://*.velda.io");
        assert!(p.matches("https://sub.velda.io"));
        assert!(p.matches("https://deep.nested.velda.io"));
        assert!(!p.matches("http://sub.velda.io"));
        assert!(!p.matches("https://other.com"));
    }

    #[test]
    fn test_origin_pattern_any() {
        let p = OriginPattern::parse("*");
        assert!(p.matches("https://any-domain.com"));
    }
}
