//! Host and method matching utilities for Layer 7 routers.

/// Extracts the hostname without port or IPv6 enclosing brackets.
///
/// Handles:
/// - `"example.com:8080"` -> `"example.com"`
/// - `"example.com"` -> `"example.com"`
/// - `"[::1]:8080"` -> `"::1"`
/// - `"[::1]"` -> `"::1"`
/// - `"[2001:db8::1]:443"` -> `"2001:db8::1"`
#[inline]
pub fn clean_host(host: &str) -> &str {
    if host.starts_with('[') {
        if let Some(close_bracket) = host.find(']') {
            &host[1..close_bracket]
        } else {
            host
        }
    } else {
        let bytes = host.as_bytes();
        if let Some(first_colon) = bytes.iter().position(|&b| b == b':') {
            // Unbracketed IPv6 has more than 1 colon (e.g. "::1" or "2001:db8::1")
            if bytes[first_colon + 1..].contains(&b':') {
                host
            } else {
                &host[..first_colon]
            }
        } else {
            host
        }
    }
}

/// Matches an incoming cleaned host against an expected pattern.
///
/// Supports exact match (case-insensitive) and wildcard patterns like `*` or `*.example.com`.
#[inline]
pub fn matches_host(expected: Option<&str>, actual: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    let Some(actual) = actual else {
        return false;
    };

    if expected == "*" {
        return true;
    }

    let actual_clean = clean_host(actual);
    let expected_clean = clean_host(expected);

    if let Some(suffix) = expected_clean.strip_prefix("*.") {
        if actual_clean.len() > suffix.len() + 1 {
            let dot_idx = actual_clean.len() - suffix.len() - 1;
            if dot_idx > 0 && actual_clean.as_bytes()[dot_idx] == b'.' {
                return actual_clean[dot_idx + 1..].eq_ignore_ascii_case(suffix);
            }
        }
        false
    } else {
        expected_clean.eq_ignore_ascii_case(actual_clean)
    }
}

/// Returns the specificity rank for a host pattern to enforce deterministic precedence:
/// - 2: Exact host (e.g. `"api.example.com"`)
/// - 1: Suffix wildcard (e.g. `"*.example.com"`)
/// - 0: Catch-all wildcard (`"*"` or `None`)
#[inline]
pub fn host_specificity(host: Option<&str>) -> u8 {
    match host {
        Some("*") | None => 0,
        Some(h) if clean_host(h).starts_with("*.") => 1,
        Some(_) => 2,
    }
}

/// Matches an incoming HTTP method against an expected method (case-insensitive).
#[inline]
pub fn matches_method(expected: Option<&str>, actual: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    let Some(actual) = actual else {
        return false;
    };
    expected.eq_ignore_ascii_case(actual)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_host() {
        assert_eq!(clean_host("example.com"), "example.com");
        assert_eq!(clean_host("example.com:8080"), "example.com");
        assert_eq!(clean_host("[::1]"), "::1");
        assert_eq!(clean_host("[::1]:8080"), "::1");
        assert_eq!(clean_host("[2001:db8::1]:443"), "2001:db8::1");
    }

    #[test]
    fn test_matches_host() {
        assert!(matches_host(None, Some("example.com")));
        assert!(matches_host(Some("*"), Some("example.com:8080")));
        assert!(!matches_host(Some("example.com"), None));
        assert!(matches_host(Some("example.com"), Some("EXAMPLE.COM:8080")));
        assert!(matches_host(Some("*.velda.io"), Some("api.velda.io:443")));
        assert!(matches_host(
            Some("*.velda.io"),
            Some("sub.api.velda.io:443")
        ));
        assert!(!matches_host(Some("*.velda.io"), Some("velda.io:443")));
        assert!(!matches_host(Some("*.velda.io"), Some(".velda.io")));
        assert!(matches_host(Some("[::1]"), Some("[::1]:8080")));
        assert!(matches_host(Some("::1"), Some("[::1]:8080")));

        assert_eq!(host_specificity(Some("api.velda.io")), 2);
        assert_eq!(host_specificity(Some("*.velda.io")), 1);
        assert_eq!(host_specificity(Some("*")), 0);
        assert_eq!(host_specificity(None), 0);
    }

    #[test]
    fn test_matches_method() {
        assert!(matches_method(None, Some("GET")));
        assert!(matches_method(Some("GET"), Some("get")));
        assert!(!matches_method(Some("POST"), Some("GET")));
        assert!(!matches_method(Some("GET"), None));
    }
}
