//! RFC 6749 Scope Matcher (`oauth2_introspection::scope`).
//!
//! Provides zero-allocation, space-delimited OAuth2 scope validation.
//! Checks if all required scopes are satisfied by the granted scopes in the IdP response.

/// Verifies whether all `required_scopes` exist in the granted `scope_str`.
/// Per RFC 6749 Section 3.3, scopes are space-delimited ASCII strings.
#[inline]
pub fn check_scopes(granted_scope_opt: Option<&str>, required_scopes: &[String]) -> bool {
    if required_scopes.is_empty() {
        return true;
    }

    let granted = match granted_scope_opt {
        Some(s) if !s.trim().is_empty() => s.trim(),
        _ => return false,
    };

    // For small required_scopes (typical 1-4 scopes), scanning space-separated tokens is faster than heap allocation
    for req in required_scopes {
        let req_str = req.trim();
        let mut matched = false;
        for token in granted.split_ascii_whitespace() {
            if token == req_str {
                matched = true;
                break;
            }
        }
        if !matched {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_required_always_passes() {
        assert!(check_scopes(None, &[]));
        assert!(check_scopes(Some(""), &[]));
        assert!(check_scopes(Some("read write"), &[]));
    }

    #[test]
    fn test_missing_granted_fails_when_required() {
        let req = vec!["read".to_string()];
        assert!(!check_scopes(None, &req));
        assert!(!check_scopes(Some(""), &req));
    }

    #[test]
    fn test_matching_scopes() {
        let req = vec!["read".to_string(), "orders:create".to_string()];
        assert!(check_scopes(Some("orders:create read profile write"), &req));
        assert!(!check_scopes(Some("read profile write"), &req));
        assert!(!check_scopes(Some("orders:create write"), &req));
    }

    #[test]
    fn test_partial_token_does_not_match() {
        let req = vec!["read".to_string()];
        assert!(!check_scopes(Some("reader write"), &req));
        assert!(!check_scopes(Some("bread write"), &req));
        assert!(check_scopes(Some("read write"), &req));
    }
}
