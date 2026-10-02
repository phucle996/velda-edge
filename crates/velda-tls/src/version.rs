use rustls::version::{TLS12, TLS13};
use rustls::{DEFAULT_VERSIONS, SupportedProtocolVersion};

use crate::error::TlsError;

/// Resolves user-configured protocol version strings (e.g. "tls1.2", "tls1.3")
/// into rustls [`SupportedProtocolVersion`] pointers.
///
/// If `versions` is empty, returns rustls safe default protocol versions (TLS 1.3 and TLS 1.2).
/// If any version string is unrecognized or unsupported, returns [`TlsError::UnsupportedProtocolVersion`].
pub fn resolve_protocol_versions(
    versions: &[String],
) -> Result<Vec<&'static SupportedProtocolVersion>, TlsError> {
    if versions.is_empty() {
        return Ok(DEFAULT_VERSIONS.to_vec());
    }

    let mut has_tls12 = false;
    let mut has_tls13 = false;

    for v in versions {
        let trimmed = v.trim().to_ascii_lowercase();
        match trimmed.as_str() {
            "tls1.2" | "tls12" | "1.2" => has_tls12 = true,
            "tls1.3" | "tls13" | "1.3" => has_tls13 = true,
            other => return Err(TlsError::UnsupportedProtocolVersion(other.to_string())),
        }
    }

    let mut result = Vec::with_capacity(2);
    // Prioritize TLS 1.3 first in negotiation order
    if has_tls13 {
        result.push(&TLS13);
    }
    if has_tls12 {
        result.push(&TLS12);
    }

    if result.is_empty() {
        return Ok(DEFAULT_VERSIONS.to_vec());
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_empty_versions_defaults_to_safe_defaults() {
        let empty = Vec::new();
        let resolved = resolve_protocol_versions(&empty).expect("default resolution");
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0], &TLS13);
        assert_eq!(resolved[1], &TLS12);
    }

    #[test]
    fn test_resolve_tls13_only() {
        let v = vec!["tls1.3".to_string()];
        let resolved = resolve_protocol_versions(&v).expect("tls1.3");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0], &TLS13);
    }

    #[test]
    fn test_resolve_tls12_only() {
        let v = vec!["tls1.2".to_string()];
        let resolved = resolve_protocol_versions(&v).expect("tls1.2");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0], &TLS12);
    }

    #[test]
    fn test_resolve_both_versions_and_deduplicate() {
        let v = vec![
            "tls1.2".to_string(),
            "TLS1.3".to_string(),
            "tls1.2".to_string(),
        ];
        let resolved = resolve_protocol_versions(&v).expect("both");
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0], &TLS13);
        assert_eq!(resolved[1], &TLS12);
    }

    #[test]
    fn test_resolve_unsupported_version_fails() {
        let v = vec!["tls1.0".to_string()];
        let err = resolve_protocol_versions(&v).unwrap_err();
        match err {
            TlsError::UnsupportedProtocolVersion(v) => assert_eq!(v, "tls1.0"),
            _ => panic!("Expected UnsupportedProtocolVersion, got {:?}", err),
        }
    }
}
