//! Tests for TlsError strongly-typed variants and formatting.

use velda_tls::TlsError;

#[test]
fn test_tls_error_display_formatting() {
    let err = TlsError::SniNotFound("missing.domain.com".into());
    assert!(err.to_string().contains("missing.domain.com"));

    let err_no_sni = TlsError::NoSniProvided;
    assert!(err_no_sni.to_string().contains("No SNI was provided"));

    let err_upstream = TlsError::UpstreamTargetNotFound("target.local".into());
    assert!(err_upstream.to_string().contains("target.local"));

    let err_proto = TlsError::UnsupportedProtocolVersion("tls1.1".into());
    assert!(err_proto.to_string().contains("tls1.1"));
}
