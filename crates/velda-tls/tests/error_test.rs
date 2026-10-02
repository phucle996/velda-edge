//! Tests for TlsError strongly-typed variants and CoreError conversion.

use velda_core::{Error as CoreError, ErrorKind};
use velda_tls::TlsError;

#[test]
fn test_tls_error_to_core_error_conversion_mapping() {
    // 1. Protocol ErrorKind: NoSniProvided, SniNotFound, Rustls
    let err_no_sni = TlsError::NoSniProvided;
    let core_err: CoreError = err_no_sni.into();
    assert_eq!(core_err.kind(), ErrorKind::Protocol);

    let err_sni = TlsError::SniNotFound("unknown.com".into());
    let core_err: CoreError = err_sni.into();
    assert_eq!(core_err.kind(), ErrorKind::Protocol);

    let err_rustls = TlsError::Rustls(rustls::Error::General("test".into()));
    let core_err: CoreError = err_rustls.into();
    assert_eq!(core_err.kind(), ErrorKind::Protocol);

    // 2. Internal ErrorKind: InvalidCertificate, InvalidPrivateKey, InvalidCaBundle, UpstreamTargetNotFound
    let err_cert = TlsError::InvalidCertificate("bad cert".into());
    let core_err: CoreError = err_cert.into();
    assert_eq!(core_err.kind(), ErrorKind::Internal);

    let err_key = TlsError::InvalidPrivateKey("bad key".into());
    let core_err: CoreError = err_key.into();
    assert_eq!(core_err.kind(), ErrorKind::Internal);

    let err_ca = TlsError::InvalidCaBundle("bad ca".into());
    let core_err: CoreError = err_ca.into();
    assert_eq!(core_err.kind(), ErrorKind::Internal);

    let err_upstream = TlsError::UpstreamTargetNotFound("svc.internal".into());
    let core_err: CoreError = err_upstream.into();
    assert_eq!(core_err.kind(), ErrorKind::Internal);

    let err_proto_ver = TlsError::UnsupportedProtocolVersion("tls1.0".into());
    let core_err: CoreError = err_proto_ver.into();
    assert_eq!(core_err.kind(), ErrorKind::Internal);

    // 3. Connection ErrorKind: HandshakeFailed, Io
    let err_hsk = TlsError::HandshakeFailed("handshake aborted".into());
    let core_err: CoreError = err_hsk.into();
    assert_eq!(core_err.kind(), ErrorKind::Connection);

    let err_io = TlsError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "peer reset connection",
    ));
    let core_err: CoreError = err_io.into();
    assert_eq!(core_err.kind(), ErrorKind::Connection);
}

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
