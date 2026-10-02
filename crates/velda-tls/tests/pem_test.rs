//! Comprehensive positive and negative tests for PEM parsing utilities.

mod common;

use common::make_test_cert;
use velda_tls::TlsError;
use velda_tls::pem::{parse_ca_bundle_pem, parse_certs_pem, parse_private_key_pem};

#[test]
fn test_parse_valid_certs_and_key() {
    let (cert_pem, key_pem) = make_test_cert(vec!["valid.example.com".into()]);

    let certs = parse_certs_pem(&cert_pem).expect("Valid certificate PEM should parse");
    assert_eq!(certs.len(), 1);

    let key = parse_private_key_pem(&key_pem).expect("Valid private key PEM should parse");
    assert!(!key.secret_der().is_empty());

    let ca_store = parse_ca_bundle_pem(&cert_pem).expect("Valid CA bundle PEM should parse");
    assert_eq!(ca_store.len(), 1);
}

#[test]
fn test_parse_empty_or_whitespace_cert_pem_fails() {
    let err_empty = parse_certs_pem("").unwrap_err();
    assert!(
        matches!(err_empty, TlsError::InvalidCertificate(_)),
        "Expected InvalidCertificate, got: {err_empty:?}"
    );

    let err_whitespace = parse_certs_pem("   \n\t  ").unwrap_err();
    assert!(
        matches!(err_whitespace, TlsError::InvalidCertificate(_)),
        "Expected InvalidCertificate, got: {err_whitespace:?}"
    );
}

#[test]
fn test_parse_corrupted_cert_pem_fails() {
    let corrupted =
        "-----BEGIN CERTIFICATE-----\nNOT_VALID_BASE64_GARBAGE!@#$%\n-----END CERTIFICATE-----";
    let err = parse_certs_pem(corrupted).unwrap_err();
    assert!(
        matches!(err, TlsError::InvalidCertificate(_)),
        "Corrupted cert PEM must return InvalidCertificate"
    );
}

#[test]
fn test_parse_empty_or_whitespace_key_pem_fails() {
    let err_empty = parse_private_key_pem("").unwrap_err();
    assert!(
        matches!(err_empty, TlsError::InvalidPrivateKey(_)),
        "Empty key PEM must return InvalidPrivateKey"
    );

    let err_whitespace = parse_private_key_pem("   \n  ").unwrap_err();
    assert!(
        matches!(err_whitespace, TlsError::InvalidPrivateKey(_)),
        "Whitespace key PEM must return InvalidPrivateKey"
    );
}

#[test]
fn test_parse_corrupted_key_pem_fails() {
    let corrupted =
        "-----BEGIN PRIVATE KEY-----\nNOT_VALID_KEY_CONTENT!@#$%\n-----END PRIVATE KEY-----";
    let err = parse_private_key_pem(corrupted).unwrap_err();
    assert!(
        matches!(err, TlsError::InvalidPrivateKey(_)),
        "Corrupted key PEM must return InvalidPrivateKey"
    );
}

#[test]
fn test_parse_cert_as_key_fails() {
    let (cert_pem, _) = make_test_cert(vec!["cert.test".into()]);
    // Passing cert PEM into key parser
    let err = parse_private_key_pem(&cert_pem).unwrap_err();
    assert!(
        matches!(err, TlsError::InvalidPrivateKey(_)),
        "Parsing a certificate as private key must fail"
    );
}

#[test]
fn test_parse_corrupted_ca_bundle_fails() {
    let corrupted =
        "-----BEGIN CERTIFICATE-----\nCORRUPTED_BUNDLE_BYTES\n-----END CERTIFICATE-----";
    let err = parse_ca_bundle_pem(corrupted).unwrap_err();
    assert!(
        matches!(
            err,
            TlsError::InvalidCertificate(_) | TlsError::InvalidCaBundle(_)
        ),
        "Corrupted CA bundle must return appropriate TlsError"
    );
}
