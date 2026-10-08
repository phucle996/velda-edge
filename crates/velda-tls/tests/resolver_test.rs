//! Unit and integration tests for SniResolver and RFC 6125 wildcard matching.

mod common;

use common::make_test_cert;
use rustls::pki_types::PrivateKeyDer;
use velda_tls::SniResolver;
use velda_tls::pem::{parse_certs_pem, parse_private_key_pem};

#[test]
fn test_exact_and_wildcard_sni_resolution() {
    let mut resolver = SniResolver::new();

    // 1. Add exact cert
    let (exact_cert, exact_key) = make_test_cert(vec!["api.example.com".into()]);
    let certs = parse_certs_pem(&exact_cert).unwrap();
    let key = parse_private_key_pem(&exact_key).unwrap();
    resolver
        .add_certificate(&["api.example.com".into()], certs, key)
        .unwrap();

    // 2. Add wildcard cert
    let (wild_cert, wild_key) = make_test_cert(vec!["*.internal.net".into()]);
    let certs = parse_certs_pem(&wild_cert).unwrap();
    let key = parse_private_key_pem(&wild_key).unwrap();
    resolver
        .add_certificate(&["*.internal.net".into()], certs, key)
        .unwrap();

    assert_eq!(resolver.exact_len(), 1);
    assert_eq!(resolver.wildcard_len(), 1);

    // Exact match & case-insensitivity
    assert!(resolver.lookup("api.example.com").is_some());
    assert!(resolver.lookup("API.EXAMPLE.COM").is_some());
    assert!(resolver.lookup("aPi.ExAmPlE.cOm").is_some());

    // Wildcard match (single label)
    assert!(resolver.lookup("svc1.internal.net").is_some());
    assert!(resolver.lookup("auth.internal.net").is_some());
    assert!(resolver.lookup("AUTH.INTERNAL.NET").is_some());

    // RFC 6125: Wildcard does NOT match multi-level / nested subdomains
    assert!(
        resolver.lookup("deep.sub.internal.net").is_none(),
        "Wildcard *.internal.net must not match multi-level deep.sub.internal.net"
    );

    // Unknown SNI (Strict rejection)
    assert!(resolver.lookup("unknown.com").is_none());
    assert!(resolver.lookup("other.example.com").is_none());
    assert!(resolver.lookup("").is_none());
    assert!(resolver.lookup("   ").is_none());
}

#[test]
fn test_sni_resolver_whitespace_trimming() {
    let mut resolver = SniResolver::new();
    let (cert, key) = make_test_cert(vec!["trimmed.org".into()]);
    let certs = parse_certs_pem(&cert).unwrap();
    let key = parse_private_key_pem(&key).unwrap();

    resolver
        .add_certificate(&["  trimmed.org  ".into()], certs, key)
        .unwrap();

    assert!(resolver.lookup("trimmed.org").is_some());
    assert!(resolver.lookup("  trimmed.org  ").is_some());
}

#[test]
fn test_sni_resolver_invalid_key_fails() {
    let mut resolver = SniResolver::new();
    let (cert, _) = make_test_cert(vec!["test.org".into()]);
    let certs = parse_certs_pem(&cert).unwrap();

    // Invalid/corrupted DER bytes for private key
    let corrupted_key = PrivateKeyDer::Pkcs8(vec![0x00, 0x01, 0x02].into());
    let err = resolver.add_certificate(&["test.org".into()], certs, corrupted_key);

    assert!(
        err.is_err(),
        "Adding invalid private key must return an error"
    );
}

#[test]
fn test_sni_config_resolver_exact_and_wildcard() {
    use velda_tls::SniConfigResolver;

    let (cert_pem, key_pem) = make_test_cert(vec!["default.com".into()]);
    let default_cfg = velda_tls::ServerTlsConfig {
        sni: vec!["default.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["http/1.1".into()],
        cert_pem: cert_pem.clone(),
        key_pem: key_pem.clone(),
        client_ca_pem: None,
    }
    .build_single(&velda_tls::TlsServerParams::from_hardware())
    .unwrap();

    let h2_cfg = velda_tls::ServerTlsConfig {
        sni: vec!["api.service.net".into(), "*.h2.service.net".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem,
        key_pem,
        client_ca_pem: None,
    }
    .build_single(&velda_tls::TlsServerParams::from_hardware())
    .unwrap();

    let mut resolver = SniConfigResolver::new(default_cfg.clone());
    resolver.add_config(
        &["api.service.net".into(), "*.h2.service.net".into()],
        h2_cfg.clone(),
    );

    assert_eq!(resolver.exact_len(), 1);
    assert_eq!(resolver.wildcard_len(), 1);

    // Exact resolution
    let resolved_api = resolver.resolve(Some("api.service.net"));
    assert_eq!(resolved_api.alpn_protocols, vec![b"h2".to_vec()]);

    let resolved_api_upper = resolver.resolve(Some("API.SERVICE.NET"));
    assert_eq!(resolved_api_upper.alpn_protocols, vec![b"h2".to_vec()]);

    // Wildcard resolution
    let resolved_sub = resolver.resolve(Some("node1.h2.service.net"));
    assert_eq!(resolved_sub.alpn_protocols, vec![b"h2".to_vec()]);

    // Nested sub-subdomain must not match wildcard per RFC 6125 -> fallback to default
    let resolved_nested = resolver.resolve(Some("deep.sub.h2.service.net"));
    assert_eq!(resolved_nested.alpn_protocols, vec![b"http/1.1".to_vec()]);

    // None or unknown SNI -> fallback to default
    let resolved_none = resolver.resolve(None);
    assert_eq!(resolved_none.alpn_protocols, vec![b"http/1.1".to_vec()]);

    let resolved_unknown = resolver.resolve(Some("unknown.domain.com"));
    assert_eq!(resolved_unknown.alpn_protocols, vec![b"http/1.1".to_vec()]);
}
