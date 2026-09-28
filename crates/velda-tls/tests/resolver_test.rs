use rcgen::generate_simple_self_signed;
use velda_tls::SniResolver;
use velda_tls::pem::{parse_certs_pem, parse_private_key_pem};

fn make_test_cert(sans: Vec<String>) -> (String, String) {
    let certified_key = generate_simple_self_signed(sans).unwrap();
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.signing_key.serialize_pem();
    (cert_pem, key_pem)
}

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

    // Exact match
    assert!(resolver.lookup("api.example.com").is_some());
    assert!(resolver.lookup("API.EXAMPLE.COM").is_some());

    // Wildcard match
    assert!(resolver.lookup("svc1.internal.net").is_some());
    assert!(resolver.lookup("auth.internal.net").is_some());

    // Unknown SNI (Strict rejection)
    assert!(resolver.lookup("unknown.com").is_none());
    assert!(resolver.lookup("other.example.com").is_none());
    assert!(resolver.lookup("").is_none());
}
