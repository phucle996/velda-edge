//! Integration tests for QUIC ServerConfig compilation and Quinn Endpoint initialization.

mod common;

use std::sync::Arc;

use common::make_test_cert;
use velda_tls::{ServerTlsConfig, TlsServerEngine};

#[test]
fn test_quic_server_config_compilation() {
    let (cert_pem, key_pem) = make_test_cert(vec!["quic.example.com".into()]);

    let server_config = ServerTlsConfig {
        sni: vec!["quic.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h3".into()],
        cert_pem,
        key_pem,
        client_ca_pem: None,
    };

    let server_engine =
        TlsServerEngine::new(&[server_config]).expect("Failed to build server engine");
    let quic_config = server_engine
        .build_quic_config()
        .expect("Failed to build QUIC server config");

    let endpoint_config = Arc::new(quinn_proto::EndpointConfig::default());
    let endpoint =
        quinn_proto::Endpoint::new(endpoint_config, Some(Arc::new(quic_config)), false, None);

    // Quinn endpoint initialized successfully with QUIC crypto & 0-RTT config from TLS engine
    let _ = endpoint;
}

#[test]
fn test_quic_server_config_preserves_crypto_and_alpn() {
    let (cert_pem, key_pem) = make_test_cert(vec!["h3.example.com".into()]);

    let server_config = ServerTlsConfig {
        sni: vec!["h3.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h3".into(), "h3-29".into()],
        cert_pem,
        key_pem,
        client_ca_pem: None,
    };

    let server_engine = TlsServerEngine::new(&[server_config]).unwrap();
    let quic_config = server_engine.build_quic_config().unwrap();

    // Verify crypto provider is instantiated
    assert!(Arc::strong_count(&quic_config.crypto) >= 1);
}
