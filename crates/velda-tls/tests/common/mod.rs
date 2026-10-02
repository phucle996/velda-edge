//! Shared integration test fixtures and mock generators for `velda-tls`.

#![allow(dead_code)]

use std::sync::Arc;

use rcgen::generate_simple_self_signed;
use rustls::ClientConfig;
use tokio_rustls::TlsConnector;
use velda_tls::pem::{parse_ca_bundle_pem, parse_certs_pem, parse_private_key_pem};

/// Generates a valid self-signed certificate and private key PEM pair for testing.
pub fn make_test_cert(sans: Vec<String>) -> (String, String) {
    let certified_key =
        generate_simple_self_signed(sans).expect("Failed to generate test certificate");
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.signing_key.serialize_pem();
    (cert_pem, key_pem)
}

/// Builds a client `TlsConnector` that trusts the provided CA PEM and configures ALPN.
pub fn make_client_connector(ca_pem: &str, alpn: Option<Vec<&str>>) -> TlsConnector {
    let root_store = parse_ca_bundle_pem(ca_pem).expect("Failed to parse CA bundle");
    let mut client_config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("Valid protocol versions")
            .with_root_certificates(root_store)
            .with_no_client_auth();

    if let Some(alpns) = alpn {
        client_config.alpn_protocols = alpns.into_iter().map(|s| s.as_bytes().to_vec()).collect();
    }

    TlsConnector::from(Arc::new(client_config))
}

/// Builds an mTLS client `TlsConnector` presenting a client certificate to the server.
pub fn make_mtls_client_connector(
    ca_pem: &str,
    client_cert_pem: &str,
    client_key_pem: &str,
    alpn: Option<Vec<&str>>,
) -> TlsConnector {
    let root_store = parse_ca_bundle_pem(ca_pem).expect("Failed to parse CA bundle");
    let client_certs = parse_certs_pem(client_cert_pem).expect("Failed to parse client certs");
    let client_key = parse_private_key_pem(client_key_pem).expect("Failed to parse client key");

    let mut client_config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("Valid protocol versions")
            .with_root_certificates(root_store)
            .with_client_auth_cert(client_certs, client_key)
            .expect("Failed to set client auth cert");

    if let Some(alpns) = alpn {
        client_config.alpn_protocols = alpns.into_iter().map(|s| s.as_bytes().to_vec()).collect();
    }

    TlsConnector::from(Arc::new(client_config))
}
