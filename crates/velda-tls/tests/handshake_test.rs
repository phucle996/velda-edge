use std::sync::Arc;

use rcgen::generate_simple_self_signed;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio_rustls::TlsConnector;
use velda_tls::pem::parse_ca_bundle_pem;
use velda_tls::{ServerTlsConfig, TlsServerEngine};

fn make_test_cert(sans: Vec<String>) -> (String, String) {
    let certified_key = generate_simple_self_signed(sans).unwrap();
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.signing_key.serialize_pem();
    (cert_pem, key_pem)
}

#[tokio::test]
async fn test_tls_handshake_and_alpn_negotiation() {
    let (cert_pem, key_pem) =
        make_test_cert(vec!["api.example.com".into(), "*.service.local".into()]);

    let server_config = ServerTlsConfig {
        sni: vec!["api.example.com".into(), "*.service.local".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into(), "http/1.1".into()],
        cert_pem: cert_pem.clone(),
        key_pem,
        client_ca_pem: None,
    };

    let server_engine = TlsServerEngine::new(&[server_config]).unwrap();

    // Setup client connector that trusts the self-signed cert as CA
    let root_store = parse_ca_bundle_pem(&cert_pem).unwrap();
    let mut client_config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(root_store)
            .with_no_client_auth();
    client_config.alpn_protocols = vec![b"h2".to_vec()];

    let connector = TlsConnector::from(Arc::new(client_config));

    // 1. Successful handshake with exact SNI
    let (client_io, server_io) = duplex(65536);

    let server_task = {
        let server_engine = server_engine.clone();
        tokio::spawn(async move {
            let mut tls_stream = server_engine.accept(server_io).await.unwrap();
            let info = TlsServerEngine::extract_handshake_info(&tls_stream);
            assert_eq!(info.alpn.as_deref(), Some("h2"));
            assert_eq!(info.sni.as_deref(), Some("api.example.com"));

            let mut buf = [0u8; 5];
            tls_stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping!");

            tls_stream.write_all(b"pong!").await.unwrap();
            tls_stream.flush().await.unwrap();
        })
    };

    let server_name = ServerName::try_from("api.example.com".to_string()).unwrap();
    let mut client_stream = connector.connect(server_name, client_io).await.unwrap();

    client_stream.write_all(b"ping!").await.unwrap();
    client_stream.flush().await.unwrap();

    let mut response = [0u8; 5];
    client_stream.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"pong!");

    server_task.await.unwrap();
}

#[tokio::test]
async fn test_strict_no_sni_or_unknown_sni_rejects_handshake() {
    let (cert_pem, key_pem) = make_test_cert(vec!["api.example.com".into()]);

    let server_config = ServerTlsConfig {
        sni: vec!["api.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem: cert_pem.clone(),
        key_pem,
        client_ca_pem: None,
    };

    let server_engine = TlsServerEngine::new(&[server_config]).unwrap();

    let root_store = parse_ca_bundle_pem(&cert_pem).unwrap();
    let client_config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(root_store)
            .with_no_client_auth();

    let connector = TlsConnector::from(Arc::new(client_config));

    // Connect with unrecognized SNI "unknown.domain.com"
    let (client_io, server_io) = duplex(65536);

    let server_task = {
        let server_engine = server_engine.clone();
        tokio::spawn(async move {
            let res = server_engine.accept(server_io).await;
            // Handshake must fail because SNI was rejected
            assert!(res.is_err(), "Server must reject unrecognized SNI");
        })
    };

    let server_name = ServerName::try_from("unknown.domain.com".to_string()).unwrap();
    let client_res = connector.connect(server_name, client_io).await;
    assert!(
        client_res.is_err(),
        "Client must fail handshake when server rejects SNI"
    );

    server_task.await.unwrap();
}
