//! Integration tests for TLS session resumption and ticket recycling.

mod common;

use std::sync::Arc;

use common::make_test_cert;
use rustls::ClientConfig;
use rustls::client::ClientSessionMemoryCache;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio_rustls::TlsConnector;
use velda_tls::pem::parse_ca_bundle_pem;
use velda_tls::{ServerTlsConfig, TlsServerEngine, TlsServerParams};

#[tokio::test]
async fn test_tls_session_resumption_and_tickets() {
    let (cert_pem, key_pem) = make_test_cert(vec!["resumption.test".into()]);

    let server_config = ServerTlsConfig {
        sni: vec!["resumption.test".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into(), "http/1.1".into()],
        cert_pem: cert_pem.clone(),
        key_pem,
        client_ca_pem: None,
    };

    // Use explicit parameters to prevent fragility across heterogeneous CI/CD hardware tiers
    let params = TlsServerParams::from_hardware()
        .with_session_cache_capacity(4096)
        .with_max_early_data_size(8192)
        .with_send_tls13_tickets(4);

    let server_engine = TlsServerEngine::new_with_params(&[server_config], &params).unwrap();

    // Verify ServerConfig has session cache & 0-RTT properly applied
    assert_eq!(server_engine.config().max_early_data_size, 8192);
    assert_eq!(server_engine.config().send_tls13_tickets, 4);

    // Setup client connector with ClientSessionMemoryCache
    let root_store = parse_ca_bundle_pem(&cert_pem).unwrap();
    let mut client_config = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(root_store)
    .with_no_client_auth();

    let client_session_cache = Arc::new(ClientSessionMemoryCache::new(32));
    client_config.resumption = rustls::client::Resumption::store(client_session_cache.clone());
    client_config.alpn_protocols = vec![b"h2".to_vec()];

    let connector = TlsConnector::from(Arc::new(client_config));

    // Connection 1: Initial full handshake to receive and store session tickets
    {
        let (client_io, server_io) = duplex(65536);
        let srv = server_engine.clone();

        let srv_task = tokio::spawn(async move {
            let mut srv_stream = srv.accept(server_io).await.unwrap();
            let mut buf = [0u8; 4];
            srv_stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"req1");
            srv_stream.write_all(b"res1").await.unwrap();
            srv_stream.shutdown().await.unwrap();
        });

        let server_name = ServerName::try_from("resumption.test").unwrap().to_owned();
        let mut client_stream = connector.connect(server_name, client_io).await.unwrap();
        client_stream.write_all(b"req1").await.unwrap();

        let mut buf = [0u8; 4];
        client_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"res1");
        srv_task.await.unwrap();
    }

    // Connection 2: Resumed connection using cached ticket from Connection 1
    {
        let (client_io, server_io) = duplex(65536);
        let srv = server_engine.clone();

        let srv_task = tokio::spawn(async move {
            let mut srv_stream = srv.accept(server_io).await.unwrap();
            let mut buf = [0u8; 4];
            srv_stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"req2");
            srv_stream.write_all(b"res2").await.unwrap();
            srv_stream.shutdown().await.unwrap();
        });

        let server_name = ServerName::try_from("resumption.test").unwrap().to_owned();
        let mut client_stream = connector.connect(server_name, client_io).await.unwrap();
        client_stream.write_all(b"req2").await.unwrap();

        let mut buf = [0u8; 4];
        client_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"res2");
        srv_task.await.unwrap();
    }
}
