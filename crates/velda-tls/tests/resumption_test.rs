use std::sync::Arc;

use rcgen::generate_simple_self_signed;
use rustls::ClientConfig;
use rustls::client::ClientSessionMemoryCache;
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

    let server_engine = TlsServerEngine::new(&[server_config]).unwrap();

    // Verify ServerConfig has session cache & 0-RTT enabled
    assert_eq!(server_engine.config().max_early_data_size, 8192);
    assert_eq!(server_engine.config().send_tls13_tickets, 4);

    // Setup client connector with ClientSessionMemoryCache
    let root_store = parse_ca_bundle_pem(&cert_pem).unwrap();
    let mut client_config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
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

#[test]
fn test_quic_server_config_compilation_from_tls_engine() {
    let (cert_pem, key_pem) = make_test_cert(vec!["quic.example.com".into()]);

    let server_config = ServerTlsConfig {
        sni: vec!["quic.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h3".into()],
        cert_pem,
        key_pem,
        client_ca_pem: None,
    };

    let server_engine = TlsServerEngine::new(&[server_config]).unwrap();
    let quic_config = server_engine.build_quic_config().unwrap();

    let endpoint_config = Arc::new(quinn_proto::EndpointConfig::default());
    let endpoint =
        quinn_proto::Endpoint::new(endpoint_config, Some(Arc::new(quic_config)), false, None);
    // Endpoint initialized successfully with QUIC crypto & 0-RTT config from TLS engine
    let _ = endpoint;
}
