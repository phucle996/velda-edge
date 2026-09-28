use std::sync::Arc;

use rcgen::generate_simple_self_signed;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio_rustls::TlsConnector;
use velda_tls::pem::{parse_ca_bundle_pem, parse_certs_pem, parse_private_key_pem};
use velda_tls::{ServerTlsConfig, TlsServerEngine};

fn make_test_cert(sans: Vec<String>) -> (String, String) {
    let certified_key = generate_simple_self_signed(sans).unwrap();
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.signing_key.serialize_pem();
    (cert_pem, key_pem)
}

#[tokio::test]
async fn test_downstream_mtls_authentication() {
    let (server_cert, server_key) = make_test_cert(vec!["mtls.example.com".into()]);
    let (ca_cert, _) = make_test_cert(vec!["ca.internal".into()]);
    let (client_cert, client_key) = make_test_cert(vec!["client.internal".into()]);
    let (untrusted_cert, untrusted_key) = make_test_cert(vec!["attacker.net".into()]);

    let server_config = ServerTlsConfig {
        sni: vec!["mtls.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem: server_cert.clone(),
        key_pem: server_key,
        client_ca_pem: Some(ca_cert.clone()),
    };

    let server_engine = TlsServerEngine::new(&[server_config]).unwrap();
    let server_root_store = parse_ca_bundle_pem(&server_cert).unwrap();

    // 1. Client without certificate must be rejected by mTLS server
    {
        let client_config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(server_root_store.clone())
                .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(client_config));
        let (client_io, server_io) = duplex(65536);

        let s_engine = server_engine.clone();
        let server_task = tokio::spawn(async move { s_engine.accept(server_io).await });

        let server_name = ServerName::try_from("mtls.example.com".to_string()).unwrap();
        let client_res = connector.connect(server_name, client_io).await;
        let server_res = server_task.await.unwrap();

        assert!(
            server_res.is_err(),
            "mTLS Server must reject client with no certificate"
        );
        let err_msg = server_res.unwrap_err().to_string();
        assert!(
            err_msg.contains("no certificates"),
            "Expected no cert error, got: {err_msg}"
        );

        // If client's connect initially completed flight, any read must immediately fail or EOF
        if let Ok(mut c_stream) = client_res {
            let mut buf = [0u8; 1];
            assert!(c_stream.read(&mut buf).await.unwrap_or(0) == 0);
        }
    }

    // 2. Client with untrusted certificate must be rejected by mTLS server
    {
        let bad_certs = parse_certs_pem(&untrusted_cert).unwrap();
        let bad_key = parse_private_key_pem(&untrusted_key).unwrap();

        let client_config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(server_root_store.clone())
                .with_client_auth_cert(bad_certs, bad_key)
                .unwrap();

        let connector = TlsConnector::from(Arc::new(client_config));
        let (client_io, server_io) = duplex(65536);

        let s_engine = server_engine.clone();
        let server_task = tokio::spawn(async move { s_engine.accept(server_io).await });

        let server_name = ServerName::try_from("mtls.example.com".to_string()).unwrap();
        let client_res = connector.connect(server_name, client_io).await;
        let server_res = server_task.await.unwrap();

        assert!(
            server_res.is_err(),
            "mTLS Server must reject untrusted client certificate"
        );

        if let Ok(mut c_stream) = client_res {
            let mut buf = [0u8; 1];
            assert!(c_stream.read(&mut buf).await.unwrap_or(0) == 0);
        }
    }

    // 3. Client with valid certificate accepted by trusted CA succeeds
    {
        let (s_cert, s_key) = make_test_cert(vec!["mtls.example.com".into()]);
        let server_config_trusted = ServerTlsConfig {
            sni: vec!["mtls.example.com".into()],
            versions: vec!["tls1.3".into()],
            alpn: vec!["h2".into()],
            cert_pem: s_cert.clone(),
            key_pem: s_key,
            client_ca_pem: Some(client_cert.clone()),
        };
        let trusted_server_engine = TlsServerEngine::new(&[server_config_trusted]).unwrap();

        let valid_certs = parse_certs_pem(&client_cert).unwrap();
        let valid_key = parse_private_key_pem(&client_key).unwrap();
        let s_store = parse_ca_bundle_pem(&s_cert).unwrap();

        let client_config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(s_store)
                .with_client_auth_cert(valid_certs, valid_key)
                .unwrap();

        let connector = TlsConnector::from(Arc::new(client_config));
        let (client_io, server_io) = duplex(65536);

        let server_task = tokio::spawn(async move {
            let mut stream = trusted_server_engine.accept(server_io).await.unwrap();
            let info = TlsServerEngine::extract_handshake_info(&stream);
            assert!(info.peer_certs.is_some(), "mTLS peer certs must be present");
            assert!(!info.peer_certs.unwrap().is_empty());

            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"mtls");
            stream.write_all(b"pass").await.unwrap();
        });

        let server_name = ServerName::try_from("mtls.example.com".to_string()).unwrap();
        let mut client_stream = connector.connect(server_name, client_io).await.unwrap();

        client_stream.write_all(b"mtls").await.unwrap();
        let mut reply = [0u8; 4];
        client_stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"pass");

        server_task.await.unwrap();
    }
}
