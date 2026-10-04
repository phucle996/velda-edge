//! Comprehensive tests for Mutual TLS (mTLS) in both Downstream and Upstream directions.

mod common;

use common::{make_client_connector, make_mtls_client_connector, make_test_cert};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use velda_tls::client::{ClientTlsConfig, TlsClientEngine};
use velda_tls::{ServerTlsConfig, TlsServerEngine};

#[tokio::test]
async fn test_downstream_mtls_authentication_flows() {
    let (server_cert, server_key) = make_test_cert(vec!["mtls.example.com".into()]);
    let (client_cert, client_key) = make_test_cert(vec!["client.internal".into()]);
    let (untrusted_cert, untrusted_key) = make_test_cert(vec!["attacker.net".into()]);

    let server_config = ServerTlsConfig {
        sni: vec!["mtls.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem: server_cert.clone(),
        key_pem: server_key,
        client_ca_pem: Some(client_cert.clone()),
    };

    let server_engine = TlsServerEngine::new(&[server_config]).unwrap();

    // 1. Client without certificate must be rejected by mTLS server
    {
        let connector = make_client_connector(&server_cert, Some(vec!["h2"]));
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

        if let Ok(mut c_stream) = client_res {
            let mut buf = [0u8; 1];
            assert_eq!(c_stream.read(&mut buf).await.unwrap_or(0), 0);
        }
    }

    // 2. Client with untrusted certificate must be rejected by mTLS server
    {
        let connector = make_mtls_client_connector(
            &server_cert,
            &untrusted_cert,
            &untrusted_key,
            Some(vec!["h2"]),
        );
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
            assert_eq!(c_stream.read(&mut buf).await.unwrap_or(0), 0);
        }
    }

    // 3. Client with valid certificate accepted by trusted CA succeeds
    {
        let connector =
            make_mtls_client_connector(&server_cert, &client_cert, &client_key, Some(vec!["h2"]));
        let (client_io, server_io) = duplex(65536);

        let s_engine = server_engine.clone();
        let server_task = tokio::spawn(async move {
            let mut stream = s_engine.accept(server_io).await.unwrap();
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

#[tokio::test]
async fn test_upstream_mtls_with_tls_client_engine() {
    let (backend_cert, backend_key) = make_test_cert(vec!["backend.internal".into()]);
    let (gateway_client_cert, gateway_client_key) =
        make_test_cert(vec!["edge-gateway.internal".into()]);

    // Backend server requiring mTLS client cert
    let backend_server_cfg = ServerTlsConfig {
        sni: vec!["backend.internal".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem: backend_cert.clone(),
        key_pem: backend_key,
        client_ca_pem: Some(gateway_client_cert.clone()),
    };
    let backend_server_engine = TlsServerEngine::new(&[backend_server_cfg]).unwrap();

    // 1. TlsClientEngine with client certificate succeeds connecting to backend
    {
        let client_tls_config = ClientTlsConfig {
            sni: vec!["backend.internal".into()],
            versions: vec!["tls1.3".into()],
            alpn: vec!["h2".into()],
            ca_pem: Some(backend_cert.clone()),
            client_cert_pem: Some(gateway_client_cert.clone()),
            client_key_pem: Some(gateway_client_key.clone()),
            insecure_skip_verify: false,
        };

        let client_engine = TlsClientEngine::new(&[client_tls_config]).unwrap();
        let (edge_io, backend_io) = duplex(65536);

        let b_engine = backend_server_engine.clone();
        let backend_task = tokio::spawn(async move {
            let mut stream = b_engine.accept(backend_io).await.unwrap();
            let info = TlsServerEngine::extract_handshake_info(&stream);
            assert!(
                info.peer_certs.is_some(),
                "Backend server must receive Gateway's client cert"
            );
            let mut buf = [0u8; 5];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello");
            stream.write_all(b"world").await.unwrap();
        });

        let mut edge_stream = client_engine
            .connect("backend.internal", edge_io)
            .await
            .expect("Upstream mTLS connect should succeed");

        edge_stream.write_all(b"hello").await.unwrap();
        let mut resp = [0u8; 5];
        edge_stream.read_exact(&mut resp).await.unwrap();
        assert_eq!(&resp, b"world");

        backend_task.await.unwrap();
    }

    // 2. TlsClientEngine without client certificate fails connecting to mTLS-requiring backend
    {
        let client_tls_config_no_auth = ClientTlsConfig {
            sni: vec!["backend.internal".into()],
            versions: vec!["tls1.3".into()],
            alpn: vec!["h2".into()],
            ca_pem: Some(backend_cert),
            client_cert_pem: None,
            client_key_pem: None,
            insecure_skip_verify: false,
        };

        let client_engine = TlsClientEngine::new(&[client_tls_config_no_auth]).unwrap();
        let (edge_io, backend_io) = duplex(65536);

        let b_engine = backend_server_engine.clone();
        let backend_task = tokio::spawn(async move {
            let res = b_engine.accept(backend_io).await;
            assert!(
                res.is_err(),
                "Backend must reject gateway without client cert"
            );
        });

        let client_res = client_engine.connect("backend.internal", edge_io).await;
        // Either connect handshake errors or post-handshake read errors
        if let Ok(mut stream) = client_res {
            let mut buf = [0u8; 1];
            assert_eq!(stream.read(&mut buf).await.unwrap_or(0), 0);
        }

        backend_task.await.unwrap();
    }
}
