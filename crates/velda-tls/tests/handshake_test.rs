//! Integration tests for TLS handshake, ALPN negotiation, and timeout defense.

mod common;

use common::{make_client_connector, make_test_cert};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use velda_tls::{ServerTlsConfig, TlsServerEngine};

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
    let connector = make_client_connector(&cert_pem, Some(vec!["h2"]));

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
async fn test_reject_unknown_sni() {
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
    let connector = make_client_connector(&cert_pem, Some(vec!["h2"]));

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

#[tokio::test]
async fn test_downstream_handshake_timeout() {
    let (cert_pem, key_pem) = make_test_cert(vec!["timeout.test".into()]);

    let server_config = ServerTlsConfig {
        sni: vec!["timeout.test".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem: cert_pem.clone(),
        key_pem,
        client_ca_pem: None,
    };

    let server_engine = TlsServerEngine::new(&[server_config]).unwrap();

    // 1. Client connects via duplex but never sends ClientHello (stalled / Slowloris attack)
    {
        let (_client_io, server_io) = duplex(1024);

        let res = server_engine
            .accept_with_explicit_timeout(server_io, std::time::Duration::from_millis(50))
            .await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            err.to_string().contains("timed out"),
            "Error should indicate timeout: {err}"
        );
    }

    // 2. Client completes handshake promptly within timeout
    {
        let (client_io, server_io) = duplex(65536);
        let s_engine = server_engine.clone();

        let srv_task = tokio::spawn(async move {
            let res = s_engine
                .accept_with_explicit_timeout(server_io, std::time::Duration::from_millis(500))
                .await;
            assert!(res.is_ok(), "Handshake within timeout must succeed");
        });

        let connector = make_client_connector(&cert_pem, Some(vec!["h2"]));
        let s_name = ServerName::try_from("timeout.test".to_string()).unwrap();
        let client_res = connector.connect(s_name, client_io).await;
        assert!(client_res.is_ok());

        srv_task.await.unwrap();
    }
}

#[tokio::test]
async fn test_version_enforcement() {
    let (cert_pem, key_pem) = make_test_cert(vec!["version.test".into()]);

    // 1. Server configured exclusively for TLS 1.3
    let server_config_tls13 = ServerTlsConfig {
        sni: vec!["version.test".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem: cert_pem.clone(),
        key_pem: key_pem.clone(),
        client_ca_pem: None,
    };
    let server_engine_tls13 = TlsServerEngine::new(&[server_config_tls13]).unwrap();

    // Client requesting TLS 1.3 -> Success
    let client_cfg_13 = velda_tls::ClientTlsConfig {
        sni: vec!["version.test".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        ca_pem: Some(cert_pem.clone()),
        client_cert_pem: None,
        client_key_pem: None,
        insecure_skip_verify: false,
    };
    let client_engine_13 = velda_tls::TlsClientEngine::new(&[client_cfg_13]).unwrap();

    let (client_io, server_io) = duplex(65536);
    let s_engine = server_engine_tls13.clone();
    let srv_task = tokio::spawn(async move { s_engine.accept(server_io).await });
    let client_res = client_engine_13.connect("version.test", client_io).await;
    assert!(
        client_res.is_ok(),
        "TLS 1.3 client connecting to TLS 1.3 server must succeed"
    );
    let srv_res = srv_task.await.unwrap();
    assert!(srv_res.is_ok(), "Server accept for TLS 1.3 must succeed");

    // Client requesting ONLY TLS 1.2 -> Must FAIL against TLS 1.3 server!
    let client_cfg_12 = velda_tls::ClientTlsConfig {
        sni: vec!["version.test".into()],
        versions: vec!["tls1.2".into()],
        alpn: vec!["h2".into()],
        ca_pem: Some(cert_pem.clone()),
        client_cert_pem: None,
        client_key_pem: None,
        insecure_skip_verify: false,
    };
    let client_engine_12 = velda_tls::TlsClientEngine::new(&[client_cfg_12]).unwrap();

    let (client_io2, server_io2) = duplex(65536);
    let s_engine2 = server_engine_tls13.clone();
    let srv_task2 = tokio::spawn(async move { s_engine2.accept(server_io2).await });
    let client_res2 = client_engine_12.connect("version.test", client_io2).await;
    assert!(
        client_res2.is_err(),
        "TLS 1.2 client connecting to TLS 1.3-only server must fail"
    );
    let _ = srv_task2.await;

    // 2. Server configured exclusively for TLS 1.2
    let server_config_tls12 = ServerTlsConfig {
        sni: vec!["version.test".into()],
        versions: vec!["tls1.2".into()],
        alpn: vec!["http/1.1".into()],
        cert_pem: cert_pem.clone(),
        key_pem,
        client_ca_pem: None,
    };
    let server_engine_tls12 = TlsServerEngine::new(&[server_config_tls12]).unwrap();

    // Client with TLS 1.3 only -> Must FAIL against TLS 1.2 server!
    let (client_io3, server_io3) = duplex(65536);
    let s_engine3 = server_engine_tls12.clone();
    let srv_task3 = tokio::spawn(async move { s_engine3.accept(server_io3).await });
    let client_res3 = client_engine_13.connect("version.test", client_io3).await;
    assert!(
        client_res3.is_err(),
        "TLS 1.3 client connecting to TLS 1.2-only server must fail"
    );
    let _ = srv_task3.await;
}
