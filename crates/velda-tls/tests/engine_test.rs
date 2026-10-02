//! Integration tests for TlsEngine, TlsClientEngine and stateless execution functions.

mod common;

use common::make_test_cert;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use velda_tls::client::TlsClientEngine;
use velda_tls::{ClientTlsConfig, ServerTlsConfig, TlsEngine, TlsError};

#[tokio::test]
async fn test_tls_engine_end_to_end() {
    let (server_cert, server_key) = make_test_cert(vec!["gateway.example.com".into()]);
    let (backend_cert, backend_key) = make_test_cert(vec!["backend.internal".into()]);

    let servers = vec![ServerTlsConfig {
        sni: vec!["gateway.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into(), "http/1.1".into()],
        cert_pem: server_cert,
        key_pem: server_key,
        client_ca_pem: None,
    }];

    let upstreams = vec![ClientTlsConfig {
        sni: vec!["backend.internal".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        ca_pem: Some(backend_cert.clone()),
        client_cert_pem: None,
        client_key_pem: None,
    }];

    let engine = TlsEngine::new(&servers, &upstreams).expect("Failed to build TlsEngine");

    // 1. Verify upstream connector capability
    assert_eq!(engine.client().len(), 1);
    assert!(!engine.client().is_empty());
    assert!(engine.client().contains_sni("backend.internal"));
    assert!(!engine.client().contains_sni("unknown.internal"));
    assert!(engine.client().get_connector("backend.internal").is_some());
    assert!(engine.client().get_connector("missing.internal").is_none());

    // 2. Test Upstream TLS connection to simulated backend server
    let backend_server_engine = velda_tls::TlsServerEngine::new(&[ServerTlsConfig {
        sni: vec!["backend.internal".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem: backend_cert,
        key_pem: backend_key,
        client_ca_pem: None,
    }])
    .unwrap();

    let (edge_io, backend_io) = duplex(65536);

    let backend_task = tokio::spawn(async move {
        let mut stream = backend_server_engine.accept(backend_io).await.unwrap();
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"req1");
        stream.write_all(b"res1").await.unwrap();
    });

    let mut edge_upstream_stream = engine.connect("backend.internal", edge_io).await.unwrap();
    edge_upstream_stream.write_all(b"req1").await.unwrap();
    let mut reply = [0u8; 4];
    edge_upstream_stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"res1");

    backend_task.await.unwrap();
}

#[tokio::test]
async fn test_stateless_execution_accept_and_connect() {
    let (server_cert, server_key) = make_test_cert(vec!["direct.example.com".into()]);

    let server_cfg = ServerTlsConfig {
        sni: vec!["direct.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        cert_pem: server_cert.clone(),
        key_pem: server_key,
        client_ca_pem: None,
    };
    let arc_server_config = ServerTlsConfig::build(&[server_cfg]).unwrap();

    let client_cfg = ClientTlsConfig {
        sni: vec!["direct.example.com".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        ca_pem: Some(server_cert),
        client_cert_pem: None,
        client_key_pem: None,
    };
    let arc_client_config = client_cfg.build().unwrap();
    assert!(client_cfg.build_client_config().is_ok());

    let (client_io, server_io) = duplex(65536);

    let server_task = tokio::spawn(async move {
        // Direct stateless execution: accept(stream, &config)
        let mut server_stream = velda_tls::server::accept(server_io, &arc_server_config)
            .await
            .unwrap();
        let mut buf = [0u8; 4];
        server_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
        server_stream.write_all(b"pong").await.unwrap();
    });

    // Direct stateless execution: connect(stream, &config, sni)
    let mut client_stream =
        velda_tls::client::connect(client_io, &arc_client_config, "direct.example.com")
            .await
            .unwrap();
    client_stream.write_all(b"ping").await.unwrap();
    let mut reply = [0u8; 4];
    client_stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"pong");

    server_task.await.unwrap();
}

#[tokio::test]
async fn test_tls_client_engine_negative_paths() {
    let client_engine = TlsClientEngine::new(&[]).unwrap();
    assert!(client_engine.is_empty());
    assert_eq!(client_engine.len(), 0);

    let (io1, _io2) = duplex(1024);
    // Connect to unconfigured upstream SNI
    let res_unknown = client_engine.connect("unknown.backend.internal", io1).await;
    assert!(matches!(
        res_unknown,
        Err(TlsError::UpstreamTargetNotFound(_))
    ));

    // Connect with invalid DNS name
    let (io3, _io4) = duplex(1024);
    let res_invalid_sni = client_engine.connect("!@#$%^ invalid dns", io3).await;
    assert!(matches!(
        res_invalid_sni,
        Err(TlsError::UpstreamTargetNotFound(_) | TlsError::SniNotFound(_))
    ));
}

#[tokio::test]
async fn test_tls_engine_without_server_rejects_accept() {
    let engine = TlsEngine::new(&[], &[]).unwrap();
    assert!(engine.server().is_none());

    let (io1, _io2) = duplex(1024);
    let res = engine.accept(io1).await;
    assert!(matches!(res, Err(TlsError::HandshakeFailed(_))));
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("No downstream TLS servers configured"),
        "Unexpected error: {err_msg}"
    );
}

#[test]
fn test_client_tls_config_mismatched_keypair_fails_transparently() {
    let (cert_pem, key_pem) = make_test_cert(vec!["test.internal".into()]);

    // 1. Cert provided but key missing
    let cfg_no_key = ClientTlsConfig {
        sni: vec!["test.internal".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        ca_pem: None,
        client_cert_pem: Some(cert_pem.clone()),
        client_key_pem: None,
    };
    let err = cfg_no_key.build_client_config().unwrap_err();
    assert!(
        matches!(err, TlsError::InvalidPrivateKey(_)),
        "Must error out when key is missing"
    );

    // 2. Key provided but cert missing
    let cfg_no_cert = ClientTlsConfig {
        sni: vec!["test.internal".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        ca_pem: None,
        client_cert_pem: None,
        client_key_pem: Some(key_pem),
    };
    let err = cfg_no_cert.build_client_config().unwrap_err();
    assert!(
        matches!(err, TlsError::InvalidCertificate(_)),
        "Must error out when cert is missing"
    );
}

#[test]
fn test_client_engine_invalid_sni_fails_fast_at_startup() {
    let bad_cfg = ClientTlsConfig {
        sni: vec!["!@# invalid dns name".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        ca_pem: None,
        client_cert_pem: None,
        client_key_pem: None,
    };
    let res = TlsClientEngine::new(&[bad_cfg]);
    assert!(
        matches!(res, Err(TlsError::SniNotFound(_))),
        "Must fail fast at compilation when SNI is not a valid DNS name"
    );
}
