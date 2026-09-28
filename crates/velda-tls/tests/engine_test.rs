use rcgen::generate_simple_self_signed;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use velda_tls::{ClientTlsConfig, ServerTlsConfig, TlsEngine};

fn make_test_cert(sans: Vec<String>) -> (String, String) {
    let certified_key = generate_simple_self_signed(sans).unwrap();
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.signing_key.serialize_pem();
    (cert_pem, key_pem)
}

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
    assert!(engine.client().contains_sni("backend.internal"));
    assert!(!engine.client().contains_sni("unknown.internal"));

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
    let arc_client_config = client_cfg.build_client_config().unwrap();

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
