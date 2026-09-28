use rcgen::generate_simple_self_signed;
use std::fs;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpStream;
use tokio::sync::watch;

use velda_edge::{EdgeConfig, EdgeSupervisor};
use velda_sync::post_sync::listener::{
    ListenerApplicationConfig, ListenerConfig, ListenerTlsConfig, ListenerTransportConfig,
    compile_listeners_to_binary,
};
use velda_sync::post_sync::tls::{TlsConfig, compile_tls_to_binary};
use velda_tls::{ClientTlsConfig, TlsClientEngine};

#[tokio::test]
async fn test_end_to_end_tls_downstream_termination() {
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_tls_e2e.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    // 1. Generate self-signed certificate for "localhost"
    let cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    // Pick an available ephemeral port
    let https_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    // 2. Compile listeners.bin with TLS enabled on port
    let listeners = vec![ListenerConfig {
        id: "https-in".into(),
        address: https_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http".into(),
            version: Some("1.1".into()),
        },
        tls: ListenerTlsConfig { enabled: true },
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0x11u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    // 3. Compile tls.bin with the generated cert and SNI "localhost"
    let tls = vec![TlsConfig {
        sni: vec!["localhost".into()],
        cert_pem: cert_pem.clone(),
        key_pem,
        client_ca_pem: None,
        versions: vec!["tls1.3".into()],
        alpn: vec!["http/1.1".into()],
    }];
    let tls_bin = compile_tls_to_binary(&tls, 1, [0x22u8; 32]).unwrap();
    fs::write(runtime_dir.join("tls.bin"), tls_bin).unwrap();

    // 4. Cold-start bootstrap supervisor
    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let shared = supervisor.shared_runtime().clone();

    assert_eq!(shared.load().listener_count(), 1);
    assert_eq!(shared.load().config.tls.len(), 1);
    assert!(
        shared.load().tls_server.is_some(),
        "Downstream TLS server engine must be compiled into RAM"
    );

    // 5. Run supervisor in background
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    // Allow supervisor to start accept loop
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 6. Connect client with TLS client engine
    let client_config = ClientTlsConfig {
        sni: vec!["localhost".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["http/1.1".into()],
        ca_pem: Some(cert_pem),
        client_cert_pem: None,
        client_key_pem: None,
    };
    let client_engine = TlsClientEngine::new(&[client_config]).unwrap();

    let tcp_stream = TcpStream::connect(https_addr).await.unwrap();
    let mut tls_client_stream = client_engine
        .connect("localhost", tcp_stream)
        .await
        .expect("Client TLS handshake to Edge must succeed");

    // Verify ALPN protocol negotiated
    let (_, client_conn) = tls_client_stream.get_ref();
    assert_eq!(
        client_conn.alpn_protocol(),
        Some(b"http/1.1".as_slice()),
        "Negotiated ALPN must be http/1.1"
    );

    // Send HTTP/1.1 request over secure TLS tunnel
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tls_client_stream
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();

    let mut resp = vec![0u8; 1024];
    let n = tls_client_stream.read(&mut resp).await.unwrap();
    let resp_str = String::from_utf8(resp[..n].to_vec()).unwrap();
    assert!(resp_str.contains("HTTP/1.1 200 OK"));
    assert!(resp_str.contains("velda-edge"));

    // 7. Graceful shutdown
    shutdown_tx.send(true).unwrap();
    let run_res = edge_task.await.unwrap();
    assert!(
        run_res.is_ok(),
        "Edge supervisor must terminate cleanly: {:?}",
        run_res
    );
}

#[tokio::test]
async fn test_end_to_end_tls_h2_downstream() {
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_h2_e2e.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    let https_addr: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let listeners = vec![ListenerConfig {
        id: "https-h2-in".into(),
        address: https_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http".into(),
            version: None,
        },
        tls: ListenerTlsConfig { enabled: true },
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0x11u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    let tls = vec![TlsConfig {
        sni: vec!["localhost".into()],
        cert_pem: cert_pem.clone(),
        key_pem,
        client_ca_pem: None,
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
    }];
    let tls_bin = compile_tls_to_binary(&tls, 1, [0x22u8; 32]).unwrap();
    fs::write(runtime_dir.join("tls.bin"), tls_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client_config = ClientTlsConfig {
        sni: vec!["localhost".into()],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into()],
        ca_pem: Some(cert_pem),
        client_cert_pem: None,
        client_key_pem: None,
    };
    let client_engine = TlsClientEngine::new(&[client_config]).unwrap();

    let tcp_stream = TcpStream::connect(https_addr).await.unwrap();
    let tls_client_stream = client_engine
        .connect("localhost", tcp_stream)
        .await
        .expect("Client TLS handshake to Edge must succeed");

    let (_, client_conn) = tls_client_stream.get_ref();
    assert_eq!(
        client_conn.alpn_protocol(),
        Some(b"h2".as_slice()),
        "Negotiated ALPN must be h2"
    );

    let (mut client, h2_conn) = h2::client::handshake(tls_client_stream).await.unwrap();
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    let req = http::Request::builder()
        .method("GET")
        .uri("https://localhost/api/status")
        .body(())
        .unwrap();

    let (response_fut, _) = client.send_request(req, true).unwrap();
    let response = response_fut.await.unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);

    let mut body = response.into_body();
    let chunk = body.data().await.unwrap().unwrap();
    let body_str = String::from_utf8(chunk.to_vec()).unwrap();
    assert!(body_str.contains("velda-edge"));
    assert!(body_str.contains("h2"));

    shutdown_tx.send(true).unwrap();
    let run_res = edge_task.await.unwrap();
    assert!(run_res.is_ok());
}
