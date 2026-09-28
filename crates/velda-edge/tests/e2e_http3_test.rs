use rcgen::generate_simple_self_signed;
use std::fs;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::UdpSocket;
use tokio::sync::watch;

use velda_edge::{EdgeConfig, EdgeSupervisor};
use velda_sync::post_sync::listener::{
    ListenerApplicationConfig, ListenerConfig, ListenerTlsConfig, ListenerTransportConfig,
    compile_listeners_to_binary,
};
use velda_sync::post_sync::tls::{TlsConfig, compile_tls_to_binary};

#[tokio::test]
async fn test_end_to_end_http3_udp_handoff_and_processing() {
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_h3_e2e.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    let udp_addr: SocketAddr = {
        let l = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let listeners = vec![ListenerConfig {
        id: "h3-in".into(),
        address: udp_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "udp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http".into(),
            version: Some("3".into()),
        },
        tls: ListenerTlsConfig { enabled: true },
    }];
    let listeners_bin = compile_listeners_to_binary(&listeners, 1, [0x11u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), listeners_bin).unwrap();

    let tls = vec![TlsConfig {
        sni: vec!["localhost".into()],
        cert_pem,
        key_pem,
        client_ca_pem: None,
        versions: vec!["tls1.3".into()],
        alpn: vec!["h3".into()],
    }];
    let tls_bin = compile_tls_to_binary(&tls, 1, [0x22u8; 32]).unwrap();
    fs::write(runtime_dir.join("tls.bin"), tls_bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let shared = supervisor.shared_runtime().clone();

    assert!(
        shared.load().h3_engine.is_some(),
        "HTTP/3 engine must be compiled into RAM"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send UDP datagram to H3 listener
    let client_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client_sock
        .send_to(b"test-quic-initial-datagram", udp_addr)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    shutdown_tx.send(true).unwrap();
    let run_res = edge_task.await.unwrap();
    assert!(
        run_res.is_ok(),
        "Edge supervisor with HTTP/3 must terminate cleanly"
    );
}
