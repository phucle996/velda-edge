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
            protocol: "http3".into(),
            version: None,
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
    let _shared = supervisor.shared_runtime().clone();

    assert!(
        velda_edge::pipeline::l7::http3::has_h3_engine("h3-in"),
        "HTTP/3 engine must be initialized in pipeline for listener"
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

#[tokio::test]
async fn test_reload_preserves_active_http3_engine_instance() {
    use std::sync::Arc;
    use velda_edge::apply_reload;
    use velda_sync::SyncNotification;

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_h3_reload.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    let cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    let udp_addr: SocketAddr = {
        let l = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let listeners = vec![ListenerConfig {
        id: "h3-reload-in".into(),
        address: udp_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "udp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http3".into(),
            version: None,
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

    // 1. Verify engine initialized and grab its pointer
    let engine_before =
        velda_edge::pipeline::l7::http3::get_or_init_h3_engine("h3-reload-in", &shared)
            .expect("H3 engine must exist");

    // 2. Perform a configuration reload (e.g. upstreams or routes changed)
    let notif = SyncNotification {
        manifest_revision: Some(2),
        bin_path: String::new(),
        changed_domains: vec!["routes".into(), "upstreams".into()],
        domain_revisions: std::collections::HashMap::new(),
        domain_bins: std::collections::HashMap::new(),
    };

    let outcome = apply_reload(&shared, &runtime_dir, &notif, None)
        .await
        .unwrap();
    assert_eq!(outcome.revision, 2);

    // 3. Verify the exact same H3 engine instance is preserved (Arc pointer equality!)
    let engine_after =
        velda_edge::pipeline::l7::http3::get_or_init_h3_engine("h3-reload-in", &shared)
            .expect("H3 engine must still exist");

    assert!(
        Arc::ptr_eq(&engine_before, &engine_after),
        "CRITICAL: HTTP/3 engine pointer must be identical across reload to preserve active QUIC connections!"
    );
}
