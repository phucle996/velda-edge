use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::watch;

use velda_edge::{EdgeConfig, EdgeSupervisor};
use velda_sync::ipc::{SyncNotification, send_notification};
use velda_sync::post_sync::listener::{
    ListenerApplicationConfig, ListenerConfig, ListenerTransportConfig, compile_listeners_to_binary,
};

#[tokio::test]
async fn test_end_to_end_cold_start_and_uds_hot_reload() {
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let runtime_dir = storage_dir.join("runtime");
    let socket_path = tmp.path().join("edge_e2e.sock");
    fs::create_dir_all(&runtime_dir).unwrap();

    // 1. Initial State: velda-sync compiles initial listeners.bin (ephemeral port)
    let dummy_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let initial_listeners = vec![ListenerConfig {
        id: "initial-http".into(),
        address: dummy_addr.to_string(),
        transport: ListenerTransportConfig {
            protocol: "tcp".into(),
        },
        application: ListenerApplicationConfig {
            protocol: "http".into(),
            version: Some("1.1".into()),
        },
        tls: Default::default(),
    }];

    let initial_bin = compile_listeners_to_binary(&initial_listeners, 1, [0x11u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), initial_bin).unwrap();

    // 2. Cold Start: Bootstrap velda-edge
    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
    let shared = supervisor.shared_runtime().clone();

    assert_eq!(shared.load().revision, 1);
    assert_eq!(shared.load().listener_count(), 1);
    assert_eq!(shared.load().config.listeners[0].id, "initial-http");

    // 3. Launch Edge supervisor in background task
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let edge_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    // Wait briefly for UDS IPC server to bind
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        socket_path.exists(),
        "velda.sock must exist while edge is running"
    );

    // 4. Hot Reload: velda-sync publishes revision 2 with a dynamic port listener
    let dynamic_port: SocketAddr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };

    let updated_listeners = vec![
        ListenerConfig {
            id: "initial-http".into(),
            address: dummy_addr.to_string(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http".into(),
                version: Some("1.1".into()),
            },
            tls: Default::default(),
        },
        ListenerConfig {
            id: "reloaded-tcp".into(),
            address: dynamic_port.to_string(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "raw".into(),
                version: None,
            },
            tls: Default::default(),
        },
    ];
    let updated_bin = compile_listeners_to_binary(&updated_listeners, 2, [0x22u8; 32]).unwrap();
    fs::write(runtime_dir.join("listeners.bin"), updated_bin).unwrap();

    let notif = SyncNotification {
        manifest_revision: Some(2),
        bin_path: "runtime/listeners.bin".into(),
        changed_domains: vec!["listeners".into()],
        domain_revisions: HashMap::new(),
        domain_bins: HashMap::new(),
    };

    // Send reload notification over UDS
    send_notification(&socket_path, &notif).await.unwrap();

    // Give a brief tick for the atomic swap and engine reconcile to execute
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Verify atomic swap to revision 2 occurred in-memory without downtime
    assert_eq!(shared.load().revision, 2);
    assert_eq!(shared.load().listener_count(), 2);
    assert_eq!(shared.load().config.listeners[1].id, "reloaded-tcp");

    // Verify TrafficEngine dynamically bound and opened the new OS port
    let stream_res = tokio::net::TcpStream::connect(dynamic_port).await;
    assert!(
        stream_res.is_ok(),
        "TrafficEngine must dynamically bind and accept connections on reloaded port"
    );

    // 5. Signal graceful shutdown
    shutdown_tx.send(true).unwrap();

    let run_res = edge_task.await.unwrap();
    assert!(
        run_res.is_ok(),
        "Edge supervisor must terminate cleanly: {:?}",
        run_res
    );

    // Verify socket file was unlinked
    assert!(
        !socket_path.exists(),
        "velda.sock must be cleaned up after shutdown"
    );
}
