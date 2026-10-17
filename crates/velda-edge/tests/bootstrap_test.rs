//! End-to-end integration tests for Edge process cold-start bootstrap,
//! hardware-adaptive sizing, runtime.json generation/merge, and supervisor lifecycle.

use std::fs;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::watch;

use velda_edge::{EdgeConfig, EdgeSupervisor, RuntimeProfile};

#[tokio::test]
async fn test_bootstrap_and_shutdown() {
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let socket_path = tmp.path().join("edge_bootstrap.sock");
    let runtime_dir = storage_dir.join("runtime");

    assert!(!runtime_dir.exists());

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor =
        EdgeSupervisor::bootstrap(config).expect("Bootstrap must succeed on unconfigured node");

    // 1. Verify runtime.json was generated automatically
    let runtime_json_path = runtime_dir.join("runtime.json");
    assert!(
        runtime_json_path.exists(),
        "runtime.json must be generated on first boot"
    );

    let profile = supervisor.runtime_profile();
    assert!(profile.discovery.max_dns_cache_capacity >= 1_000);
    assert!(profile.discovery.max_lkg_capacity >= 500);
    assert!(profile.transport.io_workers >= 1);

    // 2. Initial state without LKG binary artifacts has revision 0
    assert_eq!(supervisor.shared_runtime().load().revision, 0);
    assert_eq!(supervisor.shared_runtime().load().config.listeners.len(), 0);

    // 3. Launch background supervisor and verify graceful shutdown
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(socket_path.exists());

    shutdown_tx.send(true).unwrap();
    let res = supervisor_task.await.unwrap();
    assert!(res.is_ok());
}

#[test]
fn test_partial_runtime_override() {
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let socket_path = tmp.path().join("edge_override.sock");
    let runtime_dir = storage_dir.join("runtime");
    fs::create_dir_all(&runtime_dir).unwrap();

    // Sparse user-defined configuration: operator specifies ONLY 1 field!
    let sparse_json = r#"{
        "discovery": {
            "max_dns_cache_capacity": 88888
        }
    }"#;
    fs::write(runtime_dir.join("runtime.json"), sparse_json).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor =
        EdgeSupervisor::bootstrap(config).expect("Bootstrap with sparse runtime.json must succeed");

    let profile = supervisor.runtime_profile();
    // Overridden value strictly honored
    assert_eq!(profile.discovery.max_dns_cache_capacity, 88_888);

    // Omitted fields cleanly populated from continuous hardware fallback
    assert!(profile.discovery.max_lkg_capacity > 0);
    assert!(profile.discovery.max_negative_ttl_secs >= profile.discovery.negative_ttl_secs);
    assert!(profile.discovery.query_timeout_ms >= 1000);
    assert!(profile.transport.io_workers >= 1);

    // File was updated with complete merged snapshot
    let updated_content = fs::read_to_string(runtime_dir.join("runtime.json")).unwrap();
    let parsed: RuntimeProfile = serde_json::from_str(&updated_content).unwrap();
    assert_eq!(parsed.discovery.max_dns_cache_capacity, 88_888);
}

#[tokio::test]
async fn test_bootstrap_with_prometheus_exporter() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use velda_sync::compile_plugins_to_binary;
    use velda_sync::post_sync::plugin::PluginConfig;

    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path().join("storage");
    let socket_path = tmp.path().join("edge_prom.sock");
    let runtime_dir = storage_dir.join("runtime");
    fs::create_dir_all(&runtime_dir).unwrap();

    // Get a free port for Prometheus exporter
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let prom_port = listener.local_addr().unwrap().port();
    drop(listener);
    let prom_addr = format!("127.0.0.1:{prom_port}");

    // Compile plugins.bin with Prometheus exporter
    let exporter_plugin = PluginConfig {
        id: "prom-exporter-test".into(),
        plugin_type: "prometheus_exporter".into(),
        phase: "background".into(),
        priority: None,
        config: serde_json::json!({
            "listen": prom_addr,
            "path": "/metrics",
            "protocol": "http",
            "scrape": {
                "timeout_ms": 5000,
                "max_concurrency": 8,
                "max_buffer_size_kb": 256
            },
            "tls": { "mode": "none" },
            "cors": { "enabled": false },
            "auth": { "mode": "none" }
        }),
    };
    let bin = compile_plugins_to_binary(&[exporter_plugin], 1, [0x42u8; 32]).unwrap();
    fs::write(runtime_dir.join("plugins.bin"), bin).unwrap();

    let config = EdgeConfig::new(&storage_dir, &socket_path);
    let supervisor =
        EdgeSupervisor::bootstrap(config).expect("Bootstrap with plugins must succeed");

    assert_eq!(supervisor.shared_runtime().load().config.plugins.len(), 1);

    // Launch background supervisor
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor_task = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 1. Scrape /healthz
    {
        let mut stream = TcpStream::connect(&prom_addr).await.unwrap();
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.ends_with("OK"));
    }

    // 2. Scrape /metrics
    {
        let mut stream = TcpStream::connect(&prom_addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.contains("velda_requests_total"));
        assert!(resp.contains("velda_connections_total"));
    }

    // Graceful shutdown
    shutdown_tx.send(true).unwrap();
    let res = supervisor_task.await.unwrap();
    assert!(res.is_ok());
}
