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
        id: "prometheus_exporter".into(),
        phase: "background".into(),
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
            "cors": { "enabled": false, "allow_origins": [], "allow_methods": [], "allow_headers": [] },
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

#[test]
fn test_lkg_json_fallback_on_old_compiler_version() {
    let tmp = tempdir().unwrap();
    let storage_dir = tmp.path();
    let runtime_dir = storage_dir.join("runtime");
    let config_dir = storage_dir.join("config");
    fs::create_dir_all(&runtime_dir).unwrap();
    fs::create_dir_all(&config_dir).unwrap();

    // Write valid LKG listeners.json containing observability sampling
    let json = r#"{
        "schema_version": 1,
        "listeners": [{
            "id": "web-lkg",
            "address": "127.0.0.1:61555",
            "transport": { "protocol": "tcp" },
            "application": { "protocol": "http1", "streaming": [] },
            "tls": { "enabled": false },
            "observability": {
                "sampling": { "http": { "2xx": 0.01, "shed": 0.01 } }
            }
        }]
    }"#;
    fs::write(config_dir.join("listeners.json"), json).unwrap();

    // Write an incompatible legacy listeners.bin (corrupt compiler_version = 1)
    let listeners = velda_sync::post_sync::listener::parse_listeners(json.as_bytes()).unwrap();
    let mut bin =
        velda_sync::post_sync::listener::compile_listeners_to_binary(&listeners, 1, [0u8; 32])
            .unwrap();
    bin[8..12].copy_from_slice(&1u32.to_le_bytes());
    fs::write(runtime_dir.join("listeners.bin"), &bin).unwrap();

    let loaded = velda_edge::config::load_listeners(&runtime_dir)
        .expect("Must recover from config/listeners.json when binary compiler_version mismatches");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].id, "web-lkg");
    assert!(
        loaded[0]
            .observability
            .as_ref()
            .and_then(|o| o.sampling)
            .and_then(|s| s.http)
            .is_some()
    );
}

#[tokio::test]
async fn test_http1_unified_log_trace_sampling_and_exact_metrics() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use velda_core::StreamingMode;
    use velda_http1::Http1Config;
    use velda_observability::sampling::http::HttpSampling;
    use velda_observability::{global_metrics, global_telemetry_store, set_current_worker_lane};
    use velda_sock::Connection;

    // Seed lane and sampler state on the current_thread tokio test worker
    set_current_worker_lane(0);
    velda_observability::sampling::state::seed_worker(0);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let logs_before = global_telemetry_store().emitted_total();
    let reqs4xx_before: u64 = global_metrics()
        .snapshot()
        .pipelines
        .iter()
        .map(|p| p.requests_4xx)
        .sum();

    // Drop all 4xx (unmatched -> 404) on one stream, keep all 4xx on another, verifying
    // unified TelemetryRecord emission (single store feeding logs + traces) while metrics always counts 100%.
    let (client_res, accept_res) =
        tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept());
    let mut client = client_res.unwrap();
    let (server_drop, _) = accept_res.unwrap();
    let conn_drop = Connection::from_stream(server_drop).unwrap();
    let rt_shared = velda_edge::runtime::new_shared_runtime(velda_edge::Runtime::empty());

    let drop_sampling = HttpSampling::from_rates(0.0, 0.0, 0.0, 0.0, 1.0, Some(0.0));
    let server_task = tokio::spawn(velda_edge::pipeline::http1::handle_http1_stream(
        conn_drop,
        "test-drop".into(),
        Http1Config::default(),
        false,
        StreamingMode::DISABLED,
        None,
        drop_sampling,
        rt_shared.clone(),
    ));

    client
        .write_all(b"GET /unmatched HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    let _ = client.read_to_end(&mut buf).await;
    server_task.await.unwrap();

    assert_eq!(
        global_telemetry_store().emitted_total() - logs_before,
        0,
        "dropped 404 must not emit unified TelemetryRecord"
    );
    let reqs4xx_after_drop: u64 = global_metrics()
        .snapshot()
        .pipelines
        .iter()
        .map(|p| p.requests_4xx)
        .sum();
    assert_eq!(
        reqs4xx_after_drop - reqs4xx_before,
        1,
        "metrics must still record 100% of dropped requests"
    );

    // Now test keep_all policy: single unified TelemetryRecord emitted (+1), Metrics +1.
    let (client_res2, accept_res2) =
        tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept());
    let mut client2 = client_res2.unwrap();
    let (server_keep, _) = accept_res2.unwrap();
    let conn_keep = Connection::from_stream(server_keep).unwrap();
    let server_task2 = tokio::spawn(velda_edge::pipeline::http1::handle_http1_stream(
        conn_keep,
        "test-keep".into(),
        Http1Config::default(),
        false,
        StreamingMode::DISABLED,
        None,
        HttpSampling::keep_all(),
        rt_shared,
    ));

    client2
        .write_all(b"GET /unmatched HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf2 = Vec::new();
    let _ = client2.read_to_end(&mut buf2).await;
    server_task2.await.unwrap();

    assert_eq!(global_telemetry_store().emitted_total() - logs_before, 1);
    let reqs4xx_after_keep: u64 = global_metrics()
        .snapshot()
        .pipelines
        .iter()
        .map(|p| p.requests_4xx)
        .sum();
    assert_eq!(reqs4xx_after_keep - reqs4xx_before, 2);
}
