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

    // Omitted fields cleanly populated from hardware tier fallback
    assert!(profile.discovery.max_lkg_capacity > 0);
    assert!(profile.discovery.max_negative_ttl_secs >= profile.discovery.negative_ttl_secs);
    assert!(profile.discovery.query_timeout_ms >= 1000);
    assert!(profile.transport.io_workers >= 1);

    // File was updated with complete merged snapshot
    let updated_content = fs::read_to_string(runtime_dir.join("runtime.json")).unwrap();
    let parsed: RuntimeProfile = serde_json::from_str(&updated_content).unwrap();
    assert_eq!(parsed.discovery.max_dns_cache_capacity, 88_888);
}
