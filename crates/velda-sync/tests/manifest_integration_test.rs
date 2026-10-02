//! Purpose: End-to-End happy path reconciliation integration tests.
//!
//! Validates the full synchronization lifecycle: multi-domain manifest parsing,
//! binary compilation, on-disk artifact layout, LKG manifest persistence,
//! and idempotent re-sync behavior.

mod common;

use std::fs;
use tempfile::tempdir;
use velda_sync::post_sync::{listener, plugin, route, tls, upstream};
use velda_sync::provider::{LocalFileProvider, Provider};
use velda_sync::{SyncComposition, SyncOutcome};

#[tokio::test]
async fn test_end_to_end_manifest_sync_composition() {
    let dir = common::example_dir();
    assert!(dir.exists(), "Example directory must exist at {:?}", dir);

    let storage_dir = tempdir().unwrap();
    let socket_path = storage_dir.path().join("edge_test.sock");

    let provider = Provider::LocalFile(LocalFileProvider::new(&dir));
    let mut composition = SyncComposition::new(provider, storage_dir.path(), &socket_path);

    // 1. Initial reconciliation pass publishes all domains
    let outcome = composition
        .reconcile()
        .await
        .expect("Initial reconciliation must succeed");

    let expected_domains = vec![
        "listeners".to_string(),
        "routes".to_string(),
        "upstreams".to_string(),
        "plugins".to_string(),
        "tls".to_string(),
    ];

    match outcome {
        SyncOutcome::Updated { changed_domains } => {
            assert_eq!(changed_domains, expected_domains);
        }
        SyncOutcome::Unchanged => panic!("First sync must publish updates"),
    }

    // 2. Verify LKG manifest persistence
    let manifest_lkg = storage_dir.path().join("config/manifest.json");
    assert!(
        manifest_lkg.exists(),
        "manifest.json must be persisted to LKG"
    );

    // 3. Verify per-domain binary & JSON artifacts and unpack verification
    // Routes
    let routes_bin = storage_dir.path().join("runtime/routes.bin");
    assert!(routes_bin.exists());
    let routes_bytes = fs::read(&routes_bin).unwrap();
    let (r_header, r_list) = route::unpack_routes_from_binary(&routes_bytes).unwrap();
    assert_eq!(r_header.revision, 42);
    assert_eq!(r_list.len(), 5);

    // Upstreams
    let upstreams_bin = storage_dir.path().join("runtime/upstreams.bin");
    assert!(upstreams_bin.exists());
    let upstreams_bytes = fs::read(&upstreams_bin).unwrap();
    let (u_header, u_list) = upstream::unpack_upstreams_from_binary(&upstreams_bytes).unwrap();
    assert_eq!(u_header.revision, 42);
    assert_eq!(u_list.len(), 5);

    // Listeners
    let listeners_bin = storage_dir.path().join("runtime/listeners.bin");
    assert!(listeners_bin.exists());
    let listeners_bytes = fs::read(&listeners_bin).unwrap();
    let (l_header, l_list) = listener::unpack_listeners_from_binary(&listeners_bytes).unwrap();
    assert_eq!(l_header.revision, 42);
    assert_eq!(l_list.len(), 5);

    // Plugins
    let plugins_bin = storage_dir.path().join("runtime/plugins.bin");
    assert!(plugins_bin.exists());
    let plugins_bytes = fs::read(&plugins_bin).unwrap();
    let (p_header, p_list) = plugin::unpack_plugins_from_binary(&plugins_bytes).unwrap();
    assert_eq!(p_header.revision, 42);
    assert_eq!(p_list.len(), 10);

    // TLS
    let tls_bin = storage_dir.path().join("runtime/tls.bin");
    assert!(tls_bin.exists());
    let tls_bytes = fs::read(&tls_bin).unwrap();
    let (t_header, t_file) = tls::unpack_tls_from_binary(&tls_bytes).unwrap();
    assert_eq!(t_header.revision, 42);
    assert_eq!(t_file.len(), 2);

    // 4. Second sync with identical content must be a no-op
    let second_run = composition
        .reconcile()
        .await
        .expect("Second reconciliation must succeed");
    assert_eq!(
        second_run,
        SyncOutcome::Unchanged,
        "Subsequent reconciliation without changes must be Unchanged"
    );
}
