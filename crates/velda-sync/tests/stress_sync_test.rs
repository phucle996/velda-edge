//! Purpose: High-volume stress and throughput tests.
//!
//! Verifies that processing large configuration batches (e.g. 1000 routes)
//! completes within strict time limits without memory exhaustion or blocking.

mod common;

use std::fs;
use tempfile::tempdir;
use velda_sync::post_sync::route;

#[tokio::test]
async fn test_large_routes_payload_does_not_block() {
    let tmp = tempdir().unwrap();
    common::write_manifest(tmp.path(), 1, &[("routes", "routes.json", true)]);

    // Generate 1000 routes
    let routes: Vec<String> = (0..1000)
        .map(|i| {
            format!(
                r#"{{ "id": "route-{i}", "kind": "l7", "listener": "http", "match": {{ "path_prefix": "/svc-{i}" }}, "upstream": "us-{i}" }}"#
            )
        })
        .collect();
    let payload = format!(
        r#"{{ "schema_version": 1, "routes": [{}] }}"#,
        routes.join(",")
    );
    fs::write(tmp.path().join("routes.json"), &payload).unwrap();

    let (mut comp, storage) = common::make_composition(tmp.path());
    let start = std::time::Instant::now();
    let res = comp.reconcile().await;
    let elapsed = start.elapsed();

    assert!(res.is_ok(), "1000 routes must succeed: {:?}", res.err());
    assert!(
        elapsed.as_secs() < 5,
        "1000 routes must complete in <5s, took {:?}",
        elapsed
    );

    // Verify binary roundtrip and count
    let bin = fs::read(storage.path().join("runtime/routes.bin")).unwrap();
    let (_header, list) = route::unpack_routes_from_binary(&bin).unwrap();
    assert_eq!(list.len(), 1000);
}
