//! Purpose: Zero-entity configuration tests.
//!
//! Validates that domain files with valid schema version but empty entity arrays `[]`
//! succeed and compile into valid binary artifacts with length 0.

mod common;

use std::fs;
use tempfile::tempdir;
use velda_sync::post_sync::{listener, route, upstream};

#[tokio::test]
async fn test_empty_domains_compile() {
    let tmp = tempdir().unwrap();
    common::write_manifest(
        tmp.path(),
        1,
        &[
            ("routes", "routes.json", true),
            ("listeners", "listeners.json", true),
            ("upstreams", "upstreams.json", true),
        ],
    );
    fs::write(
        tmp.path().join("routes.json"),
        r#"{ "schema_version": 1, "routes": [] }"#,
    )
    .unwrap();
    fs::write(
        tmp.path().join("listeners.json"),
        r#"{ "schema_version": 1, "listeners": [] }"#,
    )
    .unwrap();
    fs::write(
        tmp.path().join("upstreams.json"),
        r#"{ "schema_version": 1, "upstreams": [] }"#,
    )
    .unwrap();

    let (mut comp, storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(
        res.is_ok(),
        "Empty domain collections must succeed: {:?}",
        res.err()
    );

    // Verify binary artifacts exist and unpack to length 0
    let routes_bin = storage.path().join("runtime/routes.bin");
    assert!(routes_bin.exists());
    let (_h, routes) = route::unpack_routes_from_binary(&fs::read(routes_bin).unwrap()).unwrap();
    assert_eq!(routes.len(), 0);

    let listeners_bin = storage.path().join("runtime/listeners.bin");
    assert!(listeners_bin.exists());
    let (_h, listeners) =
        listener::unpack_listeners_from_binary(&fs::read(listeners_bin).unwrap()).unwrap();
    assert_eq!(listeners.len(), 0);

    let upstreams_bin = storage.path().join("runtime/upstreams.bin");
    assert!(upstreams_bin.exists());
    let (_h, upstreams) =
        upstream::unpack_upstreams_from_binary(&fs::read(upstreams_bin).unwrap()).unwrap();
    assert_eq!(upstreams.len(), 0);
}
