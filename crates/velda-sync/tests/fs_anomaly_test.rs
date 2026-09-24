//! Purpose: Filesystem anomalies and partial failure behavior tests.
//!
//! Verifies pipeline resilience against non-file directory targets,
//! symlink recursion loops, and atomic cycle aborts when any required domain fails.

mod common;

use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_domain_path_is_directory_returns_error() {
    let tmp = tempdir().unwrap();
    common::write_manifest(tmp.path(), 1, &[("routes", "routes.json", true)]);
    // Create a directory instead of a regular file
    fs::create_dir_all(tmp.path().join("routes.json")).unwrap();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(res.is_err(), "Directory instead of file must fail");
}

#[cfg(unix)]
#[tokio::test]
async fn test_symlink_loop_does_not_block() {
    let tmp = tempdir().unwrap();
    common::write_manifest(tmp.path(), 1, &[("routes", "routes.json", true)]);

    // Create a recursive symlink loop: routes.json -> routes.json
    let link_path = tmp.path().join("routes.json");
    std::os::unix::fs::symlink(&link_path, &link_path).ok();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(res.is_err(), "Symlink loop must return error, not hang");
}

#[tokio::test]
async fn test_partial_domain_failure_aborts_entire_cycle() {
    let tmp = tempdir().unwrap();
    common::write_manifest(
        tmp.path(),
        1,
        &[
            ("listeners", "listeners.json", true),
            ("routes", "routes.json", true),
        ],
    );

    // listeners: valid payload
    fs::write(tmp.path().join("listeners.json"), r#"{ "listeners": [] }"#).unwrap();
    // routes: corrupted JSON payload
    fs::write(tmp.path().join("routes.json"), "CORRUPTED!!!").unwrap();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;

    // The pipeline processes domains sequentially; once routes fails, it aborts.
    // The overall cycle must return Err so the caller knows the sync was incomplete.
    assert!(
        res.is_err(),
        "Pipeline must abort when a required domain fails: {:?}",
        res
    );
}
