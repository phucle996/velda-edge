//! Purpose: Security boundaries against malicious path manipulation.
//!
//! Verifies that attempts to break out of the configuration root directory
//! via path traversal (`../`) or absolute paths (`/etc/...`) are rejected.

mod common;

use tempfile::tempdir;

#[tokio::test]
async fn test_path_traversal_in_manifest_path() {
    let tmp = tempdir().unwrap();
    // Hostile input: try to escape config root via relative path traversal
    common::write_manifest(tmp.path(), 1, &[("routes", "../../../etc/passwd", true)]);

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(res.is_err(), "Path traversal must not succeed");
}

#[tokio::test]
async fn test_absolute_path_in_manifest() {
    let tmp = tempdir().unwrap();
    // Hostile input: attempt to target absolute system path
    common::write_manifest(tmp.path(), 1, &[("routes", "/etc/shadow", true)]);

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(res.is_err(), "Absolute path in manifest must fail");
}
