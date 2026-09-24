//! Purpose: Robustness tests against corrupted, malformed, or degenerate payload inputs.
//!
//! Verifies that invalid JSON, corrupt byte sequences, and deep nesting
//! return proper validation errors without blocking, hanging, or panicking.

mod common;

use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_truncated_json_does_not_block() {
    let tmp = tempdir().unwrap();
    common::write_manifest(tmp.path(), 1, &[("routes", "routes.json", true)]);
    // Truncated JSON: missing closing brackets
    fs::write(
        tmp.path().join("routes.json"),
        r#"{ "routes": [ { "id": "r1" "#,
    )
    .unwrap();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(res.is_err(), "Truncated JSON must return error, not block");
}

#[tokio::test]
async fn test_invalid_utf8_does_not_block() {
    let tmp = tempdir().unwrap();
    common::write_manifest(tmp.path(), 1, &[("routes", "routes.json", true)]);
    // Binary garbage — invalid UTF-8 sequence
    fs::write(
        tmp.path().join("routes.json"),
        [0xFF, 0xFE, 0x00, 0x80, 0xC0, 0xAF],
    )
    .unwrap();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(res.is_err(), "Invalid UTF-8 must return error, not block");
}

#[tokio::test]
async fn test_extremely_deep_nesting_does_not_block() {
    let tmp = tempdir().unwrap();
    common::write_manifest(tmp.path(), 1, &[("routes", "routes.json", true)]);
    // 500 levels of nesting
    let deep = "{".repeat(500) + &"}".repeat(500);
    fs::write(tmp.path().join("routes.json"), deep).unwrap();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(res.is_err(), "Deep nesting must return error, not block");
}
