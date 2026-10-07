//! Purpose: Manifest schema, structure, domain registration & presence validation tests.
//!
//! Validates how the sync pipeline handles missing manifest files, corrupted manifest JSON,
//! missing required domain configuration, unknown domains, and gracefully skipped optional files.

mod common;

use std::fs;
use tempfile::tempdir;
use velda_sync::{SyncError, SyncOutcome};

#[tokio::test]
async fn test_missing_manifest_fails() {
    let tmp = tempdir().unwrap();
    let (mut comp, _storage) = common::make_composition(tmp.path());

    let res = comp.reconcile().await;
    assert!(
        matches!(res, Err(SyncError::Manifest(_))),
        "Missing manifest must return Manifest error: {:?}",
        res
    );
}

#[tokio::test]
async fn test_manifest_invalid_json_returns_error() {
    let tmp = tempdir().unwrap();
    fs::write(tmp.path().join("manifest.json"), "NOT VALID JSON {{{").unwrap();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(res.is_err(), "Invalid manifest JSON must fail");
}

#[tokio::test]
async fn test_manifest_empty_file_returns_error() {
    let tmp = tempdir().unwrap();
    fs::write(tmp.path().join("manifest.json"), "").unwrap();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(res.is_err(), "Empty manifest must fail");
}

#[tokio::test]
async fn test_manifest_missing_configuration_field() {
    let tmp = tempdir().unwrap();
    fs::write(
        tmp.path().join("manifest.json"),
        r#"{ "schema_version": 1, "revision": 1 }"#,
    )
    .unwrap();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(
        res.is_err(),
        "Manifest without 'configuration' field must fail"
    );
}

#[tokio::test]
async fn test_missing_required_file_in_manifest_fails() {
    let tmp = tempdir().unwrap();
    common::write_manifest(
        tmp.path(),
        1,
        &[
            ("routes", "routes.json", true),
            ("listeners", "listeners.json", true),
        ],
    );
    // Write listeners.json but omit routes.json
    fs::write(
        tmp.path().join("listeners.json"),
        r#"{ "schema_version": 1, "listeners": [] }"#,
    )
    .unwrap();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(
        matches!(res, Err(SyncError::Manifest(_))),
        "Missing required domain file must return Manifest error: {:?}",
        res
    );
}

#[tokio::test]
async fn test_manifest_with_no_files_is_unchanged() {
    let tmp = tempdir().unwrap();
    common::write_manifest(tmp.path(), 1, &[]);

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await.unwrap();
    assert_eq!(
        res,
        SyncOutcome::Unchanged,
        "Manifest with zero files must be Unchanged"
    );
}

#[tokio::test]
async fn test_unknown_domain_name_returns_error() {
    let tmp = tempdir().unwrap();
    common::write_manifest(tmp.path(), 1, &[("firewalls", "firewalls.json", true)]);
    fs::write(tmp.path().join("firewalls.json"), r#"{ "firewalls": [] }"#).unwrap();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(
        matches!(&res, Err(SyncError::Manifest(msg)) if msg.contains("Unsupported domain")),
        "Unknown domain must produce Manifest error: {:?}",
        res
    );
}

#[tokio::test]
async fn test_missing_optional_domain_skipped() {
    let tmp = tempdir().unwrap();
    common::write_manifest(
        tmp.path(),
        1,
        &[
            ("routes", "routes.json", true),
            ("tls", "tls.json", false), // optional, file does not exist
        ],
    );
    fs::write(
        tmp.path().join("routes.json"),
        r#"{ "schema_version": 1, "routes": [] }"#,
    )
    .unwrap();
    // tls.json intentionally NOT created

    let (mut comp, storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(
        res.is_ok(),
        "Optional missing file must be skipped: {:?}",
        res.err()
    );

    // routes.bin must exist, tls.bin must NOT exist
    assert!(storage.path().join("runtime/routes.bin").exists());
    assert!(!storage.path().join("runtime/tls.bin").exists());
}
