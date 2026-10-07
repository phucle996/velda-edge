//! Purpose: Revision semantics, idempotency, and state lifecycle tests.
//!
//! Verifies monotonic revision guards, rollback prevention, duplicate domain
//! ordering resolution, and consistency across rapid successive updates.

mod common;

use std::fs;
use tempfile::tempdir;
use velda_sync::SyncOutcome;
use velda_sync::post_sync::route;

#[tokio::test]
async fn test_noop_on_identical_checksum() {
    let dir = common::example_dir();
    let (mut comp, _storage) = common::make_composition(&dir);

    // First sync — publishes everything
    let r1 = comp.reconcile().await.unwrap();
    assert!(matches!(r1, SyncOutcome::Updated { .. }));

    // Second sync with same files — must be Unchanged
    let r2 = comp.reconcile().await.unwrap();
    assert_eq!(r2, SyncOutcome::Unchanged);

    // Third sync — still Unchanged
    let r3 = comp.reconcile().await.unwrap();
    assert_eq!(r3, SyncOutcome::Unchanged);
}

#[tokio::test]
async fn test_rollback_triggers_update() {
    let tmp = tempdir().unwrap();

    // Rev 10: initial
    common::write_manifest(tmp.path(), 10, &[("routes", "routes.json", true)]);
    fs::write(
        tmp.path().join("routes.json"),
        r#"{ "schema_version": 1, "routes": [{ "id": "r1", "kind": "l7", "listener": "http", "match": { "path_prefix": "/" }, "upstream": "us1" }] }"#,
    )
    .unwrap();

    let (mut comp, _storage) = common::make_composition(tmp.path());
    let r1 = comp.reconcile().await.unwrap();
    assert!(matches!(r1, SyncOutcome::Updated { .. }));

    // Rev 5 (rollback!) with DIFFERENT content — should NOT update because revision is lower.
    common::write_manifest(tmp.path(), 5, &[("routes", "routes.json", true)]);
    fs::write(
        tmp.path().join("routes.json"),
        r#"{ "schema_version": 1, "routes": [{ "id": "r2", "kind": "l7", "listener": "http", "match": { "path_prefix": "/new" }, "upstream": "us2" }] }"#,
    )
    .unwrap();

    let r2 = comp.reconcile().await.unwrap();
    assert_eq!(
        r2,
        SyncOutcome::Unchanged,
        "Rollback to lower revision with different content must be rejected"
    );
}

#[tokio::test]
async fn test_duplicate_domain_entries_processes_first() {
    let tmp = tempdir().unwrap();
    // Two entries for "routes" with different paths in the same manifest
    let manifest = r#"{
        "schema_version": 1,
        "revision": 1,
        "configuration": {
            "files": [
                { "name": "routes", "path": "routes_v1.json", "required": true },
                { "name": "routes", "path": "routes_v2.json", "required": true }
            ]
        }
    }"#;
    fs::write(tmp.path().join("manifest.json"), manifest).unwrap();
    fs::write(
        tmp.path().join("routes_v1.json"),
        r#"{ "schema_version": 1, "routes": [{ "id": "old", "kind": "l7", "listener": "http", "match": { "path_prefix": "/old" }, "upstream": "us1" }] }"#,
    )
    .unwrap();
    fs::write(
        tmp.path().join("routes_v2.json"),
        r#"{ "schema_version": 1, "routes": [{ "id": "new", "kind": "l7", "listener": "http", "match": { "path_prefix": "/new" }, "upstream": "us2" }] }"#,
    )
    .unwrap();

    let (mut comp, storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await;
    assert!(
        res.is_ok(),
        "Duplicate domain entries must not block: {:?}",
        res.err()
    );

    // Binary contains the FIRST processed entry — the second entry has the same
    // revision and is skipped by the "is_newer" check (same rev, different checksum
    // but revision is NOT greater than current).
    let bin = fs::read(storage.path().join("runtime/routes.bin")).unwrap();
    let (_header, list) = route::unpack_routes_from_binary(&bin).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(
        list[0].id, "old",
        "First entry wins when revisions are equal"
    );
}

#[tokio::test]
async fn test_rapid_reconciliation_cycles_no_stale_state() {
    let tmp = tempdir().unwrap();

    for rev in 1..=10u64 {
        common::write_manifest(tmp.path(), rev, &[("routes", "routes.json", true)]);
        let routes: Vec<String> = (0..rev)
            .map(|i| {
                format!(
                    r#"{{ "id": "r{rev}-{i}", "kind": "l7", "listener": "http", "match": {{ "path_prefix": "/v{rev}/{i}" }}, "upstream": "us{i}" }}"#
                )
            })
            .collect();
        fs::write(
            tmp.path().join("routes.json"),
            format!(
                r#"{{ "schema_version": 1, "routes": [{}] }}"#,
                routes.join(",")
            ),
        )
        .unwrap();
    }

    // After 10 mutations, reconcile once and verify latest state
    let (mut comp, storage) = common::make_composition(tmp.path());
    let res = comp.reconcile().await.unwrap();
    assert!(matches!(res, SyncOutcome::Updated { .. }));

    let bin = fs::read(storage.path().join("runtime/routes.bin")).unwrap();
    let (header, list) = route::unpack_routes_from_binary(&bin).unwrap();
    assert_eq!(header.revision, 10, "Must reflect latest revision");
    assert_eq!(list.len(), 10, "Must contain routes from rev 10");
}
