#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use velda_sync::SyncComposition;
use velda_sync::provider::{LocalFileProvider, Provider};

pub fn example_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("example")
}

/// Helper: creates a minimal SyncComposition backed by a temp directory provider.
pub fn make_composition(config_dir: &Path) -> (SyncComposition, tempfile::TempDir) {
    let storage = tempdir().unwrap();
    let socket = storage.path().join("test.sock");
    let provider = Provider::LocalFile(LocalFileProvider::new(config_dir));
    let comp = SyncComposition::new(provider, storage.path(), &socket);
    (comp, storage)
}

/// Helper: writes a manifest.json with given file entries.
pub fn write_manifest(dir: &Path, revision: u64, files: &[(&str, &str, bool)]) {
    let entries: Vec<String> = files
        .iter()
        .map(|(name, path, required)| {
            format!(r#"{{ "name": "{name}", "path": "{path}", "required": {required} }}"#)
        })
        .collect();
    let manifest = format!(
        r#"{{
  "schema_version": 1,
  "revision": {revision},
  "configuration": {{
    "files": [{entries}]
  }}
}}"#,
        entries = entries.join(",\n      ")
    );
    fs::write(dir.join("manifest.json"), manifest).unwrap();
}
