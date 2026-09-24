//! Local filesystem configuration provider.
//!
//! Owns reading `manifest.json` and domain configuration files from a local directory.

use std::fs;
use std::path::{Path, PathBuf};

use crate::{ManifestConfig, SyncError};

/// Local filesystem configuration provider (Standalone Mode).
#[derive(Debug, Clone)]
pub struct LocalFileProvider {
    config_dir: PathBuf,
}

impl LocalFileProvider {
    /// Creates a new provider pointing to the specified directory.
    pub fn new(config_dir: impl Into<PathBuf>) -> Self {
        Self {
            config_dir: config_dir.into(),
        }
    }

    /// Path to the configuration directory.
    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    /// Reads and parses `manifest.json` from the configuration directory.
    pub async fn read_manifest(&self) -> Result<(ManifestConfig, Vec<u8>), SyncError> {
        let manifest_path = self.config_dir.join("manifest.json");
        if !manifest_path.is_file() {
            return Err(SyncError::Manifest(format!(
                "Required manifest.json missing in configuration directory: {}",
                self.config_dir.display()
            )));
        }

        let bytes = fs::read(&manifest_path)?;
        let manifest: ManifestConfig =
            serde_json::from_slice(&bytes).map_err(|e| SyncError::InvalidJson {
                domain: "manifest".into(),
                reason: e.to_string(),
            })?;

        if manifest.schema_version != 1 {
            return Err(SyncError::Manifest(format!(
                "Unsupported manifest schema_version {}; expected 1",
                manifest.schema_version
            )));
        }

        Ok((manifest, bytes))
    }

    /// Reads raw bytes of a domain configuration file relative to the configuration directory.
    pub async fn read_file(&self, relative_path: &str) -> Result<Vec<u8>, SyncError> {
        let file_path = self.config_dir.join(relative_path);
        if !file_path.exists() {
            return Err(SyncError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Configuration file not found: {}", file_path.display()),
            )));
        }

        let bytes = fs::read(&file_path)?;
        Ok(bytes)
    }
}
