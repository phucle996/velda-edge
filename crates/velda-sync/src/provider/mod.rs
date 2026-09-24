//! Configuration Acquisition Providers.
//!
//! Owns acquiring raw configuration artifacts (`manifest.json` and domain files)
//! from either local filesystem (`LocalFileProvider`) or remote Control Plane
//! (`ControlPlaneProvider`).

pub mod control_plane;
pub mod local_file;

pub use control_plane::ControlPlaneProvider;
pub use local_file::LocalFileProvider;

use crate::{ManifestConfig, SyncError};

/// Operating mode for configuration acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Standalone mode: reads configuration files from local disk.
    Standalone,
    /// Control Plane mode: fetches candidate configuration from remote Control Plane.
    ControlPlane,
}

/// Unified provider abstraction (enum-based for zero dynamic dispatch and zero heap allocation).
#[derive(Debug, Clone)]
pub enum Provider {
    LocalFile(LocalFileProvider),
    ControlPlane(ControlPlaneProvider),
}

impl Provider {
    /// Reads and parses `manifest.json` along with its raw payload from the active source.
    pub async fn read_manifest(&self) -> Result<(ManifestConfig, Vec<u8>), SyncError> {
        match self {
            Self::LocalFile(p) => p.read_manifest().await,
            Self::ControlPlane(p) => p.read_manifest().await,
        }
    }

    /// Reads raw bytes of a domain configuration file from the active source.
    pub async fn read_file(&self, relative_path: &str) -> Result<Vec<u8>, SyncError> {
        match self {
            Self::LocalFile(p) => p.read_file(relative_path).await,
            Self::ControlPlane(p) => p.read_file(relative_path).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_local_file_provider_reads_manifest_and_files() {
        let tmp = tempdir().unwrap();
        let manifest_content = r#"{
            "schema_version": 1,
            "revision": 7,
            "configuration": {
                "files": [
                    { "name": "routes", "path": "routes.json", "required": true }
                ]
            }
        }"#;

        std::fs::write(tmp.path().join("manifest.json"), manifest_content).unwrap();
        std::fs::write(tmp.path().join("routes.json"), "[]").unwrap();

        let provider = Provider::LocalFile(LocalFileProvider::new(tmp.path()));

        let (manifest, bytes) = provider.read_manifest().await.unwrap();
        assert_eq!(manifest.revision, 7);
        assert!(!bytes.is_empty());

        let file_bytes = provider.read_file("routes.json").await.unwrap();
        assert_eq!(file_bytes, b"[]");
    }

    #[tokio::test]
    async fn test_control_plane_provider_staging() {
        let tmp = tempdir().unwrap();
        let manifest_content = r#"{
            "schema_version": 1,
            "revision": 99,
            "configuration": {
                "files": []
            }
        }"#;

        std::fs::write(tmp.path().join("manifest.json"), manifest_content).unwrap();

        let cp_provider = ControlPlaneProvider::new("http://localhost:8080", tmp.path());
        assert_eq!(cp_provider.endpoint(), "http://localhost:8080");

        let provider = Provider::ControlPlane(cp_provider);
        let (manifest, _) = provider.read_manifest().await.unwrap();
        assert_eq!(manifest.revision, 99);
    }
}
