//! Stage 1: Pre-Sync (Candidate Configuration Acquisition & Staging).
//!
//! Responsible for acquiring raw configuration payloads from the active Provider,
//! verifying payload decompression and SHA-256 integrity, maintaining the local staging
//! buffer, applying the Autonomous LKG fallback on network disconnection, and decoding
//! the candidate `ManifestConfig`.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use sha2::{Digest, Sha256};
use tracing::warn;

use crate::provider::Provider;
use crate::provider::control_plane::decompress_payload;
use crate::{ManifestConfig, ManifestFileEntry, ManifestFiles, SyncError};

/// Pre-sync ingestion coordinator that stages raw data from providers into candidate configs.
#[derive(Debug, Clone)]
pub struct PreSyncStage {
    provider: Provider,
    staging_dir: PathBuf,
}

impl PreSyncStage {
    /// Creates a new PreSyncStage backed by a Provider and a staging directory.
    pub fn new(provider: Provider, staging_dir: impl Into<PathBuf>) -> Self {
        Self {
            provider,
            staging_dir: staging_dir.into(),
        }
    }

    /// Acquires and decodes the candidate `manifest.json`.
    pub async fn acquire_manifest(&self) -> Result<(ManifestConfig, Vec<u8>), SyncError> {
        match &self.provider {
            Provider::LocalFile(local) => self.acquire_local_manifest(local).await,
            Provider::ControlPlane(cp) => self.acquire_control_plane_manifest(cp).await,
        }
    }

    /// Reads raw bytes of a domain configuration file.
    pub async fn read_domain_file(&self, relative_path: &str) -> Result<Vec<u8>, SyncError> {
        match &self.provider {
            Provider::LocalFile(local) => local.read_file(relative_path).await,
            Provider::ControlPlane(_) => {
                let file_path = self.staging_dir.join(relative_path);
                if !file_path.exists() {
                    return Err(SyncError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!(
                            "Staged configuration file not found: {}",
                            file_path.display()
                        ),
                    )));
                }
                let bytes = fs::read(&file_path)?;
                Ok(bytes)
            }
        }
    }

    async fn acquire_local_manifest(
        &self,
        local: &crate::provider::LocalFileProvider,
    ) -> Result<(ManifestConfig, Vec<u8>), SyncError> {
        // ====================================================================
        // Phase 1: Read Raw Manifest Bytes from Local Filesystem
        // ====================================================================
        let bytes = local.read_file("manifest.json").await.map_err(|e| {
            SyncError::Manifest(format!(
                "Required manifest.json missing in configuration directory: {e}"
            ))
        })?;

        // ====================================================================
        // Phase 4: Decode Manifest & Validate Schema Version
        // ====================================================================
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

    async fn acquire_control_plane_manifest(
        &self,
        cp: &crate::provider::ControlPlaneProvider,
    ) -> Result<(ManifestConfig, Vec<u8>), SyncError> {
        fs::create_dir_all(&self.staging_dir)?;
        let manifest_path = self.staging_dir.join("manifest.json");

        // Collect current revisions from local staging if present
        let (current_manifest_rev, domain_revisions) = if manifest_path.is_file() {
            if let Ok(bytes) = fs::read(&manifest_path) {
                if let Ok(m) = serde_json::from_slice::<ManifestConfig>(&bytes) {
                    let mut revs = HashMap::new();
                    for f in &m.configuration.files {
                        if let Some(r) = f.revision {
                            revs.insert(f.name.clone(), r);
                        }
                    }
                    (m.revision, revs)
                } else {
                    (0, HashMap::new())
                }
            } else {
                (0, HashMap::new())
            }
        } else {
            (0, HashMap::new())
        };

        // If endpoint is specified, poll Control Plane over gRPC
        if !cp.endpoint().is_empty() {
            // ================================================================
            // Phase 1: Query Delta Updates from Control Plane via gRPC
            // ================================================================
            match cp.fetch_delta(current_manifest_rev, domain_revisions).await {
                Ok(Some(response)) => {
                    let mut existing_manifest = if manifest_path.is_file() {
                        let bytes = fs::read(&manifest_path)?;
                        serde_json::from_slice::<ManifestConfig>(&bytes).unwrap_or(ManifestConfig {
                            schema_version: 1,
                            revision: response.manifest_revision,
                            configuration: ManifestFiles { files: Vec::new() },
                        })
                    } else {
                        ManifestConfig {
                            schema_version: 1,
                            revision: response.manifest_revision,
                            configuration: ManifestFiles { files: Vec::new() },
                        }
                    };

                    existing_manifest.revision = response.manifest_revision;

                    for delta in response.updated_domains {
                        // ====================================================
                        // Phase 2: Payload Decompression & SHA-256 Checksum Verification
                        // ====================================================
                        let decompressed =
                            decompress_payload(&delta.compressed_json).map_err(|e| {
                                SyncError::Validation {
                                    domain: delta.domain.clone(),
                                    reason: format!("Decompression failed: {e}"),
                                }
                            })?;

                        // Verify 32-byte sha256 checksum if provided
                        if delta.sha256.len() == 32 {
                            let mut hasher = Sha256::new();
                            hasher.update(&decompressed);
                            let actual_hash: [u8; 32] = hasher.finalize().into();
                            if actual_hash != delta.sha256.as_slice() {
                                return Err(SyncError::Validation {
                                    domain: delta.domain.clone(),
                                    reason: "SHA-256 payload checksum mismatch from Control Plane"
                                        .into(),
                                });
                            }
                        }

                        // ====================================================
                        // Phase 3: Stage Domain Files to Disk & Update Staged Manifest
                        // ====================================================
                        let domain_filename = format!("{}.json", delta.domain);
                        let file_path = self.staging_dir.join(&domain_filename);
                        fs::write(&file_path, &decompressed)?;

                        if let Some(entry) = existing_manifest
                            .configuration
                            .files
                            .iter_mut()
                            .find(|f| f.name == delta.domain)
                        {
                            entry.revision = Some(delta.revision);
                            entry.path = domain_filename;
                        } else {
                            existing_manifest
                                .configuration
                                .files
                                .push(ManifestFileEntry {
                                    name: delta.domain.clone(),
                                    path: domain_filename,
                                    required: true,
                                    revision: Some(delta.revision),
                                    hash: None,
                                });
                        }
                    }

                    // Save new canonical manifest.json to staging
                    let new_manifest_bytes = serde_json::to_vec_pretty(&existing_manifest)
                        .map_err(|e| SyncError::InvalidJson {
                            domain: "manifest".into(),
                            reason: e.to_string(),
                        })?;
                    fs::write(&manifest_path, &new_manifest_bytes)?;

                    return Ok((existing_manifest, new_manifest_bytes));
                }
                Ok(None) => {
                    // UP_TO_DATE: no changes, fall through to reading existing manifest
                }
                Err(e) => {
                    // ========================================================
                    // Phase 3 (Fallback): Autonomous Fallback to Staged LKG on RPC Failure
                    // ========================================================
                    if !manifest_path.is_file() {
                        return Err(e);
                    }
                    warn!(
                        endpoint = %cp.endpoint(),
                        error = %e,
                        "Control Plane RPC failed; falling back to staged LKG"
                    );
                }
            }
        }

        // ====================================================================
        // Phase 4: Decode Candidate Manifest from Staging & Validate Schema
        // ====================================================================
        if manifest_path.is_file() {
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

            return Ok((manifest, bytes));
        }

        Err(SyncError::Manifest(format!(
            "Control Plane candidate manifest not found at {} (endpoint: {})",
            manifest_path.display(),
            cp.endpoint()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{ControlPlaneProvider, LocalFileProvider};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_pre_sync_local_manifest_acquisition() {
        let tmp = tempdir().unwrap();
        let manifest_content = r#"{
            "schema_version": 1,
            "revision": 12,
            "configuration": {
                "files": [
                    { "name": "routes", "path": "routes.json", "required": true }
                ]
            }
        }"#;
        fs::write(tmp.path().join("manifest.json"), manifest_content).unwrap();
        fs::write(tmp.path().join("routes.json"), "[]").unwrap();

        let stage = PreSyncStage::new(
            Provider::LocalFile(LocalFileProvider::new(tmp.path())),
            tmp.path().join("staging"),
        );

        let (manifest, bytes) = stage.acquire_manifest().await.unwrap();
        assert_eq!(manifest.revision, 12);
        assert!(!bytes.is_empty());

        let routes = stage.read_domain_file("routes.json").await.unwrap();
        assert_eq!(routes, b"[]");
    }

    #[tokio::test]
    async fn test_pre_sync_control_plane_staged_fallback() {
        let tmp = tempdir().unwrap();
        let staging = tmp.path().join("staging");
        fs::create_dir_all(&staging).unwrap();

        let manifest_content = r#"{
            "schema_version": 1,
            "revision": 88,
            "configuration": {
                "files": [
                    { "name": "routes", "path": "routes.json", "required": true }
                ]
            }
        }"#;
        fs::write(staging.join("manifest.json"), manifest_content).unwrap();
        fs::write(staging.join("routes.json"), "[]").unwrap();

        // Control plane with empty endpoint acts in fallback mode
        let cp = ControlPlaneProvider::new("");
        let stage = PreSyncStage::new(Provider::ControlPlane(cp), &staging);

        let (manifest, bytes) = stage.acquire_manifest().await.unwrap();
        assert_eq!(manifest.revision, 88);
        assert!(!bytes.is_empty());

        let routes = stage.read_domain_file("routes.json").await.unwrap();
        assert_eq!(routes, b"[]");
    }
}
