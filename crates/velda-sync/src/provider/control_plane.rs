//! Remote Control Plane configuration provider.
//!
//! Connects to the centralized Control Plane via gRPC to acquire desired configuration state.
//! Supports TLS / mTLS with automatic SAN extraction and verification.

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};

use crate::{ManifestConfig, ManifestFileEntry, ManifestFiles, SyncError};

#[allow(clippy::all)]
pub mod proto {
    tonic::include_proto!("sync.v1");
}

use proto::control_plane_sync_service_client::ControlPlaneSyncServiceClient;
use proto::{FetchDeltaRequest, FetchDeltaResponse, SyncStatus};

/// Decompresses binary payload if Gzip-compressed (magic 0x1F, 0x8B), otherwise returns raw bytes.
pub fn decompress_payload(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    } else {
        Ok(data.to_vec())
    }
}

/// Remote Control Plane configuration provider (Control Plane Mode).
#[derive(Debug, Clone)]
pub struct ControlPlaneProvider {
    endpoint: String,
    timeout: Duration,
    ca_cert_path: Option<PathBuf>,
    client_cert_path: Option<PathBuf>,
    client_key_path: Option<PathBuf>,
    staging_dir: PathBuf,
}

impl ControlPlaneProvider {
    /// Creates a new provider pointing to the specified Control Plane endpoint and staging directory.
    pub fn new(endpoint: impl Into<String>, staging_dir: impl Into<PathBuf>) -> Self {
        Self {
            endpoint: endpoint.into(),
            timeout: Duration::from_secs(10),
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            staging_dir: staging_dir.into(),
        }
    }

    /// Sets a custom timeout for Control Plane network operations.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Configures TLS root CA for verifying Control Plane server authenticity.
    pub fn with_tls(mut self, ca_cert_path: impl Into<PathBuf>) -> Self {
        self.ca_cert_path = Some(ca_cert_path.into());
        self
    }

    /// Configures mutual TLS (mTLS) with client certificate and private key.
    pub fn with_mtls(
        mut self,
        ca_cert_path: impl Into<PathBuf>,
        client_cert_path: impl Into<PathBuf>,
        client_key_path: impl Into<PathBuf>,
    ) -> Self {
        self.ca_cert_path = Some(ca_cert_path.into());
        self.client_cert_path = Some(client_cert_path.into());
        self.client_key_path = Some(client_key_path.into());
        self
    }

    /// Target Control Plane endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Configured timeout duration for Control Plane RPC operations.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Staging directory where pulled candidate configurations are saved locally.
    pub fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    /// Connects to the Control Plane gRPC endpoint with automatic SAN validation.
    pub async fn connect_channel(&self) -> Result<tonic::transport::Channel, SyncError> {
        let uri: tonic::transport::Uri = self.endpoint.parse().map_err(|e| {
            SyncError::Manifest(format!(
                "Invalid Control Plane endpoint URI '{}': {e}",
                self.endpoint
            ))
        })?;

        let mut endpoint = tonic::transport::Endpoint::from(uri.clone())
            .timeout(self.timeout)
            .connect_timeout(self.timeout);

        if uri.scheme_str() == Some("https") {
            let mut tls_config = tonic::transport::ClientTlsConfig::new();

            if let Some(ca_path) = &self.ca_cert_path {
                let ca_pem = fs::read(ca_path)?;
                let ca_cert = tonic::transport::Certificate::from_pem(ca_pem);
                tls_config = tls_config.ca_certificate(ca_cert);
            }

            if let (Some(cert_path), Some(key_path)) =
                (&self.client_cert_path, &self.client_key_path)
            {
                let cert_pem = fs::read(cert_path)?;
                let key_pem = fs::read(key_path)?;
                let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
                tls_config = tls_config.identity(identity);
            }

            // Rustls/Tonic automatically extracts hostname from URI and matches against Server Cert SAN.
            endpoint = endpoint.tls_config(tls_config).map_err(|e| {
                SyncError::Manifest(format!("Failed to configure TLS for Control Plane: {e}"))
            })?;
        }

        endpoint.connect().await.map_err(|e| {
            SyncError::Manifest(format!(
                "Failed to connect to Control Plane at '{}': {e}",
                self.endpoint
            ))
        })
    }

    /// Executes Unary Polling RPC (`FetchDelta`) to retrieve updated domains from Control Plane.
    pub async fn fetch_delta(
        &self,
        manifest_revision: u64,
        domain_revisions: HashMap<String, u64>,
    ) -> Result<Option<FetchDeltaResponse>, SyncError> {
        let channel = self.connect_channel().await?;
        let mut client = ControlPlaneSyncServiceClient::new(channel);

        let request = FetchDeltaRequest {
            manifest_revision,
            domain_revisions,
        };

        let response = client
            .fetch_delta(request)
            .await
            .map_err(|status| SyncError::Manifest(format!("gRPC FetchDelta failed: {status}")))?
            .into_inner();

        if response.status == SyncStatus::UpToDate as i32 {
            return Ok(None);
        }

        Ok(Some(response))
    }

    /// Reads candidate `manifest.json` from staging directory, synchronizing via gRPC if needed.
    pub async fn read_manifest(&self) -> Result<(ManifestConfig, Vec<u8>), SyncError> {
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
        if !self.endpoint.is_empty() {
            match self
                .fetch_delta(current_manifest_rev, domain_revisions)
                .await
            {
                Ok(Some(response)) => {
                    // Control Plane provided modified domains
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

                        let domain_filename = format!("{}.json", delta.domain);
                        let file_path = self.staging_dir.join(&domain_filename);
                        fs::write(&file_path, &decompressed)?;

                        // Update file entry in manifest
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
                    // Autonomous Invariant: If remote CP fails but local staged manifest exists, use local LKG
                    if !manifest_path.is_file() {
                        return Err(e);
                    }
                    eprintln!(
                        "[velda-sync] Warning: Control Plane RPC failed ({e}); falling back to staged LKG"
                    );
                }
            }
        }

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
            self.endpoint
        )))
    }

    /// Reads raw bytes of a candidate configuration file staged from the Control Plane.
    pub async fn read_file(&self, relative_path: &str) -> Result<Vec<u8>, SyncError> {
        let file_path = self.staging_dir.join(relative_path);
        if !file_path.exists() {
            return Err(SyncError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "Control Plane staged configuration file not found: {}",
                    file_path.display()
                ),
            )));
        }

        let bytes = fs::read(&file_path)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_control_plane_provider_staging_fallback() {
        let tmp = tempdir().unwrap();
        let staging = tmp.path().join("staging");
        fs::create_dir_all(&staging).unwrap();

        let manifest_content = r#"{
            "schema_version": 1,
            "revision": 99,
            "configuration": {
                "files": [
                    { "name": "routes", "path": "routes.json", "required": true }
                ]
            }
        }"#;
        fs::write(staging.join("manifest.json"), manifest_content).unwrap();
        fs::write(staging.join("routes.json"), "[]").unwrap();

        // Control plane with empty endpoint acts in local-staged fallback mode
        let cp = ControlPlaneProvider::new("", &staging);
        let (manifest, bytes) = cp.read_manifest().await.unwrap();
        assert_eq!(manifest.revision, 99);
        assert!(!bytes.is_empty());

        let routes_bytes = cp.read_file("routes.json").await.unwrap();
        assert_eq!(routes_bytes, b"[]");
    }

    #[test]
    fn test_decompress_payload_raw_and_gzip() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let raw = b"{\"routes\": []}";
        let decompressed_raw = decompress_payload(raw).unwrap();
        assert_eq!(decompressed_raw, raw);

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(raw).unwrap();
        let compressed = encoder.finish().unwrap();

        let decompressed_gzip = decompress_payload(&compressed).unwrap();
        assert_eq!(decompressed_gzip, raw);
    }
}
