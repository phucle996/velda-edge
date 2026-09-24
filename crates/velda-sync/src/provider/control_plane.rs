//! Pure Remote Control Plane Configuration Provider.
//!
//! Provides gRPC network transport and delta polling capabilities.
//! Free of local filesystem staging, file mutations, or JSON decoding.

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

use flate2::read::GzDecoder;

use crate::SyncError;

#[allow(clippy::all)]
pub mod proto {
    tonic::include_proto!("sync.v1");
}

use proto::control_plane_sync_service_client::ControlPlaneSyncServiceClient;
pub use proto::{DomainDelta, FetchDeltaRequest, FetchDeltaResponse, SyncStatus};

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

/// Remote Control Plane gRPC transport provider.
#[derive(Debug, Clone)]
pub struct ControlPlaneProvider {
    endpoint: String,
    timeout: Duration,
    ca_cert_path: Option<PathBuf>,
    client_cert_path: Option<PathBuf>,
    client_key_path: Option<PathBuf>,
}

impl ControlPlaneProvider {
    /// Creates a new provider pointing to the specified Control Plane gRPC endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            timeout: Duration::from_secs(10),
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
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

    /// Connects to the Control Plane gRPC endpoint with automatic TLS / SAN validation.
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
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_control_plane_provider_config() {
        let cp = ControlPlaneProvider::new("https://cp.local:8443")
            .with_timeout(Duration::from_millis(5000));
        assert_eq!(cp.endpoint(), "https://cp.local:8443");
        assert_eq!(cp.timeout, Duration::from_millis(5000));
    }
}
