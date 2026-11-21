//! S3-Compatible Object Storage Log Shipper Configuration.

use serde::{Deserialize, Serialize};

use crate::config::OutboundTlsConfig;

/// Addressing style for S3-compatible endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum S3AddressingStyle {
    /// `http://endpoint/bucket/key` (MinIO, Ceph, local S3).
    #[default]
    Path,
    /// `https://bucket.s3.region.amazonaws.com/key` (AWS S3).
    VirtualHosted,
}

/// Object compression format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum S3Compression {
    #[default]
    Zstd,
    Gzip,
    None,
}

/// Validated configuration for [`super::S3LogShipperPlugin`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct S3LogShipperConfig {
    /// S3 endpoint URL (e.g. `"http://127.0.0.1:9000"` or `"https://s3.us-east-1.amazonaws.com"`).
    pub endpoint: String,
    /// Target bucket name.
    pub bucket: String,
    /// AWS region (e.g. `"us-east-1"`).
    pub region: String,
    /// Key prefix inside the bucket (e.g. `"velda-logs"`).
    pub prefix: String,
    /// Addressing style (`path` or `virtual_hosted`).
    pub addressing_style: S3AddressingStyle,
    /// AWS Access Key ID.
    pub access_key_id: String,
    /// AWS Secret Access Key.
    pub secret_access_key: String,
    /// Optional STS session token.
    pub session_token: Option<String>,
    /// Compression format (`zstd`, `gzip`, `none`).
    pub compression: S3Compression,
    /// Optional outbound TLS configuration.
    pub tls: Option<OutboundTlsConfig>,
    /// Maximum telemetry batches drained per flush cycle.
    pub max_drain_batches: usize,
    /// Flush interval in milliseconds (larger batches avoid tiny S3 objects).
    pub flush_interval_ms: u64,
    /// TCP connect timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// HTTP PUT timeout in milliseconds.
    pub request_timeout_ms: u64,
}

impl Default for S3LogShipperConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "velda-logs".to_string(),
            region: "us-east-1".to_string(),
            prefix: "velda-logs".to_string(),
            addressing_style: S3AddressingStyle::Path,
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
            compression: S3Compression::Zstd,
            tls: None,
            max_drain_batches: 256,
            flush_interval_ms: 10_000,
            connect_timeout_ms: 3000,
            request_timeout_ms: 10_000,
        }
    }
}
