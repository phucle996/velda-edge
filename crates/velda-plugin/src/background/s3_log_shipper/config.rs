//! S3-Compatible Object Storage Log Shipper Configuration (`s3_log_shipper::config`).
//!
//! ```text
//!   plugins.json ("s3_log_shipper")
//!         │
//!         ├─► etp_id ─────────────► resolved from EtpRegistry (S3Etp)
//!         ├─► prefix ─────────────► optional key prefix inside bucket ("velda-logs")
//!         ├─► compression ────────► zstd | gzip | none
//!         └─► batch / timeout ────► max_drain_batches | flush_interval_ms | request_timeout_ms
//!
//!   Plugin Config Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ etp_id: String | prefix: String | compression: S3Compression               │
//!   │ max_drain_batches: usize | flush_interval_ms: u64 | request_timeout_ms: u64│
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Endpoint URLs, bucket, region, addressing style, credentials, STS session tokens, pool capacity,
//! and TLS settings belong strictly to the referenced `s3` entry in `etp.json` /
//! [`velda_etp::EtpRegistry`].

use serde::{Deserialize, Serialize};

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
pub struct S3LogShipperConfig {
    /// Identifier of the `s3` ETP entry in [`velda_etp::EtpRegistry`].
    pub etp_id: String,
    /// Key prefix inside the bucket (e.g. `"velda-logs"`).
    #[serde(default = "default_prefix")]
    pub prefix: String,
    /// Compression format (`zstd`, `gzip`, `none`).
    #[serde(default)]
    pub compression: S3Compression,
    /// Maximum telemetry batches drained per flush cycle (1..=4096, default: 256).
    #[serde(default = "default_max_drain_batches")]
    pub max_drain_batches: usize,
    /// Flush interval in milliseconds (100..=600_000, default: 10_000).
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    /// HTTP PUT timeout in milliseconds (100..=120_000, default: 10_000).
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
}

fn default_prefix() -> String {
    "velda-logs".to_string()
}

fn default_max_drain_batches() -> usize {
    256
}

fn default_flush_interval() -> u64 {
    10_000
}

fn default_request_timeout() -> u64 {
    10_000
}

impl Default for S3LogShipperConfig {
    fn default() -> Self {
        Self {
            etp_id: String::new(),
            prefix: default_prefix(),
            compression: S3Compression::default(),
            max_drain_batches: default_max_drain_batches(),
            flush_interval_ms: default_flush_interval(),
            request_timeout_ms: default_request_timeout(),
        }
    }
}
