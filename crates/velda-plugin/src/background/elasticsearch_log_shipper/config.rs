//! Elasticsearch / OpenSearch Log Shipper Configuration (`elasticsearch_log_shipper::config`).
//!
//! ```text
//!   plugins.json ("elasticsearch_log_shipper")
//!         │
//!         ├─► etp_id ─────────────► resolved from EtpRegistry (ElasticsearchEtp)
//!         ├─► index (default) ────► "velda-logs" (or etp.config.index when empty)
//!         ├─► op_type / pipeline ─► create | index, optional ?pipeline=...
//!         ├─► compression ────────► gzip | none
//!         └─► batch / timeout ────► max_drain_batches | flush_interval_ms | request_timeout_ms
//!
//!   Plugin Config Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ etp_id: String | index: String | op_type: EsBulkOpType                     │
//!   │ pipeline: Option<String> | compression: EsCompression                      │
//!   │ max_drain_batches: usize | flush_interval_ms: u64 | request_timeout_ms: u64│
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Connection, endpoints, authentication, pool capacity, and TLS/mTLS settings belong strictly
//! to the referenced `elasticsearch` entry in `etp.json` / [`velda_etp::EtpRegistry`].

use serde::{Deserialize, Serialize};

/// Bulk API operation type for Elasticsearch / OpenSearch documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EsBulkOpType {
    /// Append-only (`{"create":{"_index":"..."}}`), required for ES Data Streams.
    #[default]
    Create,
    /// Standard upsert/index (`{"index":{"_index":"..."}}`).
    Index,
}

/// Wire compression applied to `_bulk` HTTP request bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EsCompression {
    #[default]
    Gzip,
    None,
}

/// Validated configuration for [`super::ElasticsearchLogShipperPlugin`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElasticsearchLogShipperConfig {
    /// Identifier of the `elasticsearch` ETP entry in [`velda_etp::EtpRegistry`].
    pub etp_id: String,
    /// Target index or data stream name (defaults to `"velda-logs"`; if empty, falls back to
    /// `etp.config.index` or `"velda-logs"`).
    #[serde(default = "default_index")]
    pub index: String,
    /// Bulk operation type (`create` or `index`).
    #[serde(default)]
    pub op_type: EsBulkOpType,
    /// Optional Elasticsearch ingest pipeline name.
    #[serde(default)]
    pub pipeline: Option<String>,
    /// Request body compression (`gzip` or `none`).
    #[serde(default)]
    pub compression: EsCompression,
    /// Maximum telemetry batches drained per flush cycle (1 to 4,096, default: 64).
    #[serde(default = "default_max_drain_batches")]
    pub max_drain_batches: usize,
    /// Interval in milliseconds between drain ticks (10 to 60,000 ms, default: 1,000).
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    /// HTTP request timeout in milliseconds (100 to 120,000 ms, default: 5,000).
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
}

fn default_index() -> String {
    "velda-logs".to_string()
}

fn default_max_drain_batches() -> usize {
    64
}

fn default_flush_interval() -> u64 {
    1000
}

fn default_request_timeout() -> u64 {
    5000
}

impl Default for ElasticsearchLogShipperConfig {
    fn default() -> Self {
        Self {
            etp_id: String::new(),
            index: default_index(),
            op_type: EsBulkOpType::default(),
            pipeline: None,
            compression: EsCompression::default(),
            max_drain_batches: default_max_drain_batches(),
            flush_interval_ms: default_flush_interval(),
            request_timeout_ms: default_request_timeout(),
        }
    }
}
