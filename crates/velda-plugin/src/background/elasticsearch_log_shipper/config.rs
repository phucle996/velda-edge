//! Elasticsearch / OpenSearch Log Shipper Configuration.

use serde::{Deserialize, Serialize};

use crate::config::OutboundTlsConfig;

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

/// Authentication configuration for Elasticsearch / OpenSearch.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EsAuthConfig {
    #[default]
    None,
    ApiKey {
        key: String,
    },
    Basic {
        username: String,
        password: String,
    },
    Bearer {
        token: String,
    },
}

/// Validated configuration for [`super::ElasticsearchLogShipperPlugin`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ElasticsearchLogShipperConfig {
    /// Target Elasticsearch / OpenSearch HTTP(S) endpoints (e.g. `["http://127.0.0.1:9200"]`).
    pub endpoints: Vec<String>,
    /// Target index or data stream name (e.g. `"velda-logs"`).
    pub index: String,
    /// Bulk operation type (`create` or `index`).
    pub op_type: EsBulkOpType,
    /// Optional Elasticsearch ingest pipeline name.
    pub pipeline: Option<String>,
    /// Request body compression (`gzip` or `none`).
    pub compression: EsCompression,
    /// Authentication method.
    pub auth: EsAuthConfig,
    /// Optional outbound TLS configuration.
    pub tls: Option<OutboundTlsConfig>,
    /// Maximum telemetry batches drained per flush cycle.
    pub max_drain_batches: usize,
    /// Interval in milliseconds between drain ticks when idle.
    pub flush_interval_ms: u64,
    /// TCP connect timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// HTTP request timeout in milliseconds.
    pub request_timeout_ms: u64,
}

impl Default for ElasticsearchLogShipperConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["http://127.0.0.1:9200".to_string()],
            index: "velda-logs".to_string(),
            op_type: EsBulkOpType::Create,
            pipeline: None,
            compression: EsCompression::Gzip,
            auth: EsAuthConfig::None,
            tls: None,
            max_drain_batches: 64,
            flush_interval_ms: 1000,
            connect_timeout_ms: 3000,
            request_timeout_ms: 5000,
        }
    }
}
