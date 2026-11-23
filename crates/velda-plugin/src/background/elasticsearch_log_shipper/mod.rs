//! Elasticsearch / OpenSearch Background Log Shipper (`elasticsearch_log_shipper`).
//!
//! Streams access log batches from core-local [`velda_observability::TelemetryStore`] directly into
//! Elasticsearch / OpenSearch via pooled `velda_etp::elasticsearch::ElasticsearchEtp` HTTP/1.1
//! keep-alive leases using NDJSON `_bulk` formatting.

pub mod config;
pub mod format;
pub mod plugin;
pub mod validation;
pub mod wire;

pub use config::{ElasticsearchLogShipperConfig, EsBulkOpType, EsCompression};
pub use plugin::ElasticsearchLogShipperPlugin;
pub use validation::validate_elasticsearch_log_shipper_config;
