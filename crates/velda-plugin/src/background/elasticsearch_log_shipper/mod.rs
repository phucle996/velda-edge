//! Elasticsearch / OpenSearch Background Log Shipper.

pub mod client;
pub mod config;
pub mod format;
pub mod plugin;
pub mod validation;

pub use config::{ElasticsearchLogShipperConfig, EsAuthConfig, EsBulkOpType, EsCompression};
pub use plugin::ElasticsearchLogShipperPlugin;
