//! ClickHouse Log Shipper Plugin (`clickhouse_log_shipper`).
//!
//! Streams access log batches from core-local [`velda_observability::TelemetryStore`] directly into ClickHouse
//! via HTTP ingestion using either `RowBinary` (HPC native) or `JSONEachRow` (standard NDJSON) formats.

pub mod client;
pub mod config;
pub mod format;
pub mod plugin;
pub mod validation;

pub use client::{ClickHouseEndpoint, ClickHouseHttpClient};
pub use config::{ClickHouseLogShipperConfig, TransportSecurity, VELDA_ACCESS_LOGS_TABLE};
pub use format::ClickHouseFormat;
pub use plugin::ClickHouseLogShipperPlugin;
pub use validation::validate_clickhouse_log_shipper_config;
