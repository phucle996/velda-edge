//! ClickHouse Log Shipper Plugin (`clickhouse_log_shipper`).
//!
//! Streams access log batches from core-local [`velda_observability::TelemetryStore`] directly into ClickHouse
//! via pooled `velda_etp::clickhouse::ClickHouseEtp` HTTP/1.1 keep-alive leases using either
//! `RowBinary` (HPC native) or `JSONEachRow` (standard NDJSON) formats.

pub mod config;
pub mod format;
pub mod plugin;
pub mod validation;

pub use config::{ClickHouseLogShipperConfig, VELDA_ACCESS_LOGS_TABLE};
pub use format::ClickHouseFormat;
pub use plugin::ClickHouseLogShipperPlugin;
pub use validation::validate_clickhouse_log_shipper_config;
