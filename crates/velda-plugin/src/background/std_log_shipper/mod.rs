//! Standard Stream Log Shipper Background Plugin (`std_log_shipper`).
//!
//! Consumes reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches from
//! [`velda_observability::LogStore`] across all application protocols (HTTP/1, H2, H3, gRPC, Raw L4)
//! and emits them as formatted text or JSON streams to `stdout`, `stderr`, or a filesystem file.

pub mod config;
pub mod format;
pub mod lut;
pub mod plugin;
pub mod sink;

pub use config::StdLogShipperConfig;
pub use plugin::StdLogShipperPlugin;
