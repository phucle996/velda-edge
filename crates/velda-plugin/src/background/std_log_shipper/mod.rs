//! Standard Stream Log Shipper Background Plugin (`std_log_shipper`).
//!
//! Consumes reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches from
//! [`velda_observability::LogStore`] across all application protocols (HTTP/1, H2, H3, gRPC, Raw L4)
//! and emits them as formatted text or JSON streams to `stdout`, `stderr`, or a filesystem file.

pub mod plugin;

pub use plugin::{StdLogShipperConfig, StdLogShipperPlugin, TargetStream, format_std_log_payload};
