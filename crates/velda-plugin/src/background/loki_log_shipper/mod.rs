//! Grafana Loki Log Shipper Background Plugin (`loki_log_shipper`).
//!
//! Consumes reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches from
//! [`velda_observability::LogStore`], partitions records by protocol family (`http`, `grpc`,
//! `raw`) into multi-stream Loki JSON payloads, and ships them over persistent HTTP/1.1
//! Keep-Alive connections to `/loki/api/v1/push`.

pub mod plugin;

pub use plugin::{LokiLogShipperConfig, LokiLogShipperPlugin, format_loki_push_payload};
