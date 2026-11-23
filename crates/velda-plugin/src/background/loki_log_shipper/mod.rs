//! Grafana Loki Log Shipper Background Plugin (`loki_log_shipper`).
//!
//! Subscribes to reference-counted 64-byte [`velda_observability::TelemetryRecord`] batches from
//! [`velda_observability::TelemetryStore`] across all protocols and ships them via pooled
//! `velda_etp::loki::LokiEtp` leases over HTTP/1.1 Keep-Alive (`/loki/api/v1/push`) or
//! HTTP/2 gRPC (`/logproto.Pusher/Push`).

pub mod config;
pub mod json;
pub mod plugin;
pub mod proto;
pub mod validation;
pub mod wire;

pub use config::LokiLogShipperConfig;
pub use plugin::LokiLogShipperPlugin;
pub use validation::validate_loki_log_shipper_config;
