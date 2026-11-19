//! Grafana Loki Log Shipper Background Plugin (`loki_log_shipper`).
//!
//! Subscribes to reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches from
//! [`velda_observability::LogStore`] across all protocols and ships them to Grafana Loki
//! over persistent HTTP/1.1 Keep-Alive (`/loki/api/v1/push`) or HTTP/2 gRPC (`/logproto.Pusher/Push`).
//!
//! # Architecture Subsystems
//! - [`wire`]: Hardware-optimized wire formatters, SIMD hex32, division-free integer conversions, and RFC 5952 zero-alloc IP formatters.
//! - [`json`]: Multi-stream pre-baked JSON templates, persistent scratch buffers (0 allocs), and persistent HTTP/1.1 client.
//! - [`proto`]: Direct Protobuf wire format serializer, Prometheus string syntax labels, and persistent HTTP/2 gRPC client.
//! - [`config`]: Explicit configuration definition.
//! - [`plugin`]: Background lifecycle coordinated on `velda-shadow-{i}` (`SCHED_IDLE`, Core $i$).

pub mod config;
pub mod json;
pub mod plugin;
pub mod proto;

pub use config::LokiLogShipperConfig;
pub use plugin::LokiLogShipperPlugin;

pub mod validation;
pub use validation::*;
