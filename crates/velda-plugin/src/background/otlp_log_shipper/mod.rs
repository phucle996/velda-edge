//! OpenTelemetry Log Shipper Background Plugin (`otlp_log_shipper`).
//!
//! Subscribes to reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches from
//! [`velda_observability::LogStore`] across all protocols and ships them to OpenTelemetry Collectors
//! over persistent HTTP/1.1 Keep-Alive or HTTP/2 gRPC connections.
//!
//! # Architecture Subsystems
//! - [`wire`]: Hardware-optimized wire formatters, SIMD hex32, division-free integer formatting, and RFC 4291/RFC 5952 zero-alloc IP formatters.
//! - [`json`]: Pre-baked static JSON envelope, schema URL byte constant, and persistent HTTP/1.1 Keep-Alive client.
//! - [`proto`]: Direct Protobuf wire format serializer, SIMD timestamp stamping, and persistent HTTP/2 gRPC client.
//! - [`config`]: Explicit configuration definition.
//! - [`plugin`]: Background lifecycle coordinated on `velda-shadow-{i}` (`SCHED_IDLE`, Core $i$).

pub mod config;
pub mod json;
pub mod plugin;
pub mod proto;

pub use config::OtlpLogShipperConfig;
pub use plugin::OtlpLogShipperPlugin;

pub mod validation;
pub use validation::*;
