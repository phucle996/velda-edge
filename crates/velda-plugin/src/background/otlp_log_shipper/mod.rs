//! OpenTelemetry Log Shipper Background Plugin (`otlp_log_shipper`).
//!
//! Subscribes to reference-counted 64-byte [`velda_observability::AccessLogRecord`] batches from
//! [`velda_observability::LogStore`] across all protocols and ships them to OpenTelemetry Collectors
//! over persistent HTTP/1.1 Keep-Alive or HTTP/2 gRPC connections.

pub mod client;
pub mod client_grpc;
pub mod format;
pub mod plugin;
pub mod proto;
pub mod wire;

pub use client::OtlpLogClient;
pub use client_grpc::OtlpLogsGrpcClient;
pub use format::{OtlpJsonEnvelope, format_otlp_records_json};
pub use plugin::{OtlpLogShipperConfig, OtlpLogShipperPlugin};
pub use proto::{ExportLogsServiceRequest, OtlpProtoEnvelope, format_otlp_records_proto};
pub use wire::OtlpProtoWireEnvelope;
