//! OpenTelemetry Structured Logger Post-Response Plugin Module (`opentelemetry_logger`).

pub mod client;
pub mod client_grpc;
pub mod plugin;
pub mod proto;
pub mod record;

pub use client::{OtlpLogClient, OtlpLogEndpoint};
pub use client_grpc::{OtlpLogsGrpcClient, OtlpLogsGrpcEndpoint};
pub use plugin::{OpentelemetryLoggerConfig, OpentelemetryLoggerPlugin};
pub use proto::{ExportLogsServiceRequest, format_otlp_logs_proto};
pub use record::{OtlpLogItem, format_otlp_logs_json};
