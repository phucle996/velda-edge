//! OpenTelemetry Structured Logger Post-Response Plugin Module (`opentelemetry_logger`).

pub mod client;
pub mod client_grpc;
pub mod config;
pub mod drainer;
pub mod plugin;
pub mod proto;
pub mod record;
pub mod ring_buffer;
pub mod sampling;
pub mod simd;

pub use client::{OtlpLogClient, OtlpLogEndpoint};
pub use client_grpc::{OtlpLogsGrpcClient, OtlpLogsGrpcEndpoint};
pub use config::OpentelemetryLoggerConfig;
pub use drainer::ShadowOtlpDrainer;
pub use plugin::OpentelemetryLoggerPlugin;
pub use proto::{ExportLogsServiceRequest, format_otlp_logs_proto, format_otlp_records_proto};
pub use record::{OtlpLogItem, PackedOtlpRecord, format_otlp_logs_json, format_otlp_records_json};
pub use ring_buffer::{ShardedRingBuffer, SpscRingBuffer};
pub use sampling::SamplingTable;
