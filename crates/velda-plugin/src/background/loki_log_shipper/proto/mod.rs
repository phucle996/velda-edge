//! Grafana Loki gRPC Protobuf Transport & Serialization Subsystem (`loki_log_shipper::proto`).
//!
//! Owns persistent HTTP/2 gRPC stream delivery (`/logproto.Pusher/Push`) and direct
//! Protobuf wire serialization with zero AST allocation and Prometheus string syntax labels.

pub mod client;
pub mod model;
pub mod template;

pub use client::{LokiLogsGrpcClient, LokiLogsGrpcEndpoint};
pub use model::{EntryAdapter, PushRequest, PushResponse, StreamAdapter, Timestamp};
pub use template::{LokiProtoFormatter, LokiProtoLabels, format_loki_records_proto};
