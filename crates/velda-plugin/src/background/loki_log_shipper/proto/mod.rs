//! Grafana Loki gRPC Protobuf Serialization Subsystem (`loki_log_shipper::proto`).
//!
//! Owns direct Protobuf wire serialization (`/logproto.Pusher/Push`) with zero AST allocation
//! and Prometheus string syntax labels.

pub mod model;
pub mod template;

pub use model::{EntryAdapter, PushRequest, PushResponse, StreamAdapter, Timestamp};
pub use template::{LokiProtoFormatter, LokiProtoLabels, format_loki_records_proto};
