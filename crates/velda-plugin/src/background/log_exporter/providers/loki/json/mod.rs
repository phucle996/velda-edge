//! Grafana Loki HTTP JSON Payload Serialization Subsystem (`loki_log_shipper::json`).
//!
//! Owns multi-stream pre-baked JSON template formatting (`/loki/api/v1/push`) with
//! zero-allocation persistent scratch buffers.

pub mod template;

pub use template::{LokiJsonFormatter, LokiStreamTemplate, format_loki_push_payload};
