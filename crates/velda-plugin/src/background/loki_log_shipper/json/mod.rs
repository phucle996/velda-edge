//! Grafana Loki HTTP JSON Transport & Serialization Subsystem (`loki_log_shipper::json`).
//!
//! Owns persistent HTTP/1.1 Keep-Alive socket delivery (`/loki/api/v1/push`) and multi-stream
//! pre-baked JSON template formatting with zero-allocation persistent scratch buffers.

pub mod client;
pub mod template;

pub use client::{LokiLogClient, LokiLogEndpoint};
pub use template::{LokiJsonFormatter, LokiStreamTemplate, format_loki_push_payload};
