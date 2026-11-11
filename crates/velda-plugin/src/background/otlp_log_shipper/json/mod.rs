//! OTLP JSON Transport & Serialization Subsystem (`otlp_log_shipper::json`).
//!
//! Owns persistent HTTP/1.1 Keep-Alive socket delivery (`/v1/logs`) and pre-baked
//! static JSON envelope formatting with zero-allocation LUT stamping.

pub mod client;
pub mod template;

pub use client::{OtlpLogClient, OtlpLogEndpoint};
pub use template::{OTLP_SCHEMA_URL, OtlpJsonEnvelope, format_otlp_records_json};
