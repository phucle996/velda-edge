//! Canonical gRPC status codes and trailer metadata helpers (`velda-grpc::status`).
//!
//! Re-exports the unified canonical [`GrpcStatus`] codebook from `velda-core::lut::grpc_status`
//! to ensure zero-divergence across all Data Plane protocol engines, metrics, and access log pipelines.

pub use velda_core::lut::grpc_status::*;
