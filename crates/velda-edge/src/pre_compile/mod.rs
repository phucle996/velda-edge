//! Pre-compiled runtime structures and execution pipelines.
//!
//! Subsystems:
//! - [`pipeline`]: Streaming protocol pipelines (HTTP/1, HTTP/2, HTTP/3, gRPC, TCP, UDP).
//! - [`runtime`]: Lock-free in-memory snapshot (`Runtime`), router, TLS engines, and pipeline table.
//! - [`upstream`]: Upstream dispatchers, connection reuse pools, and load balancing table.

pub mod pipeline;
pub mod runtime;
pub mod upstream;
