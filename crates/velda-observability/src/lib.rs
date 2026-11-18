//! # Velda Observability — In-Memory Metrics, Logs & Tracing Engine
//!
//! Organized into three dedicated domain subsystems:
//! - [`metrics`]: Hardware-aligned, lock-free per-core sharded counters, connection pool gauges,
//!   and SIMD-accelerated snapshot aggregation.
//! - [`logs`]: 256-byte cacheline integer [`AccessLogRecord`], per-core [`SpscLogRing`] with
//!   Non-Temporal streaming stores (L1d bypass) and L2/L3 prefetching, and reference-counted
//!   `Arc<[AccessLogRecord]>` multi-exporter fan-out ([`LogStore`]).
//! - [`traces`]: 64-byte cacheline integer [`SpanRecord`], per-core [`SpscSpanRing`] with
//!   Non-Temporal streaming stores and natural reference-counted `Arc<[SpanRecord]>`
//!   multi-exporter fan-out ([`TraceStore`]).

pub mod logs;
pub mod metrics;
pub mod traces;

pub use logs::*;
pub use metrics::*;
pub use traces::*;
