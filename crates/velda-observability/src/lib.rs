//! # Velda Observability — In-Memory Metrics & 64-Byte Access Log Engine
//!
//! Organized into two dedicated domain subsystems:
//! - [`metrics`]: Hardware-aligned, lock-free per-core sharded counters, connection pool gauges,
//!   and SIMD-accelerated snapshot aggregation.
//! - [`logs`]: 64-byte single-cacheline integer [`AccessLogRecord`], per-core [`SpscLogRing`] with
//!   Non-Temporal streaming stores (L1d bypass) and L2/L3 prefetching, and reference-counted
//!   `Arc<[AccessLogRecord]>` multi-exporter fan-out ([`LogStore`]).

pub mod logs;
pub mod metrics;

pub use logs::*;
pub use metrics::*;
