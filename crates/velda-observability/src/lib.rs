//! # Velda Observability — High-Performance Pure In-Memory Metrics Engine
//!
//! Provides hardware-aligned, lock-free, zero-allocation telemetry storage for Velda Edge.
//!
//! # Core Invariants
//! 1. **Pure In-Memory**: Zero knowledge of external serialization formats, wire protocols,
//!    or third-party systems (e.g. Prometheus, StatsD, OTLP). Exporter plugins are responsible
//!    for formatting and exposing snapshots outside this crate.
//! 2. **True Zero-Cost when Inactive**: Employs Zero-Sized Types ([`NoopTelemetry`]) and
//!    Monomorphization to ensure LLVM completely erases telemetry instructions when metrics are disabled.
//! 3. **Hardware Cacheline Alignment**: Every [`PipelineSlot`] is aligned to a 64-byte boundary
//!    (`#[repr(align(64))]`), preventing False Sharing and cross-core cache invalidation across
//!    multi-threaded Tokio workers.
//! 4. **Bounded Cardinality**: Organizes exactly 36 contiguous pipeline slots indexed directly via
//!    $\mathcal{O}(1)$ register integer arithmetic (`(downstream * 12) + (upstream * 4) + pipe`).

pub mod global;
pub mod pool;
pub mod slot;
pub mod snapshot;
pub mod store;
pub mod telemetry;
pub mod types;

// Re-exports for convenient, canonical usage across the monorepo
pub use global::GlobalMetrics;
pub use pool::{EvictionReason, PoolMetrics};
pub use slot::PipelineSlot;
pub use snapshot::{GlobalSnapshot, MetricsSnapshot, PipelineSnapshot, PoolSnapshot};
pub use store::{MetricsStore, TOTAL_PIPELINE_SLOTS};
pub use telemetry::{LiveTelemetry, NoopTelemetry, Telemetry};
pub use types::{
    AlpnProtocol, DownstreamProto, PipeStrategy, StatusClass, StreamCompletion, UpstreamProto,
};
