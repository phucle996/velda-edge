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

pub mod access_log;
pub mod global;
pub mod pool;
pub mod slot;
pub mod snapshot;
pub mod store;
pub mod telemetry;
pub mod types;
pub mod vector;

// Re-exports for convenient, canonical usage across the monorepo
pub use access_log::{
    AccessLogRecord, DEFAULT_CORE_RING_CAPACITY, LogCompletionReason, LogGrpcStatus, LogHttpMethod,
    LogProtocol, LogStore, LogSubscriber, SpscLogRing, UaFamily, UaOs, classify_user_agent,
    decode_ip_u128, encode_ip_u128, encode_upstream_ip_u32, global_log_store,
    init_global_log_store,
};
pub use global::GlobalMetrics;
pub use pool::{EvictionReason, PoolMetrics};
pub use slot::PipelineSlot;
pub use snapshot::{GlobalSnapshot, MetricsSnapshot, PipelineSnapshot, PoolSnapshot};
pub use store::{
    MAX_METRIC_WORKERS, MetricsStore, TOTAL_PIPELINE_SLOTS, current_worker_lane, global_metrics,
    init_global_metrics, set_current_worker_lane,
};
pub use telemetry::{LiveTelemetry, NoopTelemetry, Telemetry};
pub use types::{
    AlpnProtocol, DownstreamProto, PipeStrategy, StatusClass, StreamCompletion, UpstreamProto,
};
pub use vector::{
    AGGREGATE_COUNTERS, AccumulateCountersFn, PIPELINE_COUNTERS_LEN, PipelineMetricsTotals,
    add_counters, add_counters_64, add_counters_128, add_counters_256, resolve_counters_fn,
    resolve_counters_fn_for_len,
};
