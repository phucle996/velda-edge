//! Hardware-Aligned In-Memory Metrics Subsystem (`velda_observability::metrics`).
//!
//! Organizes per-core sharded pipeline counters, connection pool gauges/histograms,
//! global process counters, and SIMD-accelerated snapshot aggregation.

mod counter;
mod global;
mod pool;
mod slot;
mod snapshot;
mod store;
mod types;

pub use counter::{
    PIPELINE_COUNTERS_LEN, PipelineMetricsTotals, add_counters, add_pipeline_counters,
};
pub use global::GlobalMetrics;
pub use pool::{EvictionReason, PoolMetrics};
pub use slot::PipelineSlot;
pub use snapshot::{GlobalSnapshot, MetricsSnapshot, PipelineSnapshot, PoolSnapshot};
pub use store::{
    MAX_METRIC_WORKERS, MetricsStore, TOTAL_PIPELINE_SLOTS, current_worker_lane, global_metrics,
    init_global_metrics, set_current_worker_lane,
};
pub use types::{AlpnProtocol, PipeKind, StatusClass, StreamCompletion, WireProtocol};
