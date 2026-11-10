//! Hardware-Aligned In-Memory Metrics Subsystem (`velda_observability::metrics`).
//!
//! Organizes per-core sharded pipeline counters, connection pool gauges/histograms,
//! global process counters, and SIMD-accelerated snapshot aggregation.

pub mod global;
pub mod pool;
pub mod slot;
pub mod snapshot;
pub mod store;
pub mod telemetry;
pub mod types;
pub mod vector;

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
