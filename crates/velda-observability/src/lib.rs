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

pub use logs::{
    AccessLogRecord, DEFAULT_CORE_RING_CAPACITY, LogCompletionReason, LogGrpcStatus, LogHttpMethod,
    LogProtocol, LogStore, LogSubscriber, SpscLogRing, UaFamily, UaOs, classify_user_agent,
    decode_ip_u128, encode_ip_u128, encode_upstream_ip_u32, global_log_store,
    init_global_log_store,
};
pub use metrics::{
    AGGREGATE_COUNTERS, AccumulateCountersFn, AlpnProtocol, DownstreamProto, EvictionReason,
    GlobalMetrics, GlobalSnapshot, LiveTelemetry, MAX_METRIC_WORKERS, MetricsSnapshot,
    MetricsStore, NoopTelemetry, PIPELINE_COUNTERS_LEN, PipeStrategy, PipelineMetricsTotals,
    PipelineSlot, PipelineSnapshot, PoolMetrics, PoolSnapshot, StatusClass, StreamCompletion,
    TOTAL_PIPELINE_SLOTS, Telemetry, UpstreamProto, add_counters, add_counters_64,
    add_counters_128, add_counters_256, current_worker_lane, global_metrics, init_global_metrics,
    resolve_counters_fn, resolve_counters_fn_for_len, set_current_worker_lane,
};
