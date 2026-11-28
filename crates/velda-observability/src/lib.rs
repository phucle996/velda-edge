//! # Velda Observability — In-Memory Metrics & Unified Telemetry Engine
//!
//! Organized into three dedicated domain subsystems:
//! - [`metrics`]: Hardware-aligned, lock-free per-core sharded counters, connection pool gauges,
//!   and SIMD-accelerated snapshot aggregation.
//! - [`telemetry`]: Unified 256-byte cacheline-aligned [`TelemetryRecord`] (with trace fields in
//!   Cache Line 0), 2 MB HugePage / contiguous `mmap` per-core [`SpscTelemetryRing`] with
//!   Non-Temporal streaming stores (L1d bypass), and shared `leases`-refcounted
//!   [`TelemetryBatchPool`] fan-out ([`TelemetryStore`]) consumed by both access log shippers and
//!   the OTLP trace exporter.
//! - [`sampling`]: Thread-local Weyl-sequence listener sampling gates ([`sampling::status::StatusSampling`])
//!   keeping log and trace emission lockstep while metrics remain 100% exact.

pub(crate) mod worker_tls {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Single thread-local block shared across metrics, sampling, and telemetry so hot-path code
    /// resolves at most one TLS base address per thread instead of multiple independent TLS slots.
    pub(crate) struct WorkerTls {
        pub(crate) lane: Cell<usize>,
        pub(crate) lane_set: Cell<bool>,
        pub(crate) weyl: Cell<u64>,
    }

    impl WorkerTls {
        const fn new() -> Self {
            Self {
                lane: Cell::new(0),
                lane_set: Cell::new(false),
                weyl: Cell::new(0),
            }
        }
    }

    static LANE_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

    thread_local! {
        pub(crate) static WORKER_TLS: WorkerTls = const { WorkerTls::new() };
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn init_auto_lane(tls: &WorkerTls) -> usize {
        let lane = LANE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        tls.lane.set(lane);
        tls.lane_set.set(true);
        lane
    }
}

pub mod metrics;
pub mod sampling;
pub mod telemetry;

pub use metrics::*;
pub use telemetry::*;
