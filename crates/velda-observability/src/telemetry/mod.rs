//! Unified Lock-Free 256-Byte Cacheline Telemetry Subsystem (`velda_observability::telemetry`).
//!
//! Feeds both access log shippers and distributed trace exporters from a single core-local emit path:
//! - `record`: Canonical 256-byte (`#[repr(C, align(64))]`) pure-integer [`TelemetryRecord`] with
//!   trace fields packed in Cache Line 0 (`0..64` B) and client/URL metadata in Cache Lines 1..3.
//! - `ring`: 2 MB HugePage / contiguous `mmap`, 64-byte aligned per-core [`SpscTelemetryRing`] with
//!   Non-Temporal streaming stores on Frontline push and L2/L3 `_MM_HINT_T1` prefetching on Shadow drain.
//! - `pool`: Pre-allocated rotating [`TelemetryBatchPool`] managed via shared `leases` refcount.
//! - `store`: Multi-exporter fan-out [`TelemetryStore`] delivering leased [`TelemetryBatch`] handles.

mod pool;
mod record;
mod ring;
mod store;

pub use pool::{
    DEFAULT_BATCH_CAPACITY, DEFAULT_POOL_SLOTS, PooledTelemetryBatch, TelemetryBatch,
    TelemetryBatchPool,
};
pub use record::TelemetryRecord;
pub use ring::{DEFAULT_CORE_RING_CAPACITY, SpscTelemetryRing};
pub use store::{
    TelemetryStore, TelemetrySubscriber, global_telemetry_store, init_global_telemetry_store,
};
pub use velda_core::lut::ip::{encode_ip_u128, encode_upstream_ip_u32};
