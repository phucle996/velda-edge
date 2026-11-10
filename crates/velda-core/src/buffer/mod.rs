//! Pre-warmed User-Space Buffer and Object Pooling Subsystem (`velda_core::buffer`).
//!
//! Provides two-tier adaptive memory management: pre-warmed permanent floor plus burst standby queue
//! with coarse clock eviction, bounded by host RAM to eliminate OOM risks.

pub mod adaptive;
pub mod header;
pub mod pool;
pub mod slab;

pub use adaptive::AdaptiveBufferDimensions;
pub use header::{
    HeaderWorkspace, KEY_X_REQUEST_ID, MAX_BYPASS_HEADERS, MAX_NUMERIC_HEADERS, MAX_WIRE_HEADERS,
    NumericFmt, NumericHeaderEntry, NumericOp, X_REQUEST_ID, header_key, resolve_u64_header_value,
};
pub use pool::{
    AdaptivePoolConfig, CoreBufferPool, HeaderPool, HeaderWorkspaceLease, ShardedBufferPool,
};
pub use slab::{
    AdaptiveSlabLimits, COPY_NON_TEMPORAL_FN, CopyNonTemporalFn, PayloadSlab, PayloadSlabLease,
    PayloadSlabPool, SlabClass, ZERO_NON_TEMPORAL_FN, ZeroNonTemporalFn, copy_non_temporal,
    prefetch_l2_range, prefetch_l3_range, prefetch_nta_range, zero_non_temporal,
};
