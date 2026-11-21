//! Pre-warmed User-Space Buffer and Object Pooling Subsystem (`velda_core::buffer`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                           VELDA BUFFER & WORKSPACE HIERARCHY                            │
//! ├──────────────┬──────────────────────────────────────────────────────────────────────────┤
//! │ adaptive     │ Anti-OOM baseline & burst ceilings from host RAM + core count            │
//! │ header       │ Dual-representation HeaderWorkspace (16B register staging + HeaderMap)   │
//! │ pool         │ HeaderPool, CoreBufferPool & ShardedBufferPool (baseline + LIFO burst)   │
//! │ slab         │ Core-jailed PayloadSlabPool (4K / 16K / 32K / 64K contiguous arenas)     │
//! └──────────────┴──────────────────────────────────────────────────────────────────────────┘
//! ```

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
    AdaptiveSlabLimits, ArenaBackingKind, COPY_NON_TEMPORAL_FN, ContiguousArena, CopyNonTemporalFn,
    HUGE_PAGE_SIZE, PayloadSlab, PayloadSlabLease, PayloadSlabPool, SlabClass,
    ZERO_NON_TEMPORAL_FN, ZeroNonTemporalFn, copy_non_temporal, prefetch_l2_range,
    prefetch_l3_range, prefetch_nta_range, zero_non_temporal,
};
