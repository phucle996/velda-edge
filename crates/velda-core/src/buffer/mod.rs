//! Pre-warmed User-Space Buffer and Object Pooling Subsystem (`velda_core::buffer`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                           VELDA BUFFER & WORKSPACE HIERARCHY                            │
//! ├──────────────┬──────────────────────────────────────────────────────────────────────────┤
//! │ adaptive     │ Anti-OOM baseline & burst ceilings from host RAM + core count            │
//! │ header       │ Flat [u8 tags | slots] HeaderWorkspace with SIMD lookup & Bit-7 Bypass    │
//! │ pool         │ CoreBufferPool & ShardedBufferPool (per-core slab baseline + LIFO burst) │
//! │ slab         │ Core-jailed PayloadSlabPool (4K / 16K / 32K / 64K contiguous arenas)     │
//! └──────────────┴──────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod adaptive;
pub mod header;
pub mod pool;
pub mod slab;
