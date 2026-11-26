//! Distributed, Hierarchical Pre-Upstream Rate Limiter (`velda-plugin::pre_upstream::rate_limit`).
//!
//! ```text
//!   Request ──► CompiledKeyExtractor (CSPRNG boot_salt SWAR, up to 4 hierarchical tiers)
//!                  │
//!                  ▼
//!             RateLimitArena (Per-Core L1 CoreLane + 64-shard CLOCK Slab + Soft Overdraft)
//!                  │
//!                  ├──► etp_id: None     ──► Local node-only enforcement (pbww = 0)
//!                  └──► etp_id: Some(id) ──► Shadow Sync over RateLimitSotProvider (9 SoT ETPs):
//!                                              redis | memcached | dynamodb | postgres |
//!                                              mysql | nats | clickhouse | elasticsearch | s3
//! ```

pub mod algorithm;
pub mod arena;
pub mod key;
pub mod plugin;
pub mod providers;
pub mod validation;

pub use arena::{DirtyKeyDelta, RateLimitArena, ReconciledClusterTotal};
pub use key::CompiledKeyExtractor;
pub use plugin::RateLimitPlugin;
pub use providers::RateLimitSotProvider;
pub use validation::{FallbackBehavior, KeyStrategy, RateLimitAlgorithm, RateLimitConfig};
