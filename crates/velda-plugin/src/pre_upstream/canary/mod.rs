//! Canary Traffic Splitting Plugin Module (`canary`).
//!
//! ```text
//! Inbound L7Request (HeaderWorkspace only)
//!   ├─► 1. Header Rules (`rules` against `req.headers`)     -> force canary
//!   ├─► 2. Hash Header (`hash_headers`, SWAR + SplitMix64)  -> hash64 (0 DIV)
//!   └─► 3. Permille Ratio (`((hash64 as u128 * 1000) >> 64) < ratio`, 0..=1000)
//!         ├── true  ─► set `*target_upstream = canary_upstream_id` (0 header mutation)
//!         └── false ─► keep default upstream (AccessLevel::Read)
//! ```

pub mod config;
pub mod hash;
pub mod plugin;
pub mod validation;

pub use config::{CanaryConfig, HashStrategy, HeaderRule, RuleOperator};
pub use hash::{fast_prng_u64, hash_to_permille};
pub use plugin::CanaryPlugin;
