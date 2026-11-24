//! Canary Traffic Splitting Plugin Module (`canary`).
//!
//! ```text
//! Inbound L7Request (HeaderWorkspace only, 0 Fallback)
//!   ├─► 1. Header Rules (`rules` against `req.headers`)     -> force canary
//!   └─► 2. Multi-Header SWAR Hash (`hash_headers` all req.) -> Some(hash64) or None (baseline)
//!         └─► Permille Ratio (`((hash64 as u128 * 1000) >> 64) < ratio`, 1..=999)
//!               ├── true  ─► set `*target_upstream = canary_upstream_id` (0 header mutation)
//!               └── false ─► keep default baseline upstream (AccessLevel::Read)
//! ```

pub mod config;
pub mod hash;
pub mod plugin;
pub mod validation;

pub use config::{CanaryConfig, HeaderRule, RuleOperator};
pub use hash::hash_to_permille;
pub use plugin::CanaryPlugin;
