//! Canary Traffic Splitting Plugin Module (`canary`).

pub mod plugin;

pub use plugin::{
    CanaryConfig, CanaryPlugin, HashSource, HashStrategy, HeaderRule, RuleOperator,
    fast_prng_percent,
};
