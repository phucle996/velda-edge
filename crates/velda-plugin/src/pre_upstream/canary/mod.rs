//! Canary Traffic Splitting Pre-Upstream Submodule (`canary`).

pub mod plugin;

pub use plugin::{CanaryConfig, CanaryPlugin, HashSource, HeaderRule};
