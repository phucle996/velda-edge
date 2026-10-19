//! Canary Traffic Splitting Plugin Module (`canary`).

pub mod plugin;
pub mod strategy;

pub use plugin::{CanaryConfig, CanaryPlugin};
pub use strategy::{HashSource, HeaderRule};
