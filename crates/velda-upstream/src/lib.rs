//! # velda-upstream
//!
//! Protocol-agnostic upstream backend lifecycle, discovery, health tracking,
//! and load balancing for the Velda Edge Data Plane.
//!
//! Core invariant:
//! **Router decides $\rightarrow$ Upstream resolves $\rightarrow$ Protocol connects & executes.**

pub mod error;
pub mod health;
pub mod upstream;

// Re-exports for clean, ergonomic usage within upstream
pub use error::{Result, UpstreamError};
pub use health::{ActiveHealthConfig, HealthConfig, HealthTracker, PassiveHealthConfig};
pub use upstream::{Upstream, UpstreamTimeouts};
