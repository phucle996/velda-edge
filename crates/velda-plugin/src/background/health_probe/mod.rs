//! Health & Readiness Probe Background Plugin Submodule (`health_probe`).

pub mod plugin;
pub mod response;
pub mod state;

pub use plugin::{HealthProbeConfig, HealthProbePlugin};
pub use state::HealthState;
