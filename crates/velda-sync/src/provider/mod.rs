//! Pure Capability Providers.
//!
//! Provides workflow-independent capabilities:
//! - `local_file`: Raw filesystem read capability
//! - `control_plane`: Remote gRPC delta transport capability
//! - `logger`: Global structured tracing initialization
//! - `metrics`: In-memory atomic telemetry collector

pub mod control_plane;
pub mod local_file;
pub mod logger;
pub mod metrics;

pub use control_plane::ControlPlaneProvider;
pub use local_file::LocalFileProvider;
pub use logger::{WorkerGuard, init_logger};
pub use metrics::{MetricsSnapshot, SyncMetrics};

/// Configuration source provider variant.
#[derive(Debug, Clone)]
pub enum Provider {
    LocalFile(LocalFileProvider),
    ControlPlane(ControlPlaneProvider),
}
