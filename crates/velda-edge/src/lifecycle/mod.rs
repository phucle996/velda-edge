//! End-to-end traffic serving and supervisor lifecycle coordination.
//!
//! Subsystems:
//! - [`gateway`]: End-to-end traffic engine orchestration and pipeline dispatch.
//! - [`ipc`]: Unix Domain Socket listener receiving live reload signals from `velda-sync`.

pub mod gateway;
pub mod ipc;
pub mod overload;

pub use gateway::run_gateway;
pub use ipc::run_ipc_server;
pub use overload::{DEFAULT_OVERLOAD_SAMPLE_INTERVAL, spawn_overload_monitor};
