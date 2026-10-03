//! Crate `velda-edge`
//!
//! Composition root and lifecycle owner of the Velda Edge Data Plane.
//!
//! Architected as an ultra-thin supervisor:
//! - [`bootstrap`]: Cold-start initialization, component assembly, and supervisor loop.
//! - [`config`]: Configuration paths, options, and LKG binary artifact loaders.
//! - [`lifecycle`]: End-to-end traffic serving, runner, and IPC listener.
//! - [`pre_compile`]: Pre-compiled runtime structures, routers, upstreams, and execution pipelines.
//! - [`reload`]: Hot-reload orchestration and atomic runtime swapping.
//!
//! `velda-edge` does NOT process network requests directly; request handling
//! is owned by `velda-transport` and downstream protocol crates.

pub mod bootstrap;
pub mod config;
pub mod error;
pub mod lifecycle;
pub mod pre_compile;
pub mod reload;
pub mod runtime_profile;

pub use bootstrap::{EdgeSupervisor, bootstrap};
pub use config::EdgeConfig;
pub use error::EdgeError;
pub use lifecycle::run_gateway;
pub use pre_compile::{pipeline, runtime, upstream};
pub use reload::{ReloadOutcome, apply_reload, load_initial_runtime};
pub use runtime::{Runtime, RuntimeConfig, SharedRuntime, new_shared_runtime};
pub use runtime_profile::{RuntimeProfile, resolve_runtime_profile};
pub use velda_core::hardware::{HardwareTopology, global_hardware_topology};
