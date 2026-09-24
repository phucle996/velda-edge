//! Crate `velda-edge`
//!
//! Composition root and lifecycle owner of the Velda Edge Data Plane.
//!
//! Architected as an ultra-thin supervisor:
//! - [`bootstrap`]: Cold-start initialization, component assembly, and supervisor loop.
//! - [`config`]: Configuration paths, options, and LKG binary artifact loaders.
//! - [`runtime`]: Lock-free in-memory snapshot (`Runtime`) wrapped in [`ArcSwap`].
//! - [`reload`]: Hot-reload orchestration and atomic runtime swapping.
//! - [`uds`]: Unix Domain Socket IPC listener receiving reload notifications from `velda-sync`.
//!
//! `velda-edge` does NOT process network requests directly; request handling
//! is owned by `velda-transport` and downstream protocol crates.

pub mod bootstrap;
pub mod config;
pub mod reload;
pub mod runtime;
pub mod uds;

pub use bootstrap::{EdgeSupervisor, start};
pub use config::{EdgeConfig, EdgeError};
pub use reload::{ReloadOutcome, apply_reload};
pub use runtime::{Runtime, SharedRuntime, new_shared_runtime};
