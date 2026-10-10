//! Crate `velda-edge`
//!
//! Composition root and lifecycle owner of the Velda Edge Data Plane.
//!
//! Architected as an ultra-thin supervisor:
//! - [`bootstrap`]: Cold-start initialization, component assembly, and supervisor loop.
//! - [`config`]: Configuration paths, options, and LKG binary artifact loaders.
//! - [`lifecycle`]: End-to-end traffic serving, runner, and IPC listener.
//! - [`pipeline`]: Streaming protocol ingress pipelines (HTTP/1, HTTP/2, HTTP/3, gRPC, TCP, UDP).
//! - [`profile`]: Node-local hardware sizing and tuning profile.
//! - [`reload`]: Hot-reload orchestration and atomic runtime swapping.
//! - [`runtime`]: In-memory request serving runtime snapshot (Router, Upstreams, TLS, Pipelines).
//! - [`upstream`]: Upstream dispatchers, connection reuse pools, and load balancing table.
//!
//! `velda-edge` does NOT process network requests directly; request handling
//! is owned by `velda-transport` and downstream protocol crates.

pub mod affinity;
pub mod banner;
pub mod bootstrap;
pub mod config;
pub mod error;
pub mod lifecycle;
pub mod pipeline;
pub mod profile;
pub mod reload;
pub mod runtime;
pub mod upstream;

pub use affinity::{ThreadPinner, get_allowed_cores, pin_current_thread_to_core};
pub use banner::{format_startup_banner, print_startup_banner};
pub use bootstrap::{EdgeSupervisor, bootstrap};
pub use config::EdgeConfig;
pub use error::EdgeError;
pub use lifecycle::run_gateway;
pub use profile as runtime_profile;
pub use profile::{RuntimeProfile, resolve_runtime_profile};
pub use reload::{ReloadOutcome, apply_reload, load_initial_runtime};
pub use runtime::{Runtime, RuntimeConfig, SharedRuntime, new_shared_runtime};
pub use velda_core::hardware::{HardwareTopology, global_hardware_topology};
