//! Node-local runtime sizing and hardware tuning profile.
//!
//! # Lifecycle & Priority Hierarchy:
//! 1. **Operator Override (File Priority)**: Checks `<runtime_dir>/runtime.json`.
//!    If present and valid, operator-specified values are strictly preserved.
//! 2. **Hardware Probe Fallback**: If the file does not exist or has missing fields,
//!    probes host CPU and RAM (cgroup limit or physical) to calculate [`velda_core::CpuTier`] and [`velda_core::MemoryTier`].
//! 3. **Self-Persisting Write-Back**: Serializes the resulting profile back to
//!    `<runtime_dir>/runtime.json` (best-effort, ignoring read-only filesystem errors).
//!
//! Invariant: Hot-path request serving never reads or writes this file.
//! This lifecycle runs exclusively during cold-start bootstrap.

pub mod resolver;
pub mod runtime;
pub mod schema;

pub use resolver::resolve_runtime_profile;
pub use runtime::RuntimeProfile;
pub use schema::*;
