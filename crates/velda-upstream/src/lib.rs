//! Protocol-agnostic upstream backend and connection lifecycle (`velda_upstream`).
//!
//! ```text
//!                         UPSTREAM SELECTION WORKFLOW
//!  ┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
//!  │  1. Discovery   │ ──► │ 2. Usable Health     │ ──► │ 3. Load Balancing   │
//!  │  (DNS / Static) │     │    (Active / Passive)│     │    (RR, Maglev, WRR)│
//!  └─────────────────┘     └──────────────────────┘     └──────────┬──────────┘
//!                                                                  │
//!                                                                  ▼
//!                                                       ┌─────────────────────┐
//!                                                       │ 4. Endpoint Lease   │
//!                                                       │    (Target Selected)│
//!                                                       └─────────────────────┘
//! ```

pub mod catalog;
pub mod gate;
pub mod health;
pub mod outcome;
pub mod upstream;

pub use catalog::{UpstreamCatalog, set_global_upstream_catalog, with_upstream_bytes};
pub use gate::AdaptiveConcurrencyGate;
pub use health::{ActiveHealthConfig, HealthConfig, HealthTracker, PassiveHealthConfig};
pub use outcome::UpstreamOutcome;
pub use upstream::{Upstream, UpstreamTimeouts};
