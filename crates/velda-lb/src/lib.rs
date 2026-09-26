//! # velda-lb
//!
//! Stage 3 — High-Performance Load Balancing Subsystem for the Velda Edge Data Plane.
//!
//! Strict Invariants:
//! - **Pure In-Memory Algorithms**: Zero disk I/O, zero network calls, zero JSON parsing.
//! - **Zero-Copy & Zero Heap Allocation on Hot Path**: Operates directly over `&[Endpoint]` slices.
//! - **Direct Target IP Decision**: Returns `SocketAddr` directly to allow caller to build `ConnectionKey`.
//! - **Lock-Free Concurrency**: State-heavy balancers (Maglev, RingHash) read via atomic pointer swaps (`ArcSwap`).

pub mod algorithm;
pub mod balancer;
pub mod context;
pub mod metrics;

// Top-level re-exports for clean, ergonomic usage
pub use algorithm::*;
pub use balancer::LoadBalancer;
pub use context::SelectionContext;
pub use metrics::EndpointMetrics;

pub use velda_core::{Endpoint, EndpointId};
