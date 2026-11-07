//! Layer 4/7 RDMA Upstream managing hardware connection lifecycle, pooling, and core lane pre-warming.
//!
//! Conforms to `AGENTS.md` Section 2.8 and Section 2.9:
//! - `upstream`: Owns endpoint selection, health tracking, and connection lifecycle (WHEN & WHO).
//! - `lease`: RAII guard for acquired channels returning healthy resources to the lane shard upon drop.

pub mod lease;
pub mod upstream;

pub use lease::{RdmaClientResource, RdmaLease};
pub use upstream::RdmaUpstream;
