//! Core contracts and data models for Velda Edge.
//!
//! `velda-core` is the foundation crate of the Velda data plane.
//! It defines shared types, request/response models, lifecycle
//! contracts, and common errors used by higher-level crates.
//!
//! This crate intentionally does not implement:
//!
//! - TCP/UDP transport handling
//! - TLS termination
//! - HTTP parsing
//! - Route matching
//! - Plugin execution
//! - Upstream connection management
//! - Load balancing
//! - Runtime orchestration
//!
//! Higher-level crates such as `velda-transport`, `velda-router`,
//! `velda-plugin`, and `velda-upstream` build on these contracts.

pub mod context;
pub mod error;
pub mod l4;
pub mod l7;
pub mod lifecycle;
pub mod types;

// -----------------------------------------------------------------------------
// L4 / L7 Models
// -----------------------------------------------------------------------------

pub use l4::request::{L4Request, Peer, TransportProtocol};
pub use l4::response::{L4Action, L4Response};
pub use l7::Body;
pub use l7::request::L7Request;
pub use l7::response::L7Response;

// -----------------------------------------------------------------------------
// Context
// -----------------------------------------------------------------------------

pub use context::{ConnectionContext, RequestContext, RequestState};

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

pub use error::{Error, ErrorKind, Result};

// -----------------------------------------------------------------------------
// Lifecycle
// -----------------------------------------------------------------------------

pub use lifecycle::{Action, Hook, HookPhase, Phase};

// -----------------------------------------------------------------------------
// Shared identifiers
// -----------------------------------------------------------------------------

pub use types::{ConnectionId, RequestId, RouteId, UpstreamId};
