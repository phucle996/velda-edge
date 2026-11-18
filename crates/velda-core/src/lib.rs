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
//! Higher-level crates such as `velda-ingress`, `velda-router`,
//! `velda-plugin`, and `velda-upstream` build on these contracts.

pub mod buffer;
pub mod clock;
pub mod context;
pub mod endpoint;
pub mod error;
pub mod hardware;
pub mod l4;
pub mod l7;
pub mod lifecycle;
pub mod lut;
pub mod protocol;
pub mod provenance;
pub mod request_id;
pub mod socket;
pub mod streaming;
pub mod types;
pub mod worker;

// -----------------------------------------------------------------------------
// Core Request Buffer Workspace
// -----------------------------------------------------------------------------

pub use buffer::HeaderWorkspace;

// -----------------------------------------------------------------------------
// Canonical L1D Vocabulary
// -----------------------------------------------------------------------------

pub use lut::{CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs, WireProtocol};
pub use protocol::ProtocolFamily;

// -----------------------------------------------------------------------------
// Streaming
// -----------------------------------------------------------------------------

pub use streaming::StreamingMode;

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

pub use error::{Error, ErrorKind, RejectReason, Result};

// -----------------------------------------------------------------------------
// Lifecycle
// -----------------------------------------------------------------------------

pub use lifecycle::{Action, CachedResponseDirective, HookPhase, Phase};

// -----------------------------------------------------------------------------
// Shared Identifiers & Endpoints
// -----------------------------------------------------------------------------

pub use endpoint::{Endpoint, EndpointId};
pub use provenance::Provenance;
pub use types::{
    ConnectionId, ListenerId, RequestId, RouteId, UNASSIGNED_UPSTREAM_ID, UNMATCHED_ROUTE_ID,
    UpstreamId,
};
