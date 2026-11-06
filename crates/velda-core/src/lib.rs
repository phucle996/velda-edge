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

pub mod buffer;
pub mod coarse_time;
pub mod context;
pub mod endpoint;
pub mod error;
pub mod hardware;
pub mod l4;
pub mod l7;
pub mod lifecycle;
pub mod protocol;
pub mod provenance;
pub mod request_id;
pub mod socket;
pub mod streaming;
pub mod types;
pub mod worker;

// -----------------------------------------------------------------------------
// Pre-warmed Buffer & Slab Pools
// -----------------------------------------------------------------------------

pub use buffer::{
    AdaptiveBufferDimensions, AdaptivePoolConfig, AdaptiveSlabLimits, CoreBufferPool, HeaderPool,
    HeaderWorkspace, HeaderWorkspaceLease, KEY_X_REQUEST_ID, MAX_BYPASS_HEADERS,
    MAX_NUMERIC_HEADERS, MAX_WIRE_HEADERS, NumericFmt, NumericHeaderEntry, NumericOp, PayloadSlab,
    PayloadSlabLease, PayloadSlabPool, ShardedBufferPool, SlabClass, X_REQUEST_ID, header_key,
    resolve_u64_header_value,
};
pub use coarse_time::{coarse_now_sec, init_coarse_clock};

// -----------------------------------------------------------------------------
// Protocol Family
// -----------------------------------------------------------------------------

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

pub use lifecycle::{Action, CachedResponseDirective, Hook, HookPhase, Phase};

// -----------------------------------------------------------------------------
// Shared identifiers & Endpoints
// -----------------------------------------------------------------------------

pub use endpoint::{Endpoint, EndpointId};
pub use hardware::{
    HardwareTopology, OverloadConfig, OverloadLevel, OverloadTracker, global_hardware_topology,
    init_hardware_topology,
};
pub use provenance::Provenance;
pub use request_id::{RequestIdPool, next_request_id};
pub use socket::{SocketManager, global_socket_manager};
pub use types::{ConnectionId, RequestId, RouteId, UpstreamId};
pub use worker::{CorePartition, WorkerId, WorkerRegistry, global_worker_registry};
