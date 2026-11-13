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
pub mod coarse_time;
pub mod context;
pub mod endpoint;
pub mod error;
pub mod hardware;
pub mod l4;
pub mod l7;
pub mod lifecycle;
pub mod pipe;
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
    AdaptiveBufferDimensions, AdaptivePoolConfig, AdaptiveSlabLimits, COPY_NON_TEMPORAL_FN,
    CopyNonTemporalFn, CoreBufferPool, HeaderPool, HeaderWorkspace, HeaderWorkspaceLease,
    KEY_X_REQUEST_ID, MAX_BYPASS_HEADERS, MAX_NUMERIC_HEADERS, MAX_WIRE_HEADERS, NumericFmt,
    NumericHeaderEntry, NumericOp, PayloadSlab, PayloadSlabLease, PayloadSlabPool,
    ShardedBufferPool, SlabClass, X_REQUEST_ID, ZERO_NON_TEMPORAL_FN, ZeroNonTemporalFn,
    copy_non_temporal, header_key, prefetch_l2_range, prefetch_l3_range, prefetch_nta_range,
    resolve_u64_header_value, zero_non_temporal,
};
pub use coarse_time::{coarse_now_sec, init_coarse_clock};

// -----------------------------------------------------------------------------
// Protocol Family & Pipeline Variants
// -----------------------------------------------------------------------------

pub use pipe::{PIPE_NAMES, PipeKind};
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
pub use types::{ConnectionId, ListenerId, RequestId, RouteId, UpstreamId, fnv1a_hash_32};
pub use worker::{CorePartition, WorkerId, WorkerRegistry, global_worker_registry};
