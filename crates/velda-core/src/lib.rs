//! Core Contracts, Primitive Vocabulary & Hardware-Aligned Foundations (`velda_core`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                        VELDA-CORE PRIMITIVE & CONTRACT BOUNDARY                         │
//! ├────────────────┬────────────────────────────────────────────────────────────────────────┤
//! │ context / l4/7 │ Flat RequestContext, RequestState, L4Request/Response, L7Req/Resp      │
//! │ endpoint       │ 24B POD Endpoint (ip: u128, weight: u32, port_be: u16, is_v6: u8)      │
//! │ buffer / lut   │ Pre-warmed HeaderWorkspace/PayloadSlabPool & L1D O(1) lookup tables    │
//! │ hardware/clock │ CPU/NIC/Memory topology probing, overload guards, dual-tier clock      │
//! │ lifecycle/err  │ Narrow hook contracts (Continue/Respond/Reject) & shared Error/Reject  │
//! └────────────────┴────────────────────────────────────────────────────────────────────────┘
//! ```

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
pub mod streaming;
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

pub use l4::request::{L4Request, TransportProtocol};
pub use l4::response::{L4Action, L4Response};
pub use l7::Body;
pub use l7::request::L7Request;
pub use l7::response::L7Response;

// -----------------------------------------------------------------------------
// Context & Flat Integer Identifiers
// -----------------------------------------------------------------------------

pub use context::{
    RequestContext, RequestState, UNASSIGNED_UPSTREAM_ID, UNMATCHED_ROUTE_ID, fnv1a_hash_32,
    ja4_fingerprint_id, ja4_fingerprint_id_bytes,
};

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

pub use endpoint::Endpoint;
pub use provenance::Provenance;
