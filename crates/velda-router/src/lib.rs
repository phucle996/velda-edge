//! In-memory, protocol-isolated routing engine (`velda_router`).
//!
//! ```text
//!                          INCOMING REQUEST VIEW
//!                                    │
//!                                    ▼
//!                     ┌─────────────────────────────┐
//!                     │ Unified Contiguous Arena    │
//!                     │ IntegerBlockNode (16 Bytes) │
//!                     │ 1-Cycle ALU (chunk ^ path)  │
//!                     └──────────────┬──────────────┘
//!                                    │
//!                   ┌────────────────┴────────────────┐
//!                   ▼                                 ▼
//!              [PATH ENDS]                     [PATH CONTINUES]
//!              Exact Match                     Prefix Candidate
//!                   │                                 │
//!                   └────────────────┬────────────────┘
//!                                    ▼
//!                         Match Winning Route / 404
//! ```

pub mod catalog;
pub mod error;
pub mod host;
pub mod l4;
pub mod l7;
pub mod method;
pub mod path;
pub mod router;

pub use catalog::{RouteCatalog, set_global_route_catalog, with_route_bytes};
pub use error::RouterError;
pub use l4::{TcpRoute, TcpRouter, UdpRoute, UdpRouter, UdpSessionAffinity};
pub use l7::{
    GrpcRoute, GrpcRouteRequest, GrpcRouteRule, GrpcRouter, Http1Route, Http1RouteRequest,
    Http1RouteRule, Http1Router, Http2Route, Http2RouteRequest, Http2RouteRule, Http2Router,
    Http3Route, Http3RouteRequest, Http3RouteRule, Http3Router, ListenerGrpcRouter,
    ListenerHttp1Router, ListenerHttp2Router, ListenerHttp3Router,
};
pub use method::MethodMask;
pub use router::{Router, RouterBuilder};
pub use velda_core::RouteId;
