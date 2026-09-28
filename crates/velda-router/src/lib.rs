//! # velda-router
//!
//! High-performance, in-memory routing engine for Velda Edge Data Plane.
//!
//! Provides lock-free Layer 4 (TCP/UDP), Layer 7 HTTP (Aho-Corasick prefix/exact),
//! and Layer 7 gRPC (service/method) routing.
//! All routing structures are pre-compiled in RAM for zero-IO, zero-allocation request serving.

pub mod error;
pub mod l4;
pub mod l7;
pub mod router;

pub use error::RouterError;
pub use l4::{L4Route, L4Router};
pub use l7::{GrpcRoute, GrpcRouteRequest, GrpcRouter, HttpRoute, HttpRouteRequest, HttpRouter};
pub use router::{Router, RouterBuilder};
