//! Dedicated Layer 7 gRPC over TCP (HTTP/2 transport).
//!
//! Submodules:
//! - `server`: Downstream TCP connection handling, stream parsing, and responder.
//! - `client`: Upstream backend connection management, stream opening, and forwarding.
//! - `pipe`: Strategy-driven bidirectional stream pumping between downstream and upstream.

pub mod client;
pub mod server;
pub mod strategy;

pub use strategy::GrpcPipeStrategy;

pub use client::{
    GrpcClientRequest, GrpcClientRequestHead, GrpcClientResponse, GrpcClientResponseHead,
    GrpcUpstreamConnector, build_headers, sanitize_headers,
};
pub use server::{
    GrpcResponder, GrpcServerConnection, GrpcServerRequest, GrpcServerRequestHead,
    GrpcServerResponse, GrpcServerResponseHead, GrpcServerStream, enrich_headers,
    extract_authority, parse_grpc_path,
};
