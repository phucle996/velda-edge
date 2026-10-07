//! Dedicated Layer 7 gRPC over TCP (HTTP/2 transport).
//!
//! Submodules:
//! - `server`: Downstream TCP connection handling, stream parsing, and responder.
//! - `client`: Upstream backend connection management, stream opening, and forwarding.
//! - `pipe`: Strategy-driven bidirectional stream pumping between downstream and upstream.

pub mod client;
pub mod pipe;
pub mod server;

pub use client::{
    GrpcAccelerationPath, GrpcClientRequest, GrpcClientRequestHead, GrpcClientResponse,
    GrpcClientResponseHead, GrpcUpstreamConnector,
};
pub use server::{
    GrpcResponder, GrpcServerConnection, GrpcServerRequest, GrpcServerRequestHead,
    GrpcServerResponse, GrpcServerResponseHead, GrpcServerStream,
};
