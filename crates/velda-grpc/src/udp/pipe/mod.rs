//! Dedicated gRPC wire pipe modules over UDP isolated by streaming strategy.
//!
//! Submodules:
//! - `buffered`: Unary RPC pipe forwarding (1 request message, 1 response message).
//! - `server_stream`: Server streaming RPC pipe (1 request message, progressive response stream).
//! - `client_stream`: Client streaming RPC pipe (progressive request stream, 1 response message).
//! - `duplex`: Full-duplex bidirectional RPC pipe (concurrent request and response streaming).

pub mod buffered;
pub mod client_stream;
pub mod duplex;
pub mod server_stream;

use std::net::SocketAddr;
use tokio::net::UdpSocket;
use velda_core::{L7Request, L7Response, StreamingMode};

pub use buffered::pipe_buffered;
pub use client_stream::pipe_client_stream;
pub use duplex::pipe_duplex;
pub use server_stream::pipe_server_stream;

use crate::config::GrpcConfig;
use crate::error::GrpcError;

/// Discrete streaming strategies for gRPC over UDP request handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GrpcUdpPipeStrategy {
    /// Unary RPC: Single request message, single response message.
    Buffered,
    /// Server streaming RPC: Single request message, progressive stream of response messages.
    ServerStream,
    /// Client streaming RPC: Progressive stream of request messages, single response message.
    ClientStream,
    /// Full-duplex bidirectional streaming RPC: Concurrent streaming in both directions.
    Duplex,
}

impl GrpcUdpPipeStrategy {
    /// Derives the execution strategy directly from upstream's declared streaming mode.
    ///
    /// Validated against listener capabilities at configuration compilation time,
    /// so this derivation on the serving hot path is an infallible, zero-cost mapping.
    #[inline]
    pub fn from_streaming(streaming: StreamingMode) -> Self {
        match (streaming.client, streaming.server) {
            (true, true) => Self::Duplex,
            (false, true) => Self::ServerStream,
            (true, false) => Self::ClientStream,
            (false, false) => Self::Buffered,
        }
    }
}

/// Pipes a gRPC call over UDP to an upstream backend endpoint
/// according to the designated [`GrpcUdpPipeStrategy`].
pub async fn pipe_grpc_udp_stream(
    socket: &UdpSocket,
    target: SocketAddr,
    req: &L7Request,
    strategy: GrpcUdpPipeStrategy,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    match strategy {
        GrpcUdpPipeStrategy::Buffered => pipe_buffered(socket, target, req, config).await,
        GrpcUdpPipeStrategy::ServerStream => pipe_server_stream(socket, target, req, config).await,
        GrpcUdpPipeStrategy::ClientStream => pipe_client_stream(socket, target, req, config).await,
        GrpcUdpPipeStrategy::Duplex => pipe_duplex(socket, target, req, config).await,
    }
}

/// Pipes a unary gRPC call between downstream and upstream backend over UDP
/// using an upstream-managed socket.
#[inline]
pub async fn pipe_grpc_udp_unary(
    socket: &UdpSocket,
    target: SocketAddr,
    req: &L7Request,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    pipe_buffered(socket, target, req, config).await
}
