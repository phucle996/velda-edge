//! Dedicated gRPC wire pipe modules over UDP.
//!
//! Pumping RPC messages between downstream UDP streams and upstream backends
//! over upstream-managed sockets.

use std::net::SocketAddr;
use tokio::net::UdpSocket;
use velda_core::{L7Request, L7Response};

use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::udp::client::GrpcUdpUpstreamConnector;

/// Pipes a unary gRPC call between downstream and upstream backend over UDP
/// using an upstream-managed socket.
pub async fn pipe_grpc_udp_unary(
    socket: &UdpSocket,
    target: SocketAddr,
    req: &L7Request,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    GrpcUdpUpstreamConnector::dispatch_with_socket(socket, target, req, config).await
}
