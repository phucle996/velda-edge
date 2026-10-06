//! Dedicated gRPC wire pipe modules over UDP.
//!
//! Submodules:
//! - Pumping RPC messages between downstream UDP streams and upstream backends.

use std::net::SocketAddr;
use velda_core::{L7Request, L7Response};

use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::udp::client::GrpcUdpUpstreamConnector;

/// Pipes a unary gRPC call between downstream and upstream backend over UDP.
pub async fn pipe_grpc_udp_unary(
    req: &L7Request,
    target: SocketAddr,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let connector = GrpcUdpUpstreamConnector::default();
    connector.dispatch_unary(target, req, config).await
}
