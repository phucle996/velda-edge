//! Layer 7 gRPC over UDP downstream to RDMA upstream Duplex Streaming Bridge Pipeline.
//!
//! Handles full bidirectional streaming RPCs where downstream gRPC requests over UDP / QUIC
//! datagrams are bridged and forwarded natively over upstream RDMA kernel-bypass connections.
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use std::sync::Arc;

use velda_core::{L7Request, L7Response};
use velda_grpc::tcp::client::request::GrpcClientRequest;
use velda_grpc::{GrpcConfig, GrpcError};

use crate::upstream::grpc::GrpcRdmaUpstream;

/// Serves a bidirectional streaming gRPC request over UDP bridged to an RDMA backend.
pub async fn serve(
    req: &L7Request,
    upstream: &Arc<GrpcRdmaUpstream>,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let client_req = GrpcClientRequest::from_l7(req);

    let mut client = upstream.acquire(config).await.map_err(|e| {
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "Failed to acquire upstream gRPC over RDMA connection"
        );
        GrpcError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            e.to_string(),
        ))
    })?;

    client
        .invoke_client_request(&client_req, config)
        .await
        .map(|resp| resp.into_l7())
}
