//! Layer 7 gRPC over UDP to TCP Duplex Streaming Bridge Pipeline.
//!
//! Handles full bidirectional streaming RPCs where downstream gRPC requests
//! over UDP / QUIC datagrams are bridged and forwarded over upstream
//! gRPC TCP / HTTP/2 binary framing connections.

use std::sync::Arc;

use velda_core::{L7Request, L7Response};
use velda_grpc::config::GrpcConfig;
use velda_grpc::outcome::GrpcOutcome;
use velda_grpc::tcp::client::request::GrpcClientRequest;

use crate::upstream::GrpcTcpUpstream;

/// Serves a bidirectional streaming gRPC request over UDP bridged to a gRPC TCP backend.
pub async fn serve(
    req: &L7Request,
    upstream: &Arc<GrpcTcpUpstream>,
    config: &GrpcConfig,
) -> Result<(L7Response, Option<(u128, u16)>), GrpcOutcome> {
    let client_req = GrpcClientRequest::from_l7(req);

    let mut client = upstream.acquire(config).await.map_err(|e| {
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "Failed to acquire upstream gRPC over TCP connection"
        );
        GrpcOutcome::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            e.to_string(),
        ))
    })?;
    let upstream_addr = client.target();

    client
        .invoke_client_request(&client_req, config)
        .await
        .map(|resp| (resp.into_l7(), upstream_addr))
}
