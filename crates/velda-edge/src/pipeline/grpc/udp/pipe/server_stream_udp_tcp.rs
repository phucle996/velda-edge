//! Layer 7 gRPC over UDP to TCP Server Streaming Bridge Pipeline.
//!
//! Handles Server-side streaming RPCs where downstream gRPC requests
//! over UDP / QUIC datagrams are bridged and forwarded over upstream
//! gRPC TCP / HTTP/2 binary framing connections.

use std::sync::Arc;

use velda_core::{L7Request, L7Response};
use velda_grpc::tcp::client::request::GrpcClientRequest;
use velda_grpc::{GrpcConfig, GrpcError};

use crate::upstream::GrpcTcpUpstream;

/// Serves a server-streaming gRPC request over UDP bridged to a gRPC TCP backend.
pub async fn serve(
    req: &L7Request,
    upstream: &Arc<GrpcTcpUpstream>,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let client_req = GrpcClientRequest::from_l7(req);

    let mut client = upstream.acquire(config).await.map_err(|e| {
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "Failed to acquire upstream gRPC over TCP connection"
        );
        GrpcError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            e.to_string(),
        ))
    })?;

    let mut res = client.invoke_client_request(&client_req, config).await;

    // Self-Healing Retry (RFC 9113 §8.1.4):
    // If upstream connection dropped or remote peer returned REFUSED_STREAM,
    // retry transparently 1-shot on a fresh connection.
    if let Err(ref e) = res
        && e.is_stale_or_refused()
    {
        tracing::debug!(
            upstream = %upstream.id(),
            error = %e,
            "gRPC TCP upstream connection dropped; self-healing with fresh connection"
        );
        if let Ok(mut fresh_client) = upstream.acquire_fresh(config).await {
            res = fresh_client
                .invoke_client_request(&client_req, config)
                .await;
        }
    }

    res.map(|resp| resp.into_l7())
}
