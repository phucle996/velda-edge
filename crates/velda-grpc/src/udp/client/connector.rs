//! Upstream gRPC client protocol execution over UDP.
//!
//! Protocol handles only encoding, transmission, and response decoding over an
//! upstream-provided socket. Socket allocation, interface binding, and connection
//! lifecycle are strictly owned by the Upstream subsystem.

use std::net::SocketAddr;
use tokio::net::UdpSocket;
use velda_core::{Body, L7Request, L7Response};

use crate::config::GrpcConfig;
use crate::error::GrpcError;

/// Upstream gRPC client protocol dispatcher over UDP.
#[derive(Debug, Clone, Copy, Default)]
pub struct GrpcUdpUpstreamConnector;

impl GrpcUdpUpstreamConnector {
    /// Dispatches a unary gRPC request using a pre-allocated upstream-owned socket.
    ///
    /// The Upstream subsystem owns socket lifecycle, interface binding, and connection state.
    /// This function strictly executes protocol encoding, dispatching, and response decoding.
    pub async fn dispatch_with_socket(
        socket: &UdpSocket,
        target: SocketAddr,
        req: &L7Request,
        config: &GrpcConfig,
    ) -> Result<L7Response, GrpcError> {
        socket.connect(target).await.map_err(|e| {
            GrpcError::Internal(format!("failed to connect to upstream {target}: {e}"))
        })?;

        // Send payload if present
        if let Body::Bytes(ref b) = req.body {
            socket.send(b).await.map_err(|e| {
                GrpcError::Internal(format!("failed to send UDP payload to {target}: {e}"))
            })?;
        }

        // Wait for response with timeout
        let timeout_duration = std::time::Duration::from_millis(config.max_call_duration_ms);
        let mut buf = vec![0u8; config.max_message_size.max(65535)];

        let len = tokio::time::timeout(timeout_duration, socket.recv(&mut buf))
            .await
            .map_err(|_| GrpcError::Timeout)?
            .map_err(|e| GrpcError::Internal(format!("failed to receive UDP response: {e}")))?;

        let resp_bytes = bytes::Bytes::copy_from_slice(&buf[..len]);
        Ok(L7Response::new(
            http::StatusCode::OK,
            http::Version::HTTP_3,
            http::HeaderMap::new(),
            Body::Bytes(resp_bytes),
        ))
    }
}
