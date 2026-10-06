//! Upstream gRPC client connector over UDP.
//!
//! Sockets and egress binding are owned by the Upstream subsystem according to
//! user configuration. This connector handles protocol framing and message dispatch.

use std::net::SocketAddr;
use tokio::net::UdpSocket;
use velda_core::{Body, L7Request, L7Response};

use crate::config::GrpcConfig;
use crate::error::GrpcError;

/// Upstream gRPC client connector for UDP backends.
#[derive(Debug, Clone, Default)]
pub struct GrpcUdpUpstreamConnector {
    /// User-configured local bind address for egress UDP traffic.
    pub bind_addr: Option<SocketAddr>,
}

impl GrpcUdpUpstreamConnector {
    /// Creates a new UDP connector with an optional user-configured bind address.
    pub fn new(bind_addr: Option<SocketAddr>) -> Self {
        Self { bind_addr }
    }

    /// Dispatches a unary gRPC request using a pre-opened upstream socket.
    ///
    /// Preserves the architectural invariant: socket lifecycle and interface binding
    /// are owned by the Upstream subsystem, eliminating per-request socket churn.
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

    /// Dispatches a unary gRPC request to an upstream backend endpoint over UDP,
    /// binding to the user-configured egress address or family-appropriate wildcard.
    pub async fn dispatch_unary(
        &self,
        target: SocketAddr,
        req: &L7Request,
        config: &GrpcConfig,
    ) -> Result<L7Response, GrpcError> {
        let bind_addr = self.bind_addr.unwrap_or_else(|| {
            if target.is_ipv4() {
                SocketAddr::from(([0, 0, 0, 0], 0))
            } else {
                SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 0))
            }
        });

        let socket = UdpSocket::bind(bind_addr).await.map_err(|e| {
            GrpcError::Internal(format!(
                "failed to bind client UDP socket to {bind_addr}: {e}"
            ))
        })?;

        Self::dispatch_with_socket(&socket, target, req, config).await
    }
}
