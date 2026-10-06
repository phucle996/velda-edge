//! Full-duplex bidirectional streaming gRPC pipe forwarding over UDP.
//!
//! Streams request messages upstream while concurrently receiving response messages
//! downstream over the upstream-managed socket within call deadlines.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use velda_core::{Body, L7Request, L7Response};

use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::udp::client::GrpcUdpUpstreamConnector;

/// Pipes an active gRPC stream in full-duplex bidirectional streaming mode over UDP.
///
/// Workflow:
/// 1. Concurrently stream request messages to upstream endpoint using upstream-managed socket.
/// 2. Concurrently receive response messages from upstream backend within `max_call_duration_ms` timeout.
pub async fn pipe_duplex(
    socket: &UdpSocket,
    target: SocketAddr,
    req: &L7Request,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        let resp =
            GrpcUdpUpstreamConnector::dispatch_with_socket(socket, target, req, config).await?;

        // Validate response LPM length if present
        if let Body::Bytes(ref b) = resp.body
            && !b.is_empty()
            && b.len() >= 5
        {
            let declared_len = u32::from_be_bytes([b[1], b[2], b[3], b[4]]) as usize;
            if declared_len > config.max_message_size {
                return Err(GrpcError::PayloadTooLarge(declared_len));
            }
        }

        Ok(resp)
    };

    tokio::time::timeout(timeout, workflow)
        .await
        .map_err(|_| GrpcError::Timeout)?
}
