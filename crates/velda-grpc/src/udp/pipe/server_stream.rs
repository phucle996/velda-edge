//! Server-streaming gRPC pipe forwarding over UDP / QUIC.
//!
//! Client sends a single request message, and upstream streams back multiple response messages.
//! Data messages are forwarded over the persistent multiplexed QUIC client within call deadlines.

use std::time::Duration;
use velda_core::{Body, L7Request, L7Response};

use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::udp::client::GrpcUdpClient;

/// Pipes a server-streaming gRPC call over UDP / QUIC.
///
/// Workflow:
/// 1. Validate single downstream request message against LPM 5-byte header.
/// 2. Dispatch request to upstream endpoint via multiplexed [`GrpcUdpClient`].
/// 3. Stream response messages from upstream backend within `max_call_duration_ms` timeout.
pub async fn pipe_server_stream(
    client: &GrpcUdpClient,
    req: &L7Request,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        // Step 1: Validate downstream request message
        if let Body::Bytes(ref b) = req.body
            && !b.is_empty()
            && b.len() >= 5
        {
            let declared_len = u32::from_be_bytes([b[1], b[2], b[3], b[4]]) as usize;
            if declared_len > config.max_message_size {
                return Err(GrpcError::PayloadTooLarge(declared_len));
            }
        }

        // Step 2: Dispatch request and await streaming response
        client.send_request_ref(req).await
    };

    tokio::time::timeout(timeout, workflow)
        .await
        .map_err(|_| GrpcError::Timeout)?
}
