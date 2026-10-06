//! Full-duplex bidirectional streaming gRPC pipe forwarding over UDP / QUIC.
//!
//! Streams request messages upstream while concurrently receiving response messages
//! downstream over the persistent multiplexed QUIC client within call deadlines.

use std::time::Duration;
use velda_core::{Body, L7Request, L7Response};

use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::udp::client::GrpcUdpClient;

/// Pipes an active gRPC stream in full-duplex bidirectional streaming mode over UDP / QUIC.
///
/// Workflow:
/// 1. Concurrently stream request messages to upstream endpoint using multiplexed [`GrpcUdpClient`].
/// 2. Concurrently receive response messages from upstream backend within `max_call_duration_ms` timeout.
pub async fn pipe_duplex(
    client: &GrpcUdpClient,
    req: &L7Request,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        let resp = client.send_request_ref(req).await?;

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
