//! Unary (buffered) gRPC request-response pipe forwarding over UDP / QUIC.
//!
//! Expects a single downstream request message, validates LPM 5-byte header,
//! transmits to upstream endpoint using persistent multiplexed QUIC client, awaits response,
//! and verifies response LPM frame limits.

use std::time::Duration;
use velda_core::{Body, L7Request, L7Response};

use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::udp::client::GrpcUdpClient;

/// Pipes a Unary (non-streaming) gRPC call over UDP / QUIC.
///
/// Workflow:
/// 1. Verify downstream request body length against LPM 5-byte header and `config.max_message_size`.
/// 2. Dispatch call to upstream endpoint using multiplexed [`GrpcUdpClient`].
/// 3. Verify upstream response body against LPM 5-byte header and `config.max_message_size`.
/// 4. Bound the call within `max_call_duration_ms` timeout.
pub async fn pipe_buffered(
    client: &GrpcUdpClient,
    req: &L7Request,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let timeout = Duration::from_millis(config.max_call_duration_ms);

    let workflow = async {
        // Step 1: Validate downstream request LPM frame if body present
        if let Body::Bytes(ref b) = req.body
            && !b.is_empty()
        {
            if b.len() >= 5 {
                let declared_len = u32::from_be_bytes([b[1], b[2], b[3], b[4]]) as usize;
                if declared_len > config.max_message_size {
                    return Err(GrpcError::PayloadTooLarge(declared_len));
                }
            }
            if b.len() > config.max_message_size + 5 {
                return Err(GrpcError::PayloadTooLarge(b.len()));
            }
        }

        // Step 2: Dispatch through multiplexed QUIC client
        let resp = client.send_request_ref(req).await?;

        // Step 3: Validate upstream response LPM frame if body present
        if let Body::Bytes(ref b) = resp.body
            && !b.is_empty()
        {
            if b.len() >= 5 {
                let declared_len = u32::from_be_bytes([b[1], b[2], b[3], b[4]]) as usize;
                if declared_len > config.max_message_size {
                    return Err(GrpcError::PayloadTooLarge(declared_len));
                }
            }
            if b.len() > config.max_message_size + 5 {
                return Err(GrpcError::PayloadTooLarge(b.len()));
            }
        }

        Ok(resp)
    };

    tokio::time::timeout(timeout, workflow)
        .await
        .map_err(|_| GrpcError::Timeout)?
}
