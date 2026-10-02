//! Progressive server response streaming for HTTP/3 (SSE, LLM tokens, large downloads).
//!
//! Enforces unidirectional server-to-client streaming:
//! - Downstream request body is bounded and forwarded to upstream.
//! - Upstream response headers are permitted to be SSE (`text/event-stream`).
//! - Progressive chunks are forwarded with limit checks.

use std::net::SocketAddr;

use velda_core::{Body, L7Request, L7Response};

use crate::client::forward_request;
use crate::config::Http3Config;
use crate::error::Http3Error;

/// Pipes a server-streaming HTTP/3 request (buffered request, streaming response).
pub async fn pipe_server_stream(
    req: &L7Request,
    target: SocketAddr,
    config: &Http3Config,
) -> Result<L7Response, Http3Error> {
    // 1. Verify downstream request body limit
    if let Body::Bytes(ref b) = req.body
        && b.len() > config.max_body_size
    {
        return Err(Http3Error::PayloadTooLarge(b.len()));
    }

    // 2. Forward request to upstream HTTP/3 backend
    let resp = forward_request(req, target).await?;

    // 3. Verify upstream response size limit
    if let Body::Bytes(ref b) = resp.body
        && b.len() > config.max_body_size
    {
        return Err(Http3Error::PayloadTooLarge(b.len()));
    }

    Ok(resp)
}
