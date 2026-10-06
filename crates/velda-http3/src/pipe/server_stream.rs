//! Progressive server response streaming for HTTP/3 (SSE, LLM tokens, large downloads).
//!
//! Enforces unidirectional server-to-client streaming:
//! - Downstream request body is bounded and forwarded to upstream.
//! - Upstream response headers are permitted to be SSE (`text/event-stream`).
//! - Progressive chunks are forwarded with limit checks.

use std::net::SocketAddr;

use velda_core::{Body, L7Request, L7Response};

use crate::client::{connect, extract_sni};
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
    let server_name = extract_sni(req, &target);
    let client = connect(target, &server_name, config).await?;
    let resp = client.send_request_ref(req).await?;

    // 3. Verify upstream response size limit
    if let Body::Bytes(ref b) = resp.body
        && b.len() > config.max_body_size
    {
        return Err(Http3Error::PayloadTooLarge(b.len()));
    }

    // 4. Enforce RFC 9110/9114 no-body invariant for HEAD, 1xx, 204, 304
    let is_head = req.method == http::Method::HEAD;
    let is_no_body = resp.status.is_informational()
        || resp.status == http::StatusCode::NO_CONTENT
        || resp.status == http::StatusCode::NOT_MODIFIED;
    if is_head || is_no_body {
        return Ok(L7Response::new(
            resp.status,
            resp.version,
            resp.headers,
            Body::Empty,
        ));
    }

    Ok(resp)
}
