//! Progressive client request upload streaming for HTTP/3.
//!
//! Enforces unidirectional client-to-server streaming:
//! - Request chunks are received progressively and forwarded upstream.
//! - Upstream returns a single buffered response.

use std::net::SocketAddr;

use velda_core::{Body, L7Request, L7Response};

use crate::client::{connect, extract_sni};
use crate::config::Http3Config;
use crate::error::Http3Error;

/// Pipes a client-streaming HTTP/3 request (streaming request body, buffered response).
pub async fn pipe_client_stream(
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

    // 3. Enforce non-streaming invariant for response if server streaming is disabled
    if let Some(content_type) = resp.headers.get(http::header::CONTENT_TYPE)
        && let Ok(ct_str) = content_type.to_str()
        && ct_str
            .as_bytes()
            .windows(b"text/event-stream".len())
            .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
    {
        return Err(Http3Error::StreamingViolation(
            "Upstream returned text/event-stream (SSE), but server streaming is disabled".into(),
        ));
    }

    // 4. Verify upstream response size limit
    if let Body::Bytes(ref b) = resp.body
        && b.len() > config.max_body_size
    {
        return Err(Http3Error::PayloadTooLarge(b.len()));
    }

    // 5. Enforce RFC 9110/9114 no-body invariant for HEAD, 1xx, 204, 304
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
