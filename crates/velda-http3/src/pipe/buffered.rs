//! Pure in-memory buffered HTTP/3 request-response forwarding.
//!
//! Enforces non-streaming invariants:
//! - Request body is bounded within `max_body_size`.
//! - Forwards to upstream target backend over HTTP/3.
//! - Upstream response headers are inspected: if upstream returns `text/event-stream` (SSE)
//!   when server streaming is disabled on this listener, returns [`Http3Error::StreamingViolation`].
//! - Upstream response body is bounded within `max_body_size`.

use std::net::SocketAddr;

use velda_core::{Body, L7Request, L7Response};

use crate::client::{connect, extract_sni};
use crate::config::Http3Config;
use crate::error::Http3Error;

/// Pipes a non-streaming HTTP/3 request to the target backend endpoint.
pub async fn pipe_buffered(
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

    // 3. Enforce non-streaming invariant: reject SSE if listener has server streaming disabled
    if let Some(content_type) = resp.headers.get(http::header::CONTENT_TYPE)
        && let Ok(ct_str) = content_type.to_str()
        && ct_str
            .as_bytes()
            .windows(b"text/event-stream".len())
            .any(|w| w.eq_ignore_ascii_case(b"text/event-stream"))
    {
        return Err(Http3Error::StreamingViolation(
            "Upstream returned text/event-stream (SSE), but server streaming is disabled on this listener".into(),
        ));
    }

    // 4. Verify upstream response body limit
    if let Body::Bytes(ref b) = resp.body
        && b.len() > config.max_body_size
    {
        return Err(Http3Error::PayloadTooLarge(b.len()));
    }

    Ok(resp)
}
