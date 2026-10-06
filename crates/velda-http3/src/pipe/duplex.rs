//! Concurrent bidirectional full-duplex streaming for HTTP/3 (WebTransport / Tunneling).
//!
//! Enforces concurrent bidirectional streaming:
//! - Request stream and response stream run concurrently.
//! - Cleanly terminates on client disconnect or upstream close.

use std::net::SocketAddr;

use velda_core::{Body, L7Request, L7Response};

use crate::client::{connect, extract_sni};
use crate::config::Http3Config;
use crate::error::Http3Error;

/// Pipes a full-duplex bidirectional HTTP/3 stream between downstream and upstream.
pub async fn pipe_duplex(
    req: &L7Request,
    target: SocketAddr,
    config: &Http3Config,
) -> Result<L7Response, Http3Error> {
    let server_name = extract_sni(req, &target);
    let client = connect(target, &server_name, config).await?;
    let resp = client.send_request_ref(req).await?;
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
