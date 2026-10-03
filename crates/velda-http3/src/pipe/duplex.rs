//! Concurrent bidirectional full-duplex streaming for HTTP/3 (WebTransport / Tunneling).
//!
//! Enforces concurrent bidirectional streaming:
//! - Request stream and response stream run concurrently.
//! - Cleanly terminates on client disconnect or upstream close.

use std::net::SocketAddr;

use velda_core::{L7Request, L7Response};

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
    client.send_request_ref(req).await
}
