//! Upstream HTTP/3 client connector for `velda-upstream` and connection pool.

use std::net::SocketAddr;
use velda_core::{L7Request, L7Response};

use crate::error::Http3Error;

/// Active HTTP/3 upstream connector.
pub struct Http3UpstreamConnector;

impl Http3UpstreamConnector {
    /// Forwards an HTTP/3 L7Request to the target backend endpoint over QUIC / H3,
    /// returning the parsed L7Response.
    pub async fn forward_request(
        _req: &L7Request,
        _target: SocketAddr,
    ) -> Result<L7Response, Http3Error> {
        // High-level QUIC client egress hook - can be backed by connection pool or direct quinn connection
        Err(Http3Error::H3(
            "HTTP/3 upstream client egress connection not yet initialized".into(),
        ))
    }
}
