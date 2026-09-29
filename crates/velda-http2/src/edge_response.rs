//! Layer 7 HTTP/2 response serialization and streaming responder.
//!
//! Operates on borrowed `&L7Response` references without holding or retaining
//! response business data inside the protocol engine.

use bytes::Bytes;
use h2::server::SendResponse;
use http::Response;
use velda_core::{Body, L7Response};

use crate::error::Http2Error;

/// Responder for an active HTTP/2 stream.
///
/// Pure encoder capability: caller provides the `&L7Response`, and the responder
/// translates it into HTTP/2 HEADERS and DATA frames over the underlying H2 send stream.
pub struct Http2Responder {
    respond: SendResponse<Bytes>,
}

impl Http2Responder {
    /// Creates a new [`Http2Responder`] from an underlying H2 send handle.
    pub fn new(respond: SendResponse<Bytes>) -> Self {
        Self { respond }
    }

    /// Sends an HTTP/2 response on this stream.
    pub fn send_response(mut self, response: &L7Response) -> Result<(), Http2Error> {
        let mut builder = Response::builder()
            .status(response.status)
            .version(http::Version::HTTP_2);

        for (name, val) in &response.headers {
            builder = builder.header(name, val);
        }

        let is_end_of_stream = !response.has_body();
        let http_response = builder
            .body(())
            .map_err(|e| Http2Error::Parse(e.to_string()))?;

        let mut send_stream = self
            .respond
            .send_response(http_response, is_end_of_stream)?;

        if let Body::Bytes(ref b) = response.body {
            send_stream.send_data(b.clone(), true)?;
        }

        Ok(())
    }
}
