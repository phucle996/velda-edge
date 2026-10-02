//! Dedicated HTTP/2 wire pipe modules isolated by streaming strategy.
//!
//! Submodules:
//! - `buffered`: Pure in-memory request-response forwarding.
//! - `server_stream`: Progressive response streaming (SSE, LLM tokens, large downloads).
//! - `client_stream`: Progressive request body upload with single response.
//! - `duplex`: Concurrent bidirectional streaming (full-duplex / tunnels).

pub mod buffered;
pub mod client_stream;
pub mod duplex;
pub mod server_stream;

use bytes::BytesMut;
use h2::RecvStream;
use http::Version;
use velda_core::{Body, StreamingMode};

pub use buffered::pipe_buffered;
pub use client_stream::pipe_client_stream;
pub use duplex::pipe_duplex;
pub use server_stream::pipe_server_stream;

use crate::error::Http2Error;
use crate::server::{Http2RequestHead, Http2StreamSender};

/// Discrete streaming strategies for HTTP/2 request handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Http2PipeStrategy {
    /// Non-streaming: Full request body and response body buffered in RAM.
    Buffered,
    /// Server streaming: Request buffered, response pumped progressively as DATA frames.
    ServerStream,
    /// Client streaming: Request pumped progressively as DATA frames, response buffered.
    ClientStream,
    /// Full-duplex: Both request and response streamed concurrently.
    Duplex,
}

impl Http2PipeStrategy {
    /// Derives the execution strategy directly from upstream's declared streaming mode.
    ///
    /// Validated against listener capabilities at configuration compilation time,
    /// so this derivation on the serving hot path is an infallible, zero-cost mapping.
    #[inline]
    pub fn from_streaming(streaming: StreamingMode) -> Self {
        match (streaming.client, streaming.server) {
            (true, true) => Self::Duplex,
            (false, true) => Self::ServerStream,
            (true, false) => Self::ClientStream,
            (false, false) => Self::Buffered,
        }
    }
}

/// Builds an outbound HTTP/2 request frame consuming an owned [`Http2RequestHead`] without cloning.
#[inline]
pub(crate) fn build_outbound_request(
    mut head: Http2RequestHead,
) -> Result<http::Request<()>, Http2Error> {
    crate::headers::sanitize_h2_headers(&mut head.headers);
    let mut builder = http::Request::builder()
        .method(head.method)
        .uri(head.uri)
        .version(Version::HTTP_2);

    for (k, v) in head.headers.drain() {
        if let Some(k) = k {
            builder = builder.header(k, v);
        }
    }

    builder
        .body(())
        .map_err(|e| Http2Error::Parse(e.to_string()))
}

/// Accumulates an incoming response stream into a bounded [`Body`] with flow control release.
///
/// Fast-paths:
/// - If stream is already EOF, returns [`Body::Empty`] with 0 allocations.
/// - If stream is single-chunk, returns [`Body::Bytes`] zero-copy without allocating a buffer.
pub(crate) async fn accumulate_response_body(
    body_stream: &mut RecvStream,
    max_body_size: usize,
) -> Result<Body, Http2Error> {
    if body_stream.is_end_stream() {
        return Ok(Body::Empty);
    }

    let Some(first_chunk) = body_stream.data().await else {
        return Ok(Body::Empty);
    };

    let data = first_chunk?;
    let len = data.len();
    if len > max_body_size {
        let _ = body_stream.flow_control().release_capacity(len);
        return Err(Http2Error::PayloadTooLarge(len));
    }
    let _ = body_stream.flow_control().release_capacity(len);

    if body_stream.is_end_stream() {
        return Ok(Body::Bytes(data));
    }

    let mut body_buf = BytesMut::with_capacity(len * 2);
    body_buf.extend_from_slice(&data);

    while let Some(chunk) = body_stream.data().await {
        let chunk_data = chunk?;
        let chunk_len = chunk_data.len();
        if body_buf.len() + chunk_len > max_body_size {
            let _ = body_stream.flow_control().release_capacity(chunk_len);
            return Err(Http2Error::PayloadTooLarge(body_buf.len() + chunk_len));
        }
        body_buf.extend_from_slice(&chunk_data);
        let _ = body_stream.flow_control().release_capacity(chunk_len);
    }

    Ok(Body::Bytes(body_buf.freeze()))
}

/// Pumps incoming response stream chunks downstream with client disconnect detection.
pub(crate) async fn pump_response_stream(
    mut body_stream: RecvStream,
    mut sender: Http2StreamSender,
    max_body_size: usize,
) -> Result<(), Http2Error> {
    let mut total_bytes = 0;

    while let Some(chunk) = body_stream.data().await {
        let data = chunk?;
        let len = data.len();
        total_bytes += len;
        if total_bytes > max_body_size {
            let _ = body_stream.flow_control().release_capacity(len);
            return Err(Http2Error::PayloadTooLarge(total_bytes));
        }

        if sender.send_chunk(data).await.is_err() {
            // Downstream client disconnected mid-stream
            tracing::debug!("Downstream client disconnected during H2 response streaming");
            return Ok(());
        }

        let _ = body_stream.flow_control().release_capacity(len);
    }

    let _ = sender.finish();
    Ok(())
}
