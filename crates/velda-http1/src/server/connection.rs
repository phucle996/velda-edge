//! Downstream HTTP/1.1 Connection Driver (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Manages active TCP/TLS downstream streams, coordinating request head parsing,
//! body extraction, keep-alive state transitions, buffer compaction, and response flushing.

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use velda_core::{Body, IngressLimits};

use super::decode::{decode_body, decode_request, decode_request_head};
use super::encode::{
    send_chunk, send_chunked_end, send_response, send_response_head_chunked, send_response_parts,
};
use super::request::{Http1BodyFraming, Http1Request, Http1RequestHead};
use crate::client::response::{Http1Response, Http1ResponseHead};
use crate::config::Http1BufferConfig;
use crate::error::Http1Error;

/// An active HTTP/1.1 connection over an asynchronous downstream stream.
pub struct Http1ServerConnection<IO> {
    stream: IO,
    read_buf: BytesMut,
    write_buf: BytesMut,
    close_requested: bool,
    limits: IngressLimits,
    buf_config: Http1BufferConfig,
}

impl<IO> Http1ServerConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Creates a new HTTP/1.1 connection with mandatory generic IngressLimits configured on the listener.
    pub fn new(stream: IO, limits: IngressLimits) -> Self {
        Self::with_config(stream, limits, Http1BufferConfig::auto())
    }

    /// Creates a new HTTP/1.1 connection with explicit buffer configuration.
    pub fn with_config(stream: IO, limits: IngressLimits, buf_config: Http1BufferConfig) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(buf_config.initial_buffer_capacity),
            write_buf: BytesMut::with_capacity(buf_config.initial_buffer_capacity),
            close_requested: false,
            limits,
            buf_config,
        }
    }

    /// Returns a reference to the active ingress limits configured on this connection.
    #[inline]
    pub const fn limits(&self) -> &IngressLimits {
        &self.limits
    }

    /// Returns a reference to the buffer configuration used by this connection.
    #[inline]
    pub const fn buf_config(&self) -> &Http1BufferConfig {
        &self.buf_config
    }

    /// Compacts read/write buffers if they have grown beyond the shrink threshold
    /// and are currently empty.
    #[inline]
    pub fn compact_buffers(&mut self) {
        if self.read_buf.is_empty() && self.read_buf.capacity() > self.buf_config.shrink_threshold {
            self.read_buf = BytesMut::with_capacity(self.buf_config.initial_buffer_capacity);
        }
        if self.write_buf.is_empty() && self.write_buf.capacity() > self.buf_config.shrink_threshold
        {
            self.write_buf = BytesMut::with_capacity(self.buf_config.initial_buffer_capacity);
        }
    }

    /// Reads and decodes the next HTTP/1.1 request head from the downstream stream.
    ///
    /// Allows callers (e.g. plugin pipeline, auth, WAF header checks) to evaluate
    /// headers and reject malicious or unauthorized requests before reading any body bytes.
    pub async fn next_request_head(
        &mut self,
    ) -> Result<Option<(Http1RequestHead, Http1BodyFraming)>, Http1Error> {
        if self.close_requested {
            return Ok(None);
        }

        self.compact_buffers();

        loop {
            if let Some((head, framing)) = decode_request_head(&mut self.read_buf, &self.limits)? {
                let is_http10 = head.version == http::Version::HTTP_10;
                let conn_header = head
                    .headers
                    .get(http::header::CONNECTION)
                    .and_then(|h| h.to_str().ok());

                if is_http10 {
                    // RFC 9112 Section 9.3: HTTP/1.0 defaults to close unless keep-alive is negotiated
                    if !conn_header.is_some_and(|s| s.eq_ignore_ascii_case("keep-alive")) {
                        self.close_requested = true;
                    }
                } else if conn_header.is_some_and(|s| s.eq_ignore_ascii_case("close")) {
                    self.close_requested = true;
                }

                return Ok(Some((head, framing)));
            }

            let bytes_read = self.stream.read_buf(&mut self.read_buf).await?;
            if bytes_read == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Http1Error::Parse(
                        "Unexpected EOF while reading HTTP request head".into(),
                    ));
                }
            }
        }
    }

    /// Reads and decodes the request body according to the given framing.
    pub async fn read_body(&mut self, framing: Http1BodyFraming) -> Result<Body, Http1Error> {
        if framing == Http1BodyFraming::Empty {
            return Ok(Body::Empty);
        }

        loop {
            if let Some(body) = decode_body(&mut self.read_buf, framing, &self.limits)? {
                return Ok(body);
            }

            let bytes_read = self.stream.read_buf(&mut self.read_buf).await?;
            if bytes_read == 0 {
                return Err(Http1Error::Parse(
                    "Unexpected EOF while reading HTTP request body".into(),
                ));
            }
        }
    }

    /// Reads and decodes the next full HTTP/1.1 request (head + body).
    pub async fn next_request(&mut self) -> Result<Option<Http1Request>, Http1Error> {
        if self.close_requested {
            return Ok(None);
        }

        self.compact_buffers();

        loop {
            if let Some(req) = decode_request(&mut self.read_buf, &self.limits)? {
                let is_http10 = req.version == http::Version::HTTP_10;
                let conn_header = req
                    .headers
                    .get(http::header::CONNECTION)
                    .and_then(|h| h.to_str().ok());

                if is_http10 {
                    if !conn_header.is_some_and(|s| s.eq_ignore_ascii_case("keep-alive")) {
                        self.close_requested = true;
                    }
                } else if conn_header.is_some_and(|s| s.eq_ignore_ascii_case("close")) {
                    self.close_requested = true;
                }

                return Ok(Some(req));
            }

            let bytes_read = self.stream.read_buf(&mut self.read_buf).await?;
            if bytes_read == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Http1Error::Parse(
                        "Unexpected EOF while reading HTTP request".into(),
                    ));
                }
            }
        }
    }

    /// Serializes and flushes an HTTP/1.1 response back to the downstream stream.
    ///
    /// Automatically streams the response body with zero-copy transmission.
    pub async fn send_response(&mut self, response: &Http1Response) -> Result<(), Http1Error> {
        let close = send_response(&mut self.stream, &mut self.write_buf, response).await?;

        if close {
            self.close_requested = true;
        }

        Ok(())
    }

    /// Serializes and flushes response head and body separately.
    pub async fn send_response_parts(
        &mut self,
        head: &Http1ResponseHead,
        body: &Body,
    ) -> Result<(), Http1Error> {
        let close = send_response_parts(&mut self.stream, &mut self.write_buf, head, body).await?;

        if close {
            self.close_requested = true;
        }

        Ok(())
    }

    /// Sends the HTTP/1.1 response head configured for progressive chunked streaming.
    ///
    /// Sets `Transfer-Encoding: chunked` and immediately flushes the head to the client.
    pub async fn send_response_head_chunked(
        &mut self,
        head: &Http1ResponseHead,
    ) -> Result<(), Http1Error> {
        let close = send_response_head_chunked(
            &mut self.stream,
            &mut self.write_buf,
            head.version,
            head.status,
            &head.headers,
        )
        .await?;

        if close {
            self.close_requested = true;
        }

        Ok(())
    }

    /// Sends a single data chunk to the downstream client formatted with chunked transfer coding.
    pub async fn send_chunk(&mut self, chunk: &[u8]) -> Result<(), Http1Error> {
        send_chunk(&mut self.stream, chunk).await
    }

    /// Sends the terminal zero chunk (`0\r\n\r\n`) to cleanly conclude a chunked response.
    pub async fn send_chunked_end(&mut self) -> Result<(), Http1Error> {
        send_chunked_end(&mut self.stream).await
    }

    /// Reads the next progressive chunk from the downstream client if the body is chunked.
    ///
    /// Returns `Ok(Some(bytes))` for each chunk payload, and `Ok(None)` when the terminal
    /// chunk (`0\r\n\r\n`) is reached.
    pub async fn read_next_chunk(&mut self) -> Result<Option<bytes::Bytes>, Http1Error> {
        use super::parse::parse_single_chunk;
        use bytes::Buf;

        loop {
            if let Some((wire_len, payload, is_terminal)) = parse_single_chunk(&self.read_buf)? {
                let bytes = if is_terminal || payload.is_empty() {
                    None
                } else {
                    Some(bytes::Bytes::copy_from_slice(payload))
                };
                self.read_buf.advance(wire_len);
                return Ok(bytes);
            }

            let bytes_read = self.stream.read_buf(&mut self.read_buf).await?;
            if bytes_read == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Http1Error::Parse(
                        "Unexpected EOF while reading chunked body".into(),
                    ));
                }
            }
        }
    }

    /// Returns whether this connection was flagged to close.
    #[inline]
    pub const fn is_closed(&self) -> bool {
        self.close_requested
    }
}
