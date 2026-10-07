//! Downstream HTTP/1.1 Connection Driver (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Manages active TCP/TLS downstream streams, coordinating request head parsing,
//! body extraction, keep-alive state transitions, buffer compaction, and response flushing.

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_core::Body;

use super::request::{
    Http1BodyFraming, Http1ServerRequest, Http1ServerRequestHead, decode_body, decode_request,
    decode_request_head,
};
use super::response::{
    Http1ServerResponse, Http1ServerResponseHead, send_chunk, send_chunked_end, send_response,
    send_response_head_chunked, send_response_parts,
};
use crate::client::response::{Http1ClientResponse, Http1ClientResponseHead};
use crate::config::Http1Config;
use crate::error::Http1Error;

/// An active HTTP/1.1 connection over an asynchronous downstream stream.
pub struct Http1ServerConnection<IO> {
    stream: IO,
    read_buf: BytesMut,
    write_buf: BytesMut,
    close_requested: bool,
    config: Http1Config,
}

impl<IO> Http1ServerConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Creates a new HTTP/1.1 connection with the given [`Http1Config`].
    pub fn new(stream: IO, config: Http1Config) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(config.initial_buffer_capacity),
            write_buf: BytesMut::with_capacity(config.initial_buffer_capacity),
            close_requested: false,
            config,
        }
    }

    /// Returns a reference to the active configuration used by this connection.
    #[inline]
    pub const fn config(&self) -> &Http1Config {
        &self.config
    }

    /// Compacts read/write buffers if they have grown beyond the shrink threshold
    /// and are currently empty.
    #[inline]
    pub fn compact_buffers(&mut self) {
        if self.read_buf.is_empty() && self.read_buf.capacity() > self.config.shrink_threshold {
            self.read_buf = BytesMut::with_capacity(self.config.initial_buffer_capacity);
        }
        if self.write_buf.is_empty() && self.write_buf.capacity() > self.config.shrink_threshold {
            self.write_buf = BytesMut::with_capacity(self.config.initial_buffer_capacity);
        }
    }

    /// Reads and decodes the next HTTP/1.1 request head from the downstream stream.
    ///
    /// Allows callers (e.g. plugin pipeline, auth, WAF header checks) to evaluate
    /// headers and reject malicious or unauthorized requests before reading any body bytes.
    pub async fn next_request_head(
        &mut self,
    ) -> Result<Option<(Http1ServerRequestHead, Http1BodyFraming)>, Http1Error> {
        if self.close_requested {
            return Ok(None);
        }

        self.compact_buffers();

        loop {
            if let Some((head, framing)) = decode_request_head(&mut self.read_buf, &self.config)? {
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

            let timeout_duration = if self.read_buf.is_empty() {
                std::time::Duration::from_millis(self.config.idle_timeout_ms)
            } else {
                std::time::Duration::from_millis(self.config.header_read_timeout_ms)
            };

            let bytes_read = match tokio::time::timeout(
                timeout_duration,
                self.stream.read_buf(&mut self.read_buf),
            )
            .await
            {
                Ok(res) => res?,
                Err(_) => {
                    if self.read_buf.is_empty() {
                        return Ok(None);
                    } else {
                        return Err(Http1Error::Timeout);
                    }
                }
            };
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

        let idle_timeout = std::time::Duration::from_millis(self.config.idle_timeout_ms);

        loop {
            if let Some(body) = decode_body(&mut self.read_buf, framing, &self.config)? {
                return Ok(body);
            }

            let bytes_read =
                match tokio::time::timeout(idle_timeout, self.stream.read_buf(&mut self.read_buf))
                    .await
                {
                    Ok(res) => res?,
                    Err(_) => return Err(Http1Error::Timeout),
                };
            if bytes_read == 0 {
                return Err(Http1Error::Parse(
                    "Unexpected EOF while reading HTTP request body".into(),
                ));
            }
        }
    }

    /// Reads and decodes the next full HTTP/1.1 request (head + body).
    pub async fn next_request(&mut self) -> Result<Option<Http1ServerRequest>, Http1Error> {
        if self.close_requested {
            return Ok(None);
        }

        self.compact_buffers();

        loop {
            if let Some(req) = decode_request(&mut self.read_buf, &self.config)? {
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

            let timeout_duration = if self.read_buf.is_empty() {
                std::time::Duration::from_millis(self.config.idle_timeout_ms)
            } else {
                std::time::Duration::from_millis(self.config.header_read_timeout_ms)
            };

            let bytes_read = match tokio::time::timeout(
                timeout_duration,
                self.stream.read_buf(&mut self.read_buf),
            )
            .await
            {
                Ok(res) => res?,
                Err(_) => {
                    if self.read_buf.is_empty() {
                        return Ok(None);
                    } else {
                        return Err(Http1Error::Timeout);
                    }
                }
            };
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

    /// Serializes and flushes an HTTP/1.1 downstream response back to the client.
    ///
    /// Automatically streams the response body with zero-copy transmission.
    pub async fn send_response(
        &mut self,
        response: &Http1ServerResponse,
    ) -> Result<(), Http1Error> {
        let close = send_response(
            &mut self.stream,
            &mut self.write_buf,
            response,
            self.close_requested,
        )
        .await?;

        if close {
            self.close_requested = true;
        }

        Ok(())
    }

    /// Serializes and flushes an upstream client response directly downstream.
    pub async fn send_client_response(
        &mut self,
        response: &Http1ClientResponse,
    ) -> Result<(), Http1Error> {
        let server_resp = Http1ServerResponse::new(
            response.status,
            response.version,
            response.headers.clone(),
            response.body.clone(),
        );
        self.send_response(&server_resp).await
    }

    /// Serializes and flushes response head and body separately.
    pub async fn send_response_parts(
        &mut self,
        head: &Http1ServerResponseHead,
        body: &Body,
    ) -> Result<(), Http1Error> {
        let close = send_response_parts(
            &mut self.stream,
            &mut self.write_buf,
            head,
            body,
            self.close_requested,
        )
        .await?;

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
        head: &Http1ServerResponseHead,
    ) -> Result<(), Http1Error> {
        let close = send_response_head_chunked(
            &mut self.stream,
            &mut self.write_buf,
            head.version,
            head.status,
            &head.headers,
            self.close_requested,
        )
        .await?;

        if close {
            self.close_requested = true;
        }

        Ok(())
    }

    /// Sends the HTTP/1.1 response head configured for progressive chunked streaming from upstream client head.
    pub async fn send_client_response_head_chunked(
        &mut self,
        head: &Http1ClientResponseHead,
    ) -> Result<(), Http1Error> {
        let server_head =
            Http1ServerResponseHead::new(head.status, head.version, head.headers.clone());
        self.send_response_head_chunked(&server_head).await
    }

    /// Transmits raw body bytes directly downstream without chunked framing (for HTTP/1.0 fallback).
    pub async fn send_raw_bytes(&mut self, bytes: &[u8]) -> Result<(), Http1Error> {
        self.stream.write_all(bytes).await?;
        self.stream.flush().await?;
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
        use super::request::parse_single_chunk;
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

            let idle_timeout = std::time::Duration::from_millis(self.config.idle_timeout_ms);
            let bytes_read =
                match tokio::time::timeout(idle_timeout, self.stream.read_buf(&mut self.read_buf))
                    .await
                {
                    Ok(res) => res?,
                    Err(_) => return Err(Http1Error::Timeout),
                };
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

    /// Transmits `HTTP/1.1 100 Continue\r\n\r\n` to downstream stream (RFC 9110 §10.1.1).
    ///
    /// Unblocks clients that paused transmission waiting for server confirmation before sending body.
    #[inline]
    pub async fn send_100_continue(&mut self) -> Result<(), Http1Error> {
        static HTTP_100_CONTINUE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";
        self.stream.write_all(HTTP_100_CONTINUE).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Marks this connection to close cleanly after the current response is finished.
    #[inline]
    pub fn mark_close(&mut self) {
        self.close_requested = true;
    }

    /// Returns whether this connection was flagged to close.
    #[inline]
    pub const fn is_closed(&self) -> bool {
        self.close_requested
    }

    /// Performs a lingering close on downstream stream (RFC 9112 lingering close).
    ///
    /// Shuts down the write half to transmit FIN, then drains any lingering unread incoming bytes
    /// within a short timeout (100ms) to prevent TCP RST from dropping downstream response data.
    pub async fn lingering_close(&mut self) {
        let _ = self.stream.shutdown().await;
        let mut discard = [0u8; 1024];
        let _ = tokio::time::timeout(std::time::Duration::from_millis(100), async {
            loop {
                match self.stream.read(&mut discard).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        })
        .await;
    }
}
