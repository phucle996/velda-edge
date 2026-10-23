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
    HeadEncodeOptions, Http1ServerResponse, Http1ServerResponseHead, encode_chunked_head,
    encode_head, send_chunked_head, send_head_coalesced, send_parts, send_response,
};
use crate::client::response::{Http1ClientResponse, Http1ClientResponseHead};
use crate::config::Http1Config;
use crate::error::Http1Error;

/// An active HTTP/1.1 connection over an asynchronous downstream stream.
#[repr(align(64))]
pub struct Http1ServerConnection<IO> {
    pub stream: IO,
    pub read_buf: BytesMut,
    pub write_buf: BytesMut,
    pub upstream_read_buf: BytesMut,
    pub upstream_write_buf: BytesMut,
    pub close_requested: bool,
    pub current_request_id: Option<http::header::HeaderValue>,
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
            upstream_read_buf: BytesMut::with_capacity(config.initial_buffer_capacity),
            upstream_write_buf: BytesMut::with_capacity(4096),
            close_requested: false,
            current_request_id: None,
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
        if self.upstream_read_buf.is_empty()
            && self.upstream_read_buf.capacity() > self.config.shrink_threshold
        {
            self.upstream_read_buf = BytesMut::with_capacity(self.config.initial_buffer_capacity);
        }
        if self.upstream_write_buf.is_empty() && self.upstream_write_buf.capacity() > 16_384 {
            self.upstream_write_buf = BytesMut::with_capacity(4096);
        }
    }

    /// Reads and decodes the next HTTP/1.1 request head from the downstream stream.
    ///
    /// Allows callers (e.g. plugin pipeline, auth, WAF header checks) to evaluate
    /// headers and reject malicious or unauthorized requests before reading any body bytes.
    pub async fn next_request_head(
        &mut self,
    ) -> Result<Option<(Http1ServerRequestHead, Http1BodyFraming)>, Http1Error> {
        self.current_request_id = None;
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

            let bytes_read =
                match read_with_timeout(&mut self.stream, &mut self.read_buf, timeout_duration)
                    .await
                {
                    Ok(res) => res,
                    Err(Http1Error::Timeout) => {
                        if self.read_buf.is_empty() {
                            return Ok(None);
                        } else {
                            return Err(Http1Error::Timeout);
                        }
                    }
                    Err(e) => return Err(e),
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

            if self.read_buf.len() > self.config.max_body_size.saturating_add(4096) {
                return Err(Http1Error::PayloadTooLarge(self.read_buf.len()));
            }

            let target_reserve = match framing {
                Http1BodyFraming::ContentLength(total) => {
                    let remaining = total.saturating_sub(self.read_buf.len());
                    remaining.min(self.config.client_body_buffer_size)
                }
                _ => self.config.client_body_buffer_size,
            };
            if self.read_buf.capacity() - self.read_buf.len() < target_reserve {
                self.read_buf.reserve(target_reserve);
            }

            let bytes_read =
                match read_with_timeout(&mut self.stream, &mut self.read_buf, idle_timeout).await {
                    Ok(res) => res,
                    Err(Http1Error::Timeout) => return Err(Http1Error::Timeout),
                    Err(e) => return Err(e),
                };
            if bytes_read == 0 {
                return Err(Http1Error::Parse(
                    "Unexpected EOF while reading HTTP request body".into(),
                ));
            }
        }
    }

    /// Reads and decodes request body directly into a pre-warmed user-space slab leased from `slab_pool`.
    ///
    /// # Zero-Allocation Invariant
    /// 1. If `framing` is Content-Length and `len <= 65536`, borrows an eager MMU-mapped physical slab
    ///    from `slab_pool`.
    /// 2. Drains any already-buffered bytes from `self.read_buf` into `lease[..buffered]`.
    /// 3. Streams the remaining bytes directly from `self.stream` into `lease[buffered..total]` (Zero heap allocations).
    /// 4. If `slab_pool` is unavailable, payload exceeds 64KB, or framing is chunked, falls back seamlessly
    ///    to standard buffered decoding (`read_body`).
    pub async fn read_body_into_slab(
        &mut self,
        framing: Http1BodyFraming,
        slab_pool: Option<&std::sync::Arc<velda_core::PayloadSlabPool>>,
    ) -> Result<Body, Http1Error> {
        if framing == Http1BodyFraming::Empty {
            return Ok(Body::Empty);
        }

        if let Http1BodyFraming::ContentLength(total_len) = framing {
            if total_len == 0 {
                return Ok(Body::Empty);
            }
            if total_len > self.config.max_body_size {
                return Err(Http1Error::PayloadTooLarge(total_len));
            }

            if let Some(pool) = slab_pool
                && let Some(mut lease) = pool.acquire_for_size(total_len)
            {
                let already_buffered = self.read_buf.len().min(total_len);
                if already_buffered > 0 {
                    let drained = self.read_buf.split_to(already_buffered);
                    lease[..already_buffered].copy_from_slice(&drained);
                }

                let mut current_offset = already_buffered;
                let idle_timeout = std::time::Duration::from_millis(self.config.idle_timeout_ms);

                while current_offset < total_len {
                    let dest = &mut lease[current_offset..total_len];
                    let n = match tokio::time::timeout(idle_timeout, self.stream.read(dest)).await {
                        Ok(Ok(n)) => n,
                        Ok(Err(e)) => return Err(Http1Error::Io(e)),
                        Err(_) => return Err(Http1Error::Timeout),
                    };
                    if n == 0 {
                        return Err(Http1Error::Parse(
                            "Unexpected EOF while reading HTTP request body into slab".into(),
                        ));
                    }
                    current_offset += n;
                }

                return Ok(Body::from_slab(lease, total_len));
            }
        }

        // Fallback for Chunked, >64KB, or when pool ceiling is reached
        self.read_body(framing).await
    }

    /// Reads and decodes the next full HTTP/1.1 request (head + body).
    pub async fn next_request(&mut self) -> Result<Option<Http1ServerRequest>, Http1Error> {
        self.current_request_id = None;
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

            if self.read_buf.len()
                > self
                    .config
                    .max_header_size
                    .saturating_add(self.config.max_body_size)
                    .saturating_add(4096)
            {
                return Err(Http1Error::PayloadTooLarge(self.read_buf.len()));
            }

            let timeout_duration = if self.read_buf.is_empty() {
                std::time::Duration::from_millis(self.config.idle_timeout_ms)
            } else {
                std::time::Duration::from_millis(self.config.header_read_timeout_ms)
            };

            let bytes_read =
                match read_with_timeout(&mut self.stream, &mut self.read_buf, timeout_duration)
                    .await
                {
                    Ok(res) => res,
                    Err(Http1Error::Timeout) => {
                        if self.read_buf.is_empty() {
                            return Ok(None);
                        } else {
                            return Err(Http1Error::Timeout);
                        }
                    }
                    Err(e) => return Err(e),
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

    /// Sets the authoritative L7 Request ID for the currently active request transaction.
    #[inline]
    pub fn set_request_id(&mut self, request_id: Option<http::header::HeaderValue>) {
        self.current_request_id = request_id;
    }

    /// Returns the authoritative L7 Request ID for the current request transaction, if any.
    #[inline]
    pub fn request_id(&self) -> Option<&http::header::HeaderValue> {
        self.current_request_id.as_ref()
    }

    /// Serializes response head into internal write_buf, automatically injecting the connection's
    /// authoritative L7 Request ID header if not already present in `headers`.
    #[inline]
    pub fn encode_head(
        &mut self,
        version: http::Version,
        status: http::StatusCode,
        headers: &http::HeaderMap,
        body_len: Option<usize>,
        force_close: bool,
    ) {
        encode_head(
            version,
            status,
            headers,
            HeadEncodeOptions {
                body_len,
                force_close,
                request_id: self.current_request_id.as_ref(),
            },
            &mut self.write_buf,
        );
    }

    /// Backward-compatibility alias for `encode_head`.
    #[inline]
    pub fn encode_response_head(
        &mut self,
        version: http::Version,
        status: http::StatusCode,
        headers: &http::HeaderMap,
        body_len: Option<usize>,
        force_close: bool,
    ) {
        self.encode_head(version, status, headers, body_len, force_close);
    }

    /// Serializes chunked response head into internal write_buf, automatically injecting the
    /// connection's authoritative L7 Request ID header if not already present in `headers`.
    #[inline]
    pub fn encode_chunked_head(
        &mut self,
        version: http::Version,
        status: http::StatusCode,
        headers: &http::HeaderMap,
        force_close: bool,
    ) {
        encode_chunked_head(
            version,
            status,
            headers,
            HeadEncodeOptions {
                body_len: None,
                force_close,
                request_id: self.current_request_id.as_ref(),
            },
            &mut self.write_buf,
        );
    }

    /// Backward-compatibility alias for `encode_chunked_head`.
    #[inline]
    pub fn encode_response_head_chunked(
        &mut self,
        version: http::Version,
        status: http::StatusCode,
        headers: &http::HeaderMap,
        force_close: bool,
    ) {
        self.encode_chunked_head(version, status, headers, force_close);
    }

    /// Serializes and flushes an HTTP/1.1 downstream response back to the client.
    ///
    /// Automatically streams the response body with zero-copy transmission and enforces
    /// the authoritative L7 `x-request-id` header invariant.
    pub async fn send_response(
        &mut self,
        response: &Http1ServerResponse,
    ) -> Result<(), Http1Error> {
        let close = send_response(
            &mut self.stream,
            &mut self.write_buf,
            response,
            self.close_requested,
            self.current_request_id.as_ref(),
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

    /// Serializes and flushes response head and body separately with authoritative L7 request ID injection.
    pub async fn send_parts(
        &mut self,
        head: &Http1ServerResponseHead,
        body: &Body,
    ) -> Result<(), Http1Error> {
        let close = send_parts(
            &mut self.stream,
            &mut self.write_buf,
            head,
            body,
            self.close_requested,
            self.current_request_id.as_ref(),
        )
        .await?;

        if close {
            self.close_requested = true;
        }

        Ok(())
    }

    /// Backward-compatibility alias for `send_parts`.
    #[inline]
    pub async fn send_response_parts(
        &mut self,
        head: &Http1ServerResponseHead,
        body: &Body,
    ) -> Result<(), Http1Error> {
        self.send_parts(head, body).await
    }

    /// Sends the HTTP/1.1 response head configured for progressive chunked streaming.
    ///
    /// Sets `Transfer-Encoding: chunked`, enforces authoritative L7 request ID injection,
    /// and immediately flushes the head to the client.
    pub async fn send_chunked_head(
        &mut self,
        head: &Http1ServerResponseHead,
    ) -> Result<(), Http1Error> {
        let close = send_chunked_head(
            &mut self.stream,
            &mut self.write_buf,
            head.version,
            head.status,
            &head.headers,
            HeadEncodeOptions {
                body_len: None,
                force_close: self.close_requested,
                request_id: self.current_request_id.as_ref(),
            },
        )
        .await?;

        if close {
            self.close_requested = true;
        }

        Ok(())
    }

    /// Backward-compatibility alias for `send_chunked_head`.
    #[inline]
    pub async fn send_response_head_chunked(
        &mut self,
        head: &Http1ServerResponseHead,
    ) -> Result<(), Http1Error> {
        self.send_chunked_head(head).await
    }

    /// Sends the HTTP/1.1 response head configured for progressive chunked streaming from upstream client head.
    pub async fn send_client_response_head_chunked(
        &mut self,
        head: &Http1ClientResponseHead,
    ) -> Result<(), Http1Error> {
        let server_head =
            Http1ServerResponseHead::new(head.status, head.version, head.headers.clone());
        self.send_chunked_head(&server_head).await
    }

    /// Serializes and flushes response head with chunked framing, optionally coalescing
    /// an initial chunk payload and terminal chunk into the same wire transmission.
    pub async fn send_head_coalesced(
        &mut self,
        head: &Http1ServerResponseHead,
        initial_chunk: Option<&[u8]>,
        is_terminated: bool,
    ) -> Result<bool, Http1Error> {
        let close = send_head_coalesced(
            &mut self.stream,
            &mut self.write_buf,
            head,
            initial_chunk,
            is_terminated,
            HeadEncodeOptions {
                body_len: None,
                force_close: self.close_requested,
                request_id: self.current_request_id.as_ref(),
            },
        )
        .await?;

        if close {
            self.close_requested = true;
        }

        Ok(close)
    }

    /// Backward-compatibility alias for `send_head_coalesced`.
    #[inline]
    pub async fn send_response_head_chunked_coalesced(
        &mut self,
        head: &Http1ServerResponseHead,
        initial_chunk: Option<&[u8]>,
        is_terminated: bool,
    ) -> Result<bool, Http1Error> {
        self.send_head_coalesced(head, initial_chunk, is_terminated)
            .await
    }

    /// Transmits raw body bytes directly downstream without chunked framing (for HTTP/1.0 fallback).
    pub async fn send_raw_bytes(&mut self, bytes: &[u8]) -> Result<(), Http1Error> {
        self.stream.write_all(bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Reads the next progressive chunk from the downstream client if the body is chunked.
    ///
    /// Returns `Ok(Some(bytes))` for each chunk payload, and `Ok(None)` when the terminal
    /// chunk (`0\r\n\r\n`) is reached.
    pub async fn read_next_chunk(&mut self) -> Result<Option<bytes::Bytes>, Http1Error> {
        loop {
            if let Some(res) = crate::wire::decode_chunk(&mut self.read_buf)? {
                return Ok(res);
            }

            let idle_timeout = std::time::Duration::from_millis(self.config.idle_timeout_ms);
            let target_reserve = self.config.client_body_buffer_size;
            if self.read_buf.is_empty() {
                if self.read_buf.capacity() < target_reserve
                    || self.read_buf.capacity() > target_reserve * 2
                {
                    self.read_buf = BytesMut::with_capacity(target_reserve);
                }
            } else if self.read_buf.capacity() - self.read_buf.len() < target_reserve {
                self.read_buf.reserve(target_reserve);
            }
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

/// Fast-path socket reader: polls stream immediately without registering a timer in Tokio's
/// timer wheel if incoming data has already arrived in the kernel socket receive buffer.
/// Falls back to `tokio::time::timeout` only when the socket genuinely would block.
#[inline]
pub(crate) async fn read_with_timeout<IO>(
    stream: &mut IO,
    buf: &mut BytesMut,
    timeout_duration: std::time::Duration,
) -> Result<usize, Http1Error>
where
    IO: AsyncRead + Unpin,
{
    if buf.capacity().saturating_sub(buf.len()) < 1024 {
        buf.reserve(4096);
    }

    let poll_res = std::future::poll_fn(|cx| {
        let mut read_buf = tokio::io::ReadBuf::uninit(buf.spare_capacity_mut());
        match std::pin::Pin::new(&mut *stream).poll_read(cx, &mut read_buf) {
            std::task::Poll::Ready(Ok(())) => {
                let filled = read_buf.filled().len();
                unsafe {
                    let new_len = buf.len() + filled;
                    buf.set_len(new_len);
                }
                std::task::Poll::Ready(Some(Ok(filled)))
            }
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Some(Err(Http1Error::Io(e)))),
            std::task::Poll::Pending => std::task::Poll::Ready(None),
        }
    })
    .await;

    if let Some(res) = poll_res {
        return res;
    }

    match tokio::time::timeout(timeout_duration, stream.read_buf(buf)).await {
        Ok(res) => Ok(res?),
        Err(_) => Err(Http1Error::Timeout),
    }
}
