//! Downstream HTTP/2 Server Connection Driver (RFC 9113).
//!
//! Coordinates H2 connection handshake, settings frame negotiation,
//! and multiplexed stream accept loop.

use bytes::Bytes;
use h2::server::{Builder, Connection};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use velda_core::L7Request;

use super::request::{Http2ServerRequest, Http2ServerRequestHead, Http2StreamReceiver};
use super::response::Http2Responder;
use crate::config::Http2Config;
use crate::error::Http2Error;

/// Control Frame Flood Tracker (Anti-DoS Heuristic for HTTP/2).
///
/// Tracks total bytes read from transport against actual payload bytes received
/// across streams. If total control overhead exceeds payload + 1MB, detects flood
/// and aborts the connection.
#[derive(Debug, Default)]
#[repr(C)]
pub struct Http2FloodTracker {
    /// Total bytes read from the underlying transport stream.
    pub total_bytes: AtomicU64,
    /// Total payload/headers bytes accounted from incoming streams.
    pub payload_bytes: AtomicU64,
}

impl Http2FloodTracker {
    /// Creates a new flood tracker with zero counters.
    #[inline]
    pub fn new() -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            payload_bytes: AtomicU64::new(0),
        }
    }

    /// Records bytes read from the transport socket.
    #[inline(always)]
    pub fn on_bytes_read(&self, n: usize) {
        self.total_bytes.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Records payload bytes received in HEADERS or DATA frames.
    #[inline(always)]
    pub fn on_payload_read(&self, n: usize) {
        self.payload_bytes.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Returns the total bytes read so far.
    #[inline(always)]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Returns the total payload bytes accounted so far.
    #[inline(always)]
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes.load(Ordering::Relaxed)
    }

    /// Evaluates whether control frame flood exceeds the safety threshold:
    /// `(total_bytes >> 3) > payload_bytes + 1MB` (1,048,576 bytes).
    #[inline(always)]
    pub fn is_flood_detected(&self) -> bool {
        let total = self.total_bytes.load(Ordering::Relaxed);
        let payload = self.payload_bytes.load(Ordering::Relaxed);
        (total >> 3) > payload.saturating_add(1_048_576)
    }
}

/// Transparent I/O stream wrapper that records raw transport bytes read
/// and enforces control frame flood limits before passing frames to `h2`.
pub struct FloodGuardedStream<IO> {
    inner: IO,
    tracker: Arc<Http2FloodTracker>,
}

impl<IO> FloodGuardedStream<IO> {
    /// Wraps an underlying I/O stream with flood tracking.
    #[inline]
    pub fn new(inner: IO, tracker: Arc<Http2FloodTracker>) -> Self {
        Self { inner, tracker }
    }

    /// Returns a reference to the tracker.
    #[inline]
    pub fn tracker(&self) -> &Arc<Http2FloodTracker> {
        &self.tracker
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for FloodGuardedStream<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let prev_len = buf.filled().len();
        let res = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &res {
            let n = buf.filled().len().saturating_sub(prev_len);
            if n > 0 {
                self.tracker.on_bytes_read(n);
                if self.tracker.is_flood_detected() {
                    return Poll::Ready(Err(cold_flood_error()));
                }
            }
        }
        res
    }
}

#[cold]
#[inline(never)]
fn cold_flood_error() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "HTTP/2 control frame flood detected",
    )
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for FloodGuardedStream<IO> {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// An active HTTP/2 downstream connection over an asynchronous stream.
pub struct Http2ServerConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    connection: Connection<FloodGuardedStream<IO>, Bytes>,
    config: Http2Config,
    flood_tracker: Arc<Http2FloodTracker>,
}

impl<IO> Http2ServerConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Performs the HTTP/2 server handshake over the underlying I/O stream
    /// configured with [`Http2Config`] and anti-flood protection.
    pub async fn handshake(stream: IO, config: Http2Config) -> Result<Self, Http2Error> {
        let mut builder = Builder::default();
        builder
            .initial_connection_window_size(config.initial_connection_window_size)
            .initial_window_size(config.initial_stream_window_size)
            .max_concurrent_streams(config.max_concurrent_streams)
            .max_frame_size(config.max_frame_size)
            .max_header_list_size(config.max_header_list_size)
            .max_send_buffer_size(config.max_send_buffer_size)
            .max_concurrent_reset_streams(config.max_consecutive_resets as usize)
            .max_pending_accept_reset_streams(config.max_consecutive_resets as usize);

        let flood_tracker = Arc::new(Http2FloodTracker::new());
        let guarded_stream = FloodGuardedStream::new(stream, Arc::clone(&flood_tracker));

        let connection = builder.handshake(guarded_stream).await?;
        Ok(Self {
            connection,
            config,
            flood_tracker,
        })
    }

    /// Returns a reference to the HTTP/2 configuration of this connection.
    #[inline]
    pub const fn config(&self) -> &Http2Config {
        &self.config
    }

    /// Returns a reference to the flood tracker monitoring this connection.
    #[inline]
    pub fn flood_tracker(&self) -> &Arc<Http2FloodTracker> {
        &self.flood_tracker
    }

    /// Returns whether control frame flood has been detected on this connection.
    #[inline]
    pub fn is_flood_detected(&self) -> bool {
        self.flood_tracker.is_flood_detected()
    }

    /// Initiates a graceful shutdown of the HTTP/2 connection by sending a GOAWAY frame (RFC 9113).
    ///
    /// Notifies the client that no new streams will be accepted while allowing in-flight streams
    /// to drain and complete.
    #[inline]
    pub fn graceful_shutdown(&mut self) {
        self.connection.graceful_shutdown();
    }

    /// Accepts the next multiplexed request stream as a native [`Http2ServerRequest`].
    pub async fn accept_h2_request(
        &mut self,
    ) -> Result<Option<(Http2ServerRequest, Http2Responder)>, Http2Error> {
        if self.flood_tracker.is_flood_detected() {
            return Err(Http2Error::FloodDetected);
        }

        let Some(res) = self.connection.accept().await else {
            return Ok(None);
        };

        let (request, respond) = match res {
            Ok(pair) => pair,
            Err(e) => {
                if self.flood_tracker.is_flood_detected() {
                    return Err(Http2Error::FloodDetected);
                }
                return Err(Http2Error::from(e));
            }
        };

        let stream_id = respond.stream_id();
        let (parts, body_stream) = request.into_parts();
        let head =
            Http2ServerRequestHead::new(parts.method, parts.uri, parts.headers, Some(stream_id));
        self.flood_tracker
            .on_payload_read(head.estimated_header_bytes());

        let mut receiver = Http2StreamReceiver::new(body_stream, self.config.max_body_size)
            .with_flood_tracker(Arc::clone(&self.flood_tracker));
        let body = receiver.consume_all().await?;
        let mut req = Http2ServerRequest::new(head, body);
        req.head.stream_id = Some(stream_id);

        let responder =
            Http2Responder::new(respond).with_alt_svc(self.config.alt_svc_header_value());
        Ok(Some((req, responder)))
    }

    /// Accepts the next multiplexed request stream and converts it directly into a canonical [`L7Request`]
    /// along with its dedicated [`Http2Responder`].
    pub async fn accept_request(
        &mut self,
    ) -> Result<Option<(L7Request, Http2Responder)>, Http2Error> {
        let opt = self.accept_h2_request().await?;
        Ok(opt.map(|(req, responder)| (req.into_l7_request(), responder)))
    }

    /// Accepts the next multiplexed request stream as an incoming head, progressive stream receiver,
    /// and dedicated responder.
    pub async fn accept_streaming_request(
        &mut self,
    ) -> Result<Option<(Http2ServerRequestHead, Http2StreamReceiver, Http2Responder)>, Http2Error>
    {
        if self.flood_tracker.is_flood_detected() {
            return Err(Http2Error::FloodDetected);
        }

        let Some(res) = self.connection.accept().await else {
            return Ok(None);
        };

        let (request, respond) = match res {
            Ok(pair) => pair,
            Err(e) => {
                if self.flood_tracker.is_flood_detected() {
                    return Err(Http2Error::FloodDetected);
                }
                return Err(Http2Error::from(e));
            }
        };

        let stream_id = respond.stream_id();
        let (parts, body_stream) = request.into_parts();
        let head =
            Http2ServerRequestHead::new(parts.method, parts.uri, parts.headers, Some(stream_id));
        self.flood_tracker
            .on_payload_read(head.estimated_header_bytes());

        let receiver = Http2StreamReceiver::new(body_stream, self.config.max_body_size)
            .with_flood_tracker(Arc::clone(&self.flood_tracker));
        let responder =
            Http2Responder::new(respond).with_alt_svc(self.config.alt_svc_header_value());

        Ok(Some((head, receiver, responder)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flood_tracker_heuristics() {
        let tracker = Http2FloodTracker::new();
        assert!(!tracker.is_flood_detected());

        // Under 1MB control overhead: not detected
        tracker.on_bytes_read(1_000_000);
        assert!(!tracker.is_flood_detected());

        // 8MB control bytes with 0 payload: (8MB >> 3) = 1MB <= 1MB -> boundary
        tracker.on_bytes_read(7_000_000);
        assert!(!tracker.is_flood_detected());

        // Exceeds 1MB safety threshold without payload: (9MB >> 3) = 1.125MB > 1.0MB
        tracker.on_bytes_read(1_000_000);
        assert!(tracker.is_flood_detected());

        // With legitimate payload of 2MB: (9MB >> 3) = 1.125MB <= 2MB + 1MB -> flood cleared
        tracker.on_payload_read(2 * 1_048_576);
        assert!(!tracker.is_flood_detected());
    }
}
