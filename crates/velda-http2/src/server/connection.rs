//! Downstream HTTP/2 Server Connection Driver (RFC 9113).
//!
//! Coordinates H2 connection handshake, settings frame negotiation,
//! and multiplexed stream accept loop.

use bytes::Bytes;
use h2::server::{Builder, Connection};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::{IngressLimits, L7Request};

use super::decode::{Http2StreamReceiver, decode_request};
use super::encode::Http2Responder;
use super::request::{Http2Request, Http2RequestHead};
use crate::config::Http2Config;
use crate::error::Http2Error;

/// An active HTTP/2 downstream connection over an asynchronous stream.
pub struct Http2ServerConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    connection: Connection<IO, Bytes>,
    limits: IngressLimits,
    config: Http2Config,
}

impl<IO> Http2ServerConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Performs the HTTP/2 server handshake over the underlying I/O stream
    /// with default configuration and mandatory ingress safety limits.
    pub async fn handshake(stream: IO, limits: IngressLimits) -> Result<Self, Http2Error> {
        Self::handshake_with_config(stream, limits, Http2Config::default()).await
    }

    /// Performs the HTTP/2 server handshake with explicit [`Http2Config`] settings.
    pub async fn handshake_with_config(
        stream: IO,
        limits: IngressLimits,
        config: Http2Config,
    ) -> Result<Self, Http2Error> {
        let mut builder = Builder::default();
        builder
            .initial_connection_window_size(config.initial_connection_window_size)
            .initial_window_size(config.initial_stream_window_size)
            .max_concurrent_streams(config.max_concurrent_streams)
            .max_frame_size(config.max_frame_size)
            .max_header_list_size(config.max_header_list_size);

        let connection = builder.handshake(stream).await?;
        Ok(Self {
            connection,
            limits,
            config,
        })
    }

    /// Returns a reference to the active ingress limits configured on this connection.
    #[inline]
    pub const fn limits(&self) -> &IngressLimits {
        &self.limits
    }

    /// Returns a reference to the HTTP/2 configuration of this connection.
    #[inline]
    pub const fn config(&self) -> &Http2Config {
        &self.config
    }

    /// Accepts the next multiplexed request stream as a native [`Http2Request`].
    ///
    /// Non-blocking for existing streams: each invocation retrieves a new stream
    /// and its dedicated [`Http2Responder`].
    pub async fn accept_h2_request(
        &mut self,
    ) -> Result<Option<(Http2Request, Http2Responder)>, Http2Error> {
        let Some(res) = self.connection.accept().await else {
            return Ok(None);
        };

        let (request, respond) = res?;
        let stream_id = respond.stream_id();
        let mut req = decode_request(request, &self.limits).await?;
        req.head.stream_id = Some(stream_id);

        Ok(Some((req, Http2Responder::new(respond))))
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
    ///
    /// Allows callers to evaluate headers immediately and process chunks progressively (e.g. streaming uploads,
    /// SSE dispatching, or pass-through proxying) without buffering the entire body into memory.
    pub async fn accept_streaming_request(
        &mut self,
    ) -> Result<Option<(Http2RequestHead, Http2StreamReceiver, Http2Responder)>, Http2Error> {
        let Some(res) = self.connection.accept().await else {
            return Ok(None);
        };

        let (request, respond) = res?;
        let stream_id = respond.stream_id();
        let (parts, body_stream) = request.into_parts();
        let head = Http2RequestHead::new(parts.method, parts.uri, parts.headers, Some(stream_id));
        let receiver = Http2StreamReceiver::new(body_stream, self.limits.max_body_size);
        let responder = Http2Responder::new(respond);

        Ok(Some((head, receiver, responder)))
    }
}
