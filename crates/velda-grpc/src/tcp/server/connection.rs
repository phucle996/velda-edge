//! Downstream gRPC server connection lifecycle and stream accept loop.
//!
//! Powered directly by native `velda-http2` ([`Http2ServerConnection`]) with single-pass
//! HPACK decoding into [`velda_core::HeaderWorkspace`].

use tokio::io::{AsyncRead, AsyncWrite};
use velda_http2::proto::hpack::H2ForwardingAddrs;
use velda_http2::server::connection::Http2ServerConnection;

use super::responder::GrpcResponder;
use super::stream::GrpcServerStream;
use crate::config::GrpcConfig;
use crate::outcome::GrpcOutcome;

/// An active gRPC downstream server connection backed by native HTTP/2 framing.
pub struct GrpcServerConnection {
    inner: Http2ServerConnection,
}

impl GrpcServerConnection {
    /// Performs HTTP/2 server handshake over the transport stream configuring
    /// limits and stream concurrency from [`GrpcConfig`].
    pub async fn handshake<IO>(stream: IO, config: &GrpcConfig) -> Result<Self, GrpcOutcome>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let h2_cfg = config.to_http2_config();
        let inner = Http2ServerConnection::handshake(stream, h2_cfg)
            .await
            .map_err(GrpcOutcome::from_http2)?;
        Ok(Self { inner })
    }

    /// Performs HTTP/2 server handshake with 1-pass proxy forwarding header enrichment
    /// executed directly during HPACK ingress decode.
    pub async fn handshake_with_forwarding<IO>(
        stream: IO,
        config: &GrpcConfig,
        forwarding: H2ForwardingAddrs,
    ) -> Result<Self, GrpcOutcome>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let h2_cfg = config.to_http2_config();
        let inner = Http2ServerConnection::handshake_with_forwarding(stream, h2_cfg, forwarding)
            .await
            .map_err(GrpcOutcome::from_http2)?;
        Ok(Self { inner })
    }

    /// Non-blocking accept returning `Some(...)` if an incoming gRPC request stream is queued.
    #[inline]
    pub fn try_accept(&mut self) -> Result<Option<GrpcServerStream>, GrpcOutcome> {
        let Some(res) = self.inner.try_accept_streaming_request() else {
            return Ok(None);
        };
        match res.map_err(GrpcOutcome::from_http2)? {
            Some((head, receiver, responder)) => Ok(Some(GrpcServerStream {
                head,
                receiver,
                respond: GrpcResponder::new(responder),
            })),
            None => Ok(None),
        }
    }

    /// Accepts the next incoming gRPC stream from the client without buffering the body.
    pub async fn accept(&mut self) -> Result<Option<GrpcServerStream>, GrpcOutcome> {
        let Some((head, receiver, responder)) = self
            .inner
            .accept_streaming_request()
            .await
            .map_err(GrpcOutcome::from_http2)?
        else {
            return Ok(None);
        };
        Ok(Some(GrpcServerStream {
            head,
            receiver,
            respond: GrpcResponder::new(responder),
        }))
    }
}
