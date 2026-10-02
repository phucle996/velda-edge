//! Dedicated HTTP/3 wire pipe modules isolated by streaming strategy.
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

use std::net::SocketAddr;

use velda_core::{L7Request, L7Response, StreamingMode};

pub use buffered::pipe_buffered;
pub use client_stream::pipe_client_stream;
pub use duplex::pipe_duplex;
pub use server_stream::pipe_server_stream;

use crate::config::Http3Config;
use crate::error::Http3Error;

/// Discrete streaming strategies for HTTP/3 request handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Http3PipeStrategy {
    /// Non-streaming: Full request body and response body buffered in RAM.
    Buffered,
    /// Server streaming: Request buffered, response pumped progressively.
    ServerStream,
    /// Client streaming: Request pumped progressively, response buffered.
    ClientStream,
    /// Full-duplex: Both request and response streamed concurrently.
    Duplex,
}

impl Http3PipeStrategy {
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

    /// Dispatches an HTTP/3 request through the derived streaming pipe.
    pub async fn dispatch(
        self,
        req: &L7Request,
        target: SocketAddr,
        config: &Http3Config,
    ) -> Result<L7Response, Http3Error> {
        match self {
            Self::Buffered => pipe_buffered(req, target, config).await,
            Self::ServerStream => pipe_server_stream(req, target, config).await,
            Self::ClientStream => pipe_client_stream(req, target, config).await,
            Self::Duplex => pipe_duplex(req, target, config).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::{Body, StreamingMode};

    #[test]
    fn test_strategy_derivation_from_streaming_mode() {
        assert_eq!(
            Http3PipeStrategy::from_streaming(StreamingMode::new(false, false)),
            Http3PipeStrategy::Buffered
        );
        assert_eq!(
            Http3PipeStrategy::from_streaming(StreamingMode::new(false, true)),
            Http3PipeStrategy::ServerStream
        );
        assert_eq!(
            Http3PipeStrategy::from_streaming(StreamingMode::new(true, false)),
            Http3PipeStrategy::ClientStream
        );
        assert_eq!(
            Http3PipeStrategy::from_streaming(StreamingMode::new(true, true)),
            Http3PipeStrategy::Duplex
        );
    }

    #[tokio::test]
    async fn test_pipe_buffered_payload_too_large() {
        let config = Http3Config {
            max_body_size: 10,
            ..Http3Config::auto()
        };
        let req = L7Request::new(
            http::Method::POST,
            http::Uri::from_static("/upload"),
            http::Version::HTTP_3,
            http::HeaderMap::new(),
            Body::Bytes(bytes::Bytes::from_static(b"0123456789too-large-payload")),
        );
        let target: SocketAddr = "127.0.0.1:4433".parse().unwrap();
        let res = pipe_buffered(&req, target, &config).await;
        assert!(matches!(res, Err(Http3Error::PayloadTooLarge(27))));
    }
}
