//! # Low-Level HTTP/1.1 Wire Protocol Baseline (RFC 9112)
//!
//! **Architectural Role**: This module is the pure wire-framing engine ("thợ cơ khí đường truyền").
//! It operates strictly on established HTTP/1.1 client streams without any knowledge of:
//! - Gateway routing tables or listeners
//! - Upstream connection pool leasing or lifecycle
//! - Load balancing algorithms
//! - Proxy metadata enrichment (`X-Forwarded-*`, `Host`, etc.)
//!
//! For high-level Gateway orchestration, connection leasing, self-healing, and
//! cross-protocol bridging, see `velda_edge::pipeline`.
//!
//! Submodules:
//! - `buffered`: Pure in-memory request-response forwarding for REST APIs.
//! - `server_stream`: Progressive response streaming (SSE, LLM tokens, large downloads).
//! - `client_stream`: Chunked request body upload with single response.
//! - `duplex`: Concurrent bidirectional streaming (full-duplex / tunnels).

pub mod buffered;
pub mod client_stream;
pub mod duplex;
pub mod server_stream;

use velda_core::StreamingMode;

pub use buffered::pipe_buffered;
pub use client_stream::pipe_client_stream;
pub use duplex::pipe_duplex;
pub use server_stream::pipe_server_stream;

use crate::error::Http1Error;

/// Discrete streaming strategies for HTTP/1.1 request handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Http1PipeStrategy {
    /// Non-streaming: Full request body and response body buffered in RAM.
    Buffered,
    /// Server streaming: Request buffered, response pumped progressively as chunks.
    ServerStream,
    /// Client streaming: Request pumped progressively as chunks, response buffered.
    ClientStream,
    /// Full-duplex: Both request and response streamed concurrently.
    Duplex,
}

impl Http1PipeStrategy {
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

    /// Resolves the concrete strategy from listener capabilities and upstream requirements.
    ///
    /// # Errors
    /// Returns [`Http1Error::StreamingViolation`] if upstream requires streaming capabilities
    /// that are disabled on the ingress listener.
    pub fn resolve(
        listener_streaming: StreamingMode,
        upstream_streaming: StreamingMode,
    ) -> Result<Self, Http1Error> {
        if upstream_streaming.server && !listener_streaming.server {
            return Err(Http1Error::StreamingViolation(
                "Upstream requires server streaming, but listener has streaming.server disabled"
                    .into(),
            ));
        }

        if upstream_streaming.client && !listener_streaming.client {
            return Err(Http1Error::StreamingViolation(
                "Upstream requires client streaming, but listener has streaming.client disabled"
                    .into(),
            ));
        }

        Ok(Self::from_streaming(upstream_streaming))
    }
}

pub use crate::client::header::{HOP_BY_HOP, sanitize_headers};

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;

    #[test]
    fn test_resolve_pipe_strategy() {
        let disabled = StreamingMode::DISABLED;
        let server = StreamingMode::SERVER;
        let client = StreamingMode::CLIENT;
        let bidi = StreamingMode::new(true, true);

        // 1. Both disabled -> Buffered
        assert_eq!(
            Http1PipeStrategy::resolve(disabled, disabled).unwrap(),
            Http1PipeStrategy::Buffered
        );

        // 2. Listener supports server, upstream requires server -> ServerStream
        assert_eq!(
            Http1PipeStrategy::resolve(server, server).unwrap(),
            Http1PipeStrategy::ServerStream
        );

        // 3. Listener supports bidi, upstream requires server -> ServerStream
        assert_eq!(
            Http1PipeStrategy::resolve(bidi, server).unwrap(),
            Http1PipeStrategy::ServerStream
        );

        // 4. Listener supports bidi, upstream requires bidi -> Duplex
        assert_eq!(
            Http1PipeStrategy::resolve(bidi, bidi).unwrap(),
            Http1PipeStrategy::Duplex
        );

        // 5. Upstream requires server streaming, but listener has it disabled -> Error
        assert!(Http1PipeStrategy::resolve(disabled, server).is_err());

        // 6. Upstream requires client streaming, but listener has it disabled -> Error
        assert!(Http1PipeStrategy::resolve(disabled, client).is_err());
    }

    #[test]
    fn test_sanitize_hop_by_hop_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONNECTION,
            "custom-header, keep-alive".parse().unwrap(),
        );
        headers.insert("keep-alive", "timeout=5".parse().unwrap());
        headers.insert(http::header::UPGRADE, "websocket".parse().unwrap());
        headers.insert("custom-header", "custom-value".parse().unwrap());
        headers.insert(
            http::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );

        sanitize_headers(&mut headers);

        assert!(!headers.contains_key(http::header::CONNECTION));
        assert!(!headers.contains_key("keep-alive"));
        assert!(!headers.contains_key(http::header::UPGRADE));
        assert!(!headers.contains_key("custom-header"));
        assert!(headers.contains_key(http::header::CONTENT_TYPE));
    }
}
