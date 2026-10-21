//! Discrete streaming execution strategy for HTTP/1.1 request handling.

use velda_core::StreamingMode;

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

    /// Resolves the effective execution strategy for a specific request based on
    /// client upload framing (chunked) and client response requirement (SSE).
    #[inline]
    pub fn effective(self, is_chunked: bool, is_sse: bool) -> Self {
        match self {
            Self::Buffered => Self::Buffered,
            Self::ServerStream => {
                if is_sse {
                    Self::ServerStream
                } else {
                    Self::Buffered
                }
            }
            Self::ClientStream => {
                if is_chunked {
                    Self::ClientStream
                } else {
                    Self::Buffered
                }
            }
            Self::Duplex => {
                if is_chunked && is_sse {
                    Self::Duplex
                } else if is_chunked {
                    Self::ClientStream
                } else if is_sse {
                    Self::ServerStream
                } else {
                    Self::Buffered
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
