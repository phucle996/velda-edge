//! Discrete streaming execution strategy for HTTP/2 request handling.

use velda_core::StreamingMode;

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
