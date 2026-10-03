//! Dedicated HTTP/2 wire pipe modules isolated by streaming strategy.
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

use velda_core::StreamingMode;

pub use buffered::pipe_buffered;
pub use client_stream::pipe_client_stream;
pub use duplex::pipe_duplex;
pub use server_stream::pipe_server_stream;

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
}
