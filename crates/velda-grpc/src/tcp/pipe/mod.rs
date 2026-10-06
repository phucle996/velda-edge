//! Dedicated gRPC wire pipe modules isolated by streaming strategy.
//!
//! Submodules:
//! - `buffered`: Unary RPC pipe forwarding (1 request message, 1 response message).
//! - `server_stream`: Server streaming RPC pipe (1 request message, progressive response stream).
//! - `client_stream`: Client streaming RPC pipe (progressive request stream, 1 response message).
//! - `duplex`: Full-duplex bidirectional RPC pipe (concurrent request and response streaming).

pub mod buffered;
pub mod client_stream;
pub mod duplex;
pub mod server_stream;

use velda_core::StreamingMode;

pub use buffered::pipe_buffered;
pub use client_stream::pipe_client_stream;
pub use duplex::pipe_duplex;
pub use server_stream::pipe_server_stream;

/// Discrete streaming strategies for gRPC RPC request handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GrpcPipeStrategy {
    /// Unary RPC: Single request message, single response message.
    Buffered,
    /// Server streaming RPC: Single request message, progressive stream of response messages.
    ServerStream,
    /// Client streaming RPC: Progressive stream of request messages, single response message.
    ClientStream,
    /// Full-duplex bidirectional streaming RPC: Concurrent streaming in both directions.
    Duplex,
}

impl GrpcPipeStrategy {
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
