//! Discrete streaming strategies for gRPC RPC request handling over TCP.

use velda_core::StreamingMode;

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
