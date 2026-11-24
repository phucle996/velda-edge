//! Upstream HTTP/3 progressive streaming response handle (RFC 9114).

use http::{StatusCode, Version};
use velda_core::HeaderWorkspace;

/// Upstream HTTP/3 progressive streaming response handle.
pub struct Http3StreamResponse {
    /// HTTP status code.
    pub status: StatusCode,
    /// Protocol version (HTTP/3.0).
    pub version: Version,
    /// Response headers.
    pub headers: HeaderWorkspace,
    /// Progressive response DATA chunk receiver.
    pub chunk_rx: tokio::sync::mpsc::Receiver<Result<bytes::Bytes, crate::error::Http3Error>>,
}

impl Http3StreamResponse {
    /// Creates a new streaming response handle.
    #[inline]
    pub fn new(
        status: StatusCode,
        headers: HeaderWorkspace,
        chunk_rx: tokio::sync::mpsc::Receiver<Result<bytes::Bytes, crate::error::Http3Error>>,
    ) -> Self {
        Self {
            status,
            version: Version::HTTP_3,
            headers,
            chunk_rx,
        }
    }

    /// Asynchronously awaits the next response chunk.
    ///
    /// Returns `Some(Ok(bytes))` on chunk arrival, `Some(Err(e))` on protocol/stream error,
    /// or `None` on clean end of stream (FIN reached).
    #[inline]
    pub async fn next_chunk(&mut self) -> Option<Result<bytes::Bytes, crate::error::Http3Error>> {
        self.chunk_rx.recv().await
    }
}
