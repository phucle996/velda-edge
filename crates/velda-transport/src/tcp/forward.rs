//! Bidirectional byte forwarding for L4 stream proxying.

use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::connection::Connection;
use crate::error::{Result, TransportError};

/// Transfer statistics for bidirectional stream forwarding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransferStats {
    /// Bytes transmitted from client to server (upstream).
    pub client_to_server_bytes: u64,
    /// Bytes transmitted from server (upstream) to client.
    pub server_to_client_bytes: u64,
}

impl TransferStats {
    /// Creates a new transfer statistics record.
    #[inline]
    pub const fn new(client_to_server_bytes: u64, server_to_client_bytes: u64) -> Self {
        Self {
            client_to_server_bytes,
            server_to_client_bytes,
        }
    }

    /// Returns the total bytes transferred across both directions.
    #[inline]
    pub const fn total_bytes(&self) -> u64 {
        self.client_to_server_bytes + self.server_to_client_bytes
    }
}

/// Forwards bytes bidirectionally between two asynchronous streams using custom buffer sizes.
pub async fn forward_bidirectional_with_sizes<C, S>(
    client: &mut C,
    server: &mut S,
    client_buf_size: usize,
    server_buf_size: usize,
) -> Result<TransferStats>
where
    C: AsyncRead + AsyncWrite + Unpin + ?Sized,
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let (client_to_server, server_to_client) =
        tokio::io::copy_bidirectional_with_sizes(client, server, client_buf_size, server_buf_size)
            .await
            .map_err(TransportError::Forward)?;

    Ok(TransferStats {
        client_to_server_bytes: client_to_server,
        server_to_client_bytes: server_to_client,
    })
}

/// Forwards bytes bidirectionally between two asynchronous streams using
/// [`tokio::io::copy_bidirectional`] with automatic half-close (TCP FIN) handling.
///
/// Uses standard 64 KB buffers for high-bandwidth L4 stream forwarding.
pub async fn forward_bidirectional<C, S>(client: &mut C, server: &mut S) -> Result<TransferStats>
where
    C: AsyncRead + AsyncWrite + Unpin + ?Sized,
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    forward_bidirectional_with_sizes(client, server, 65536, 65536).await
}

/// Forwards bytes bidirectionally between an active [`Connection`] and an upstream [`TcpStream`]
/// using the specified buffer size (e.g. from [`super::config::TcpListenerConfig::copy_buffer_size`]).
pub async fn forward_connection_with_size(
    mut client: Connection,
    mut server: TcpStream,
    buffer_size: usize,
) -> Result<TransferStats> {
    forward_bidirectional_with_sizes(client.stream_mut(), &mut server, buffer_size, buffer_size)
        .await
}

/// Forwards bytes bidirectionally between an active [`Connection`] and an upstream [`TcpStream`].
///
/// Updates the internal byte counters of the client [`Connection`].
pub async fn forward_connection(client: Connection, server: TcpStream) -> Result<TransferStats> {
    forward_connection_with_size(client, server, 65536).await
}

/// Connects to a target upstream address and pumps bytes bidirectionally with the client connection.
pub async fn connect_and_forward(
    client: Connection,
    upstream_addr: SocketAddr,
) -> Result<TransferStats> {
    let server = TcpStream::connect(upstream_addr)
        .await
        .map_err(|e| TransportError::Connect {
            addr: upstream_addr,
            source: e,
        })?;

    let _ = server.set_nodelay(true);

    forward_connection(client, server).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer_stats() {
        let stats = TransferStats::new(1024, 2048);
        assert_eq!(stats.client_to_server_bytes, 1024);
        assert_eq!(stats.server_to_client_bytes, 2048);
        assert_eq!(stats.total_bytes(), 3072);
    }
}
