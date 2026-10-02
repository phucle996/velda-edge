//! Upstream HTTP/2 Client Connection Establishment (RFC 9113).
//!
//! Handles TCP connection establishment, TLS/generic stream handshakes, and background H2
//! connection driver execution.

use std::net::SocketAddr;

use bytes::Bytes;
use tokio::net::TcpStream;

use crate::config::Http2Config;
use crate::error::Http2Error;

/// Connects to the upstream target over TCP and performs the HTTP/2 client handshake.
///
/// Spawns the H2 connection driver onto a background Tokio task.
pub async fn connect(
    target: SocketAddr,
    config: &Http2Config,
) -> Result<h2::client::SendRequest<Bytes>, Http2Error> {
    let stream = TcpStream::connect(target).await?;
    connect_stream(stream, config).await
}

/// Performs the HTTP/2 client handshake over an arbitrary asynchronous I/O stream (e.g. TLS or TCP).
///
/// Spawns the H2 connection driver onto a background Tokio task.
pub async fn connect_stream<IO>(
    stream: IO,
    config: &Http2Config,
) -> Result<h2::client::SendRequest<Bytes>, Http2Error>
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut builder = h2::client::Builder::default();
    builder
        .initial_connection_window_size(config.initial_connection_window_size)
        .initial_window_size(config.initial_stream_window_size)
        .max_concurrent_streams(config.max_concurrent_streams)
        .max_frame_size(config.max_frame_size)
        .max_header_list_size(config.max_header_list_size)
        .enable_push(config.enable_push)
        .max_send_buffer_size(config.max_send_buffer_size)
        .max_concurrent_reset_streams(config.max_consecutive_resets as usize)
        .max_pending_accept_reset_streams(config.max_consecutive_resets as usize);

    let (client, h2_conn) = builder.handshake(stream).await?;
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    Ok(client)
}
