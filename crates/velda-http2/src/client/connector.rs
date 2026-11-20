//! Upstream HTTP/2 Client Connection Establishment (RFC 9113).
//!
//! Handles TCP connection establishment, TLS/generic stream handshakes, and background H2
//! connection driver execution.

use bytes::Bytes;
use velda_tls::TlsClientEngine;

use crate::config::Http2Config;
use crate::error::Http2Error;

/// Connects directly to an HTTP/2 backend target `(target_ip, target_port_be)` via `velda-sock`.
///
/// TLS ALPN is validated strictly: `h2` must be successfully negotiated
/// (a negotiated value other than `h2` is rejected), never used to choose the protocol.
///
/// Spawns the H2 connection driver onto a background Tokio task.
pub async fn connect(
    target_ip: u128,
    target_port_be: u16,
    tls: Option<(&TlsClientEngine, &str)>,
    config: &Http2Config,
    timeout: Option<std::time::Duration>,
) -> Result<h2::client::SendRequest<Bytes>, Http2Error> {
    let connect_timeout = timeout.unwrap_or(std::time::Duration::from_secs(10));
    let stream =
        velda_sock::connect::connect_tcp_u128(target_ip, target_port_be, connect_timeout).await?;

    let Some((engine, sni)) = tls else {
        // Enforce connect timeout on cleartext HTTP/2 handshake
        let h2_fut = connect_stream(stream, config);
        return if let Some(to) = timeout {
            tokio::time::timeout(to, h2_fut).await.map_err(|_| {
                Http2Error::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "HTTP/2 cleartext handshake timed out",
                ))
            })?
        } else {
            h2_fut.await
        };
    };

    // Enforce connect timeout on upstream TLS handshake to eliminate indefinite task hangs
    let tls_fut = engine.connect(sni, stream);
    let tls_stream = if let Some(to) = timeout {
        tokio::time::timeout(to, tls_fut)
            .await
            .map_err(|_| {
                Http2Error::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Upstream TLS handshake timed out",
                ))
            })?
            .map_err(|e| Http2Error::Tls(e.to_string()))?
    } else {
        tls_fut.await.map_err(|e| Http2Error::Tls(e.to_string()))?
    };

    // RFC 9113 Section 3.3 mandate: HTTP/2 over TLS MUST negotiate ALPN identification "h2".
    // Missing (None) or non-h2 ALPN is strictly rejected to prevent protocol desynchronization.
    let alpn = tls_stream.get_ref().1.alpn_protocol();
    if alpn != Some(b"h2") {
        return Err(Http2Error::Tls(format!(
            "upstream negotiated ALPN {:?}, strictly expected \"h2\" (RFC 9113 Section 3.3)",
            alpn.map(String::from_utf8_lossy)
        )));
    }

    // Enforce connect timeout on initial HTTP/2 SETTINGS frame exchange over TLS
    let h2_fut = connect_stream(tls_stream, config);
    if let Some(to) = timeout {
        tokio::time::timeout(to, h2_fut).await.map_err(|_| {
            Http2Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Upstream HTTP/2 TLS handshake timed out",
            ))
        })?
    } else {
        h2_fut.await
    }
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
