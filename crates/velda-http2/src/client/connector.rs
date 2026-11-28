//! Upstream HTTP/2 Client Connection Establishment (RFC 9113).
//!
//! Handles TCP connection establishment, TLS/generic stream handshakes, and background H2
//! connection driver execution.

use velda_tls::TlsClientEngine;

use crate::client::connection::Http2Client;
use crate::config::Http2Config;
use crate::outcome::Http2Outcome;

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
) -> Result<Http2Client, Http2Outcome> {
    let connect_timeout = timeout.unwrap_or(std::time::Duration::from_secs(10));
    let stream = velda_sock::connect::connect_tcp_u128(target_ip, target_port_be, connect_timeout)
        .await
        .map_err(Http2Outcome::Io)?;

    let Some((engine, sni)) = tls else {
        let h2_fut = connect_stream(stream, config);
        return if let Some(to) = timeout {
            tokio::time::timeout(to, h2_fut).await.map_err(|_| {
                Http2Outcome::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "HTTP/2 cleartext handshake timed out",
                ))
            })?
        } else {
            h2_fut.await
        };
    };

    let tls_fut = engine.connect(sni, stream);
    let tls_stream = if let Some(to) = timeout {
        tokio::time::timeout(to, tls_fut)
            .await
            .map_err(|_| {
                Http2Outcome::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Upstream TLS handshake timed out",
                ))
            })?
            .map_err(|e| Http2Outcome::Tls(e.to_string()))?
    } else {
        tls_fut
            .await
            .map_err(|e| Http2Outcome::Tls(e.to_string()))?
    };

    let alpn = tls_stream.get_ref().1.alpn_protocol();
    if alpn != Some(b"h2") {
        return Err(Http2Outcome::Tls(format!(
            "upstream negotiated ALPN {:?}, strictly expected \"h2\" (RFC 9113 Section 3.3)",
            alpn.map(String::from_utf8_lossy)
        )));
    }

    let h2_fut = connect_stream(tls_stream, config);
    if let Some(to) = timeout {
        tokio::time::timeout(to, h2_fut).await.map_err(|_| {
            Http2Outcome::Io(std::io::Error::new(
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
) -> Result<Http2Client, Http2Outcome>
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    Http2Client::handshake(stream, config).await
}
