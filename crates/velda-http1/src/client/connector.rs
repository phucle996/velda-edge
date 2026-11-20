//! Upstream HTTP/1.1 Client Connection Establishment and Socket Acceleration (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles outbound HTTP/1.1 client connection establishment, Linux TCP acceleration paths,
//! and TLS ALPN negotiation directly to backend microservices.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use velda_tls::{TlsClientEngine, TlsStream};

use crate::error::Http1Error;

/// Upstream stream connection either over cleartext TCP or encrypted TLS.
pub enum UpstreamHttp1Stream {
    Plain(tokio::net::TcpStream),
    Tls(Box<TlsStream<tokio::net::TcpStream>>),
}

impl UpstreamHttp1Stream {
    /// Validates whether the underlying TCP stream is still open and healthy.
    ///
    /// Performs a non-blocking 1-byte read without blocking:
    /// - `Err(WouldBlock)`: Connection is open and idle (no pending data) -> healthy.
    /// - `Ok(0)`: Remote peer closed the connection (FIN/EOF) -> unhealthy.
    /// - `Ok(n)`: Unexpected unread bytes on idle keep-alive connection -> unhealthy.
    /// - `Err(_)`: Socket reset or I/O error -> unhealthy.
    pub fn is_healthy(&self) -> bool {
        let mut buf = [0u8; 1];
        let res = match self {
            Self::Plain(s) => s.try_read(&mut buf),
            Self::Tls(s) => s.get_ref().0.try_read(&mut buf),
        };
        matches!(res, Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock)
    }
}

impl std::fmt::Debug for UpstreamHttp1Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(s) => f.debug_tuple("Plain").field(s).finish(),
            Self::Tls(_) => f.debug_tuple("Tls").finish(),
        }
    }
}

impl AsyncRead for UpstreamHttp1Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for UpstreamHttp1Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(s) => s.is_write_vectored(),
            Self::Tls(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Connects directly to an HTTP/1.1 backend target `(target_ip, target_port_be)` via `velda-sock`.
///
/// `tls` is `Some((engine, sni))` for TLS upstreams. The SNI is the upstream's pre-compiled
/// static property; it is never derived from the request Host or the target IP.
pub async fn connect(
    target_ip: u128,
    target_port_be: u16,
    tls: Option<(&TlsClientEngine, &str)>,
    timeout: Option<std::time::Duration>,
) -> Result<UpstreamHttp1Stream, Http1Error> {
    let connect_timeout = timeout.unwrap_or(std::time::Duration::from_secs(10));
    let tcp_stream =
        velda_sock::connect::connect_tcp_u128(target_ip, target_port_be, connect_timeout)
            .await
            .map_err(Http1Error::Io)?;

    if let Some((engine, sni)) = tls {
        // Enforce connect timeout on upstream TLS handshake to eliminate indefinite task hangs (tarpit / slowloris defense)
        let tls_connect_fut = engine.connect(sni, tcp_stream);
        let tls_stream = if let Some(to) = timeout {
            tokio::time::timeout(to, tls_connect_fut)
                .await
                .map_err(|_| {
                    Http1Error::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "TLS handshake timed out",
                    ))
                })?
                .map_err(|e| Http1Error::Io(std::io::Error::other(e.to_string())))?
        } else {
            tls_connect_fut
                .await
                .map_err(|e| Http1Error::Io(std::io::Error::other(e.to_string())))?
        };

        // RFC 9112 Section 2: HTTP/1.1 over TLS strictly requires ALPN "http/1.1"
        let alpn = tls_stream.get_ref().1.alpn_protocol();
        if alpn != Some(b"http/1.1") {
            return Err(Http1Error::Io(std::io::Error::other(format!(
                "upstream negotiated ALPN {:?}, strictly expected \"http/1.1\"",
                alpn.map(String::from_utf8_lossy)
            ))));
        }
        Ok(UpstreamHttp1Stream::Tls(Box::new(tls_stream)))
    } else {
        Ok(UpstreamHttp1Stream::Plain(tcp_stream))
    }
}
