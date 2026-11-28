//! Upstream HTTP/1.1 Client Connection Establishment and Socket Acceleration (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles outbound HTTP/1.1 client connection establishment, Linux TCP acceleration paths,
//! and TLS ALPN negotiation directly to backend microservices.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use velda_tls::{TlsClientEngine, TlsStream};

use crate::outcome::Http1Outcome;

/// Upstream stream connection either over cleartext TCP or encrypted TLS.
pub enum UpstreamHttp1Stream {
    Plain(tokio::net::TcpStream),
    PlainCold(std::net::TcpStream),
    Tls(Box<TlsStream<tokio::net::TcpStream>>),
    /// Placeholder state during reactor detach/re-attach transitions.
    Detached,
}

impl UpstreamHttp1Stream {
    /// Validates whether the underlying TCP stream is still open and healthy.
    pub fn is_healthy(&self) -> bool {
        let mut buf = [0u8; 1];
        let res = match self {
            Self::Plain(s) => s.try_read(&mut buf),
            Self::PlainCold(s) => s.peek(&mut buf),
            Self::Tls(s) => s.get_ref().0.try_read(&mut buf),
            Self::Detached => return false,
        };
        matches!(res, Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock)
    }

    /// Detaches a plaintext TCP stream from the current core's Tokio `epoll` reactor
    /// before moving into the shared cross-core `ColdPool`.
    pub fn detach_for_cold(&mut self) {
        if matches!(self, Self::Plain(_)) {
            let prev = std::mem::replace(self, Self::Detached);
            if let Self::Plain(tokio_stream) = prev {
                match tokio_stream.into_std() {
                    Ok(std_stream) => *self = Self::PlainCold(std_stream),
                    Err(_) => *self = Self::Detached,
                }
            }
        }
    }

    /// Re-attaches an unregistered `std::net::TcpStream` pulled from the shared `ColdPool`
    /// onto the acquiring core's Tokio `epoll` reactor.
    pub fn attach_for_hot(&mut self) {
        if matches!(self, Self::PlainCold(_)) {
            let prev = std::mem::replace(self, Self::Detached);
            if let Self::PlainCold(std_stream) = prev {
                let _ = std_stream.set_nonblocking(true);
                match tokio::net::TcpStream::from_std(std_stream) {
                    Ok(tokio_stream) => *self = Self::Plain(tokio_stream),
                    Err(_) => *self = Self::Detached,
                }
            }
        }
    }
}

impl std::fmt::Debug for UpstreamHttp1Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(s) => f.debug_tuple("Plain").field(s).finish(),
            Self::PlainCold(s) => f.debug_tuple("PlainCold").field(s).finish(),
            Self::Tls(_) => f.debug_tuple("Tls").finish(),
            Self::Detached => f.write_str("Detached"),
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
            Self::PlainCold(_) | Self::Detached => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "stream not attached to tokio reactor",
            ))),
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
            Self::PlainCold(_) | Self::Detached => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "stream not attached to tokio reactor",
            ))),
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
            Self::PlainCold(_) | Self::Detached => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "stream not attached to tokio reactor",
            ))),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(s) => s.is_write_vectored(),
            Self::Tls(s) => s.is_write_vectored(),
            Self::PlainCold(_) | Self::Detached => false,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            Self::PlainCold(_) | Self::Detached => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "stream not attached to tokio reactor",
            ))),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            Self::PlainCold(_) | Self::Detached => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "stream not attached to tokio reactor",
            ))),
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
) -> Result<UpstreamHttp1Stream, Http1Outcome> {
    let connect_timeout = timeout.unwrap_or(std::time::Duration::from_secs(10));
    let tcp_stream =
        velda_sock::connect::connect_tcp_u128(target_ip, target_port_be, connect_timeout)
            .await
            .map_err(Http1Outcome::Io)?;

    if let Some((engine, sni)) = tls {
        // Enforce connect timeout on upstream TLS handshake to eliminate indefinite task hangs (tarpit / slowloris defense)
        let tls_connect_fut = engine.connect(sni, tcp_stream);
        let tls_stream = if let Some(to) = timeout {
            tokio::time::timeout(to, tls_connect_fut)
                .await
                .map_err(|_| {
                    Http1Outcome::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "TLS handshake timed out",
                    ))
                })?
                .map_err(|e| Http1Outcome::Io(std::io::Error::other(e.to_string())))?
        } else {
            tls_connect_fut
                .await
                .map_err(|e| Http1Outcome::Io(std::io::Error::other(e.to_string())))?
        };

        // RFC 9112 Section 2: HTTP/1.1 over TLS strictly requires ALPN "http/1.1"
        let alpn = tls_stream.get_ref().1.alpn_protocol();
        if alpn != Some(b"http/1.1") {
            return Err(Http1Outcome::Io(std::io::Error::other(format!(
                "upstream negotiated ALPN {:?}, strictly expected \"http/1.1\"",
                alpn.map(String::from_utf8_lossy)
            ))));
        }
        Ok(UpstreamHttp1Stream::Tls(Box::new(tls_stream)))
    } else {
        Ok(UpstreamHttp1Stream::Plain(tcp_stream))
    }
}
