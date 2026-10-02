//! Upstream HTTP/1.1 stream connection (Plain TCP or encrypted TLS).

use std::net::SocketAddr;
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

/// Connects directly to an HTTP/1.1 backend target with optional TLS encryption.
pub async fn connect_stream(
    target: SocketAddr,
    is_tls: bool,
    target_sni: Option<&str>,
    tls_client: Option<&TlsClientEngine>,
    host: Option<&str>,
) -> Result<UpstreamHttp1Stream, Http1Error> {
    let tcp_stream = tokio::net::TcpStream::connect(target)
        .await
        .map_err(Http1Error::Io)?;

    if is_tls {
        let target_ip_str = target.ip().to_string();
        let sni = target_sni.or(host).unwrap_or(&target_ip_str);
        let client_engine = tls_client.ok_or_else(|| {
            Http1Error::InvalidConfig(format!(
                "Upstream requires TLS for {sni}, but no tls_client engine configured"
            ))
        })?;
        let tls_stream = client_engine
            .connect(sni, tcp_stream)
            .await
            .map_err(|e| Http1Error::Io(std::io::Error::other(e.to_string())))?;
        Ok(UpstreamHttp1Stream::Tls(Box::new(tls_stream)))
    } else {
        Ok(UpstreamHttp1Stream::Plain(tcp_stream))
    }
}
