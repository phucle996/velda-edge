//! Unified HTTP connection dispatcher and protocol binder.

use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::HttpError;
use crate::h1::Http1Connection;
use crate::h2::Http2Connection;
use crate::version::HttpVersion;

/// An active HTTP connection bound to a specific protocol version.
pub enum HttpConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// HTTP/1.0 or HTTP/1.1 connection.
    Http1(Http1Connection<IO>),
    /// HTTP/2 connection.
    Http2(Box<Http2Connection<IO>>),
}

/// Binds an asynchronous I/O stream to the resolved concrete HTTP version.
pub async fn bind_connection<IO>(
    stream: IO,
    version: HttpVersion,
) -> Result<HttpConnection<IO>, HttpError>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    match version {
        HttpVersion::Http1 => Ok(HttpConnection::Http1(Http1Connection::new(stream))),
        HttpVersion::Http2 => {
            let conn = Http2Connection::handshake(stream).await?;
            Ok(HttpConnection::Http2(Box::new(conn)))
        }
        HttpVersion::Http3 => Err(HttpError::UnsupportedVersion(
            "HTTP/3 runs over QUIC datagrams, not streaming TCP/TLS sockets".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_bind_http1_connection() {
        let (_client, server) = duplex(1024);
        let conn = bind_connection(server, HttpVersion::Http1).await.unwrap();
        assert!(matches!(conn, HttpConnection::Http1(_)));
    }
}
