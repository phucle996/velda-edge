//! HTTP/1.1 server connection handler and request loop.

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use velda_core::{L7Request, L7Response};

use super::codec::{decode_request, encode_response};
use crate::error::HttpError;

/// Buffer capacity initialized for HTTP/1.1 reads.
const INITIAL_BUFFER_CAPACITY: usize = 4096;

/// An active HTTP/1.1 connection over an asynchronous stream.
pub struct Http1Connection<IO> {
    stream: IO,
    read_buf: BytesMut,
    write_buf: BytesMut,
    close_requested: bool,
}

impl<IO> Http1Connection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Creates a new HTTP/1.1 connection wrapping the provided I/O stream.
    pub fn new(stream: IO) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(INITIAL_BUFFER_CAPACITY),
            write_buf: BytesMut::with_capacity(INITIAL_BUFFER_CAPACITY),
            close_requested: false,
        }
    }

    /// Reads and decodes the next HTTP/1.1 request from the stream.
    ///
    /// Returns `Ok(None)` when the peer has cleanly closed the connection.
    pub async fn next_request(&mut self) -> Result<Option<L7Request>, HttpError> {
        if self.close_requested {
            return Ok(None);
        }

        loop {
            // 1. Try decoding existing bytes in buffer
            if let Some(req) = decode_request(&mut self.read_buf)? {
                if req
                    .headers
                    .get(http::header::CONNECTION)
                    .and_then(|h| h.to_str().ok())
                    .is_some_and(|s| s.eq_ignore_ascii_case("close"))
                {
                    self.close_requested = true;
                }
                return Ok(Some(req));
            }

            // 2. Read more bytes from the underlying stream
            let bytes_read = self.stream.read_buf(&mut self.read_buf).await?;
            if bytes_read == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None); // Clean EOF
                } else {
                    return Err(HttpError::Parse(
                        "Unexpected EOF while reading HTTP request".into(),
                    ));
                }
            }
        }
    }

    /// Serializes and flushes an HTTP/1.1 response back to the stream.
    pub async fn send_response(&mut self, response: &L7Response) -> Result<(), HttpError> {
        self.write_buf.clear();
        encode_response(response, &mut self.write_buf);

        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;

        if response
            .headers
            .get(http::header::CONNECTION)
            .and_then(|h| h.to_str().ok())
            .is_some_and(|s| s.eq_ignore_ascii_case("close"))
        {
            self.close_requested = true;
        }

        Ok(())
    }

    /// Returns whether this connection was flagged to close.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.close_requested
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_http1_connection_ping_pong() {
        let (client_io, server_io) = duplex(1024);
        let mut server = Http1Connection::new(server_io);

        // Client writes a request
        let client_task = tokio::spawn(async move {
            let mut io = client_io;
            io.write_all(b"GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();

            let mut resp_buf = vec![0u8; 1024];
            let n = io.read(&mut resp_buf).await.unwrap();
            String::from_utf8(resp_buf[..n].to_vec()).unwrap()
        });

        // Server receives request and sends response
        let req = server.next_request().await.unwrap().unwrap();
        assert_eq!(req.path(), "/ping");

        let res = L7Response::from_bytes(StatusCode::OK, b"pong".to_vec());
        server.send_response(&res).await.unwrap();

        let client_response = client_task.await.unwrap();
        assert!(client_response.contains("HTTP/1.1 200 OK\r\n"));
        assert!(client_response.contains("pong"));
    }
}
