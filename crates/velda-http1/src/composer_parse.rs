//! HTTP/1.1 downstream connection acceptor and request loop for `velda-composer`.

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use velda_core::{L7Request, L7Response};

use crate::codec::decode_request;
use crate::error::Http1Error;

const INITIAL_BUFFER_CAPACITY: usize = 4096;

/// An active HTTP/1.1 connection over an asynchronous downstream stream.
pub struct Http1ServerConnection<IO> {
    stream: IO,
    read_buf: BytesMut,
    write_buf: BytesMut,
    close_requested: bool,
}

impl<IO> Http1ServerConnection<IO>
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
    pub async fn next_request(&mut self) -> Result<Option<L7Request>, Http1Error> {
        if self.close_requested {
            return Ok(None);
        }

        loop {
            if let Some(req) = decode_request(&mut self.read_buf)? {
                let is_http10 = req.version == http::Version::HTTP_10;
                let conn_header = req
                    .headers
                    .get(http::header::CONNECTION)
                    .and_then(|h| h.to_str().ok());

                if is_http10 {
                    // RFC 9112 Section 9.3: HTTP/1.0 defaults to close unless keep-alive is negotiated
                    if !conn_header.is_some_and(|s| s.eq_ignore_ascii_case("keep-alive")) {
                        self.close_requested = true;
                    }
                } else if conn_header.is_some_and(|s| s.eq_ignore_ascii_case("close")) {
                    self.close_requested = true;
                }

                return Ok(Some(req));
            }

            let bytes_read = self.stream.read_buf(&mut self.read_buf).await?;
            if bytes_read == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Http1Error::Parse(
                        "Unexpected EOF while reading HTTP request".into(),
                    ));
                }
            }
        }
    }

    /// Serializes and flushes an HTTP/1.1 response back to the downstream stream.
    ///
    /// Delegates encoding and stream writing to [`crate::edge_response::send_response`].
    pub async fn send_response(&mut self, response: &L7Response) -> Result<(), Http1Error> {
        let close =
            crate::edge_response::send_response(&mut self.stream, &mut self.write_buf, response)
                .await?;

        if close {
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
