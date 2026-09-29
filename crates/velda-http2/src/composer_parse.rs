//! HTTP/2 downstream connection acceptor and stream multiplexer for `velda-composer`.

use bytes::{Bytes, BytesMut};
use h2::server::{Connection, handshake};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::{Body, L7Request};

use crate::edge_response::Http2Responder;
use crate::error::Http2Error;

/// An active HTTP/2 downstream connection over an asynchronous stream.
pub struct Http2ServerConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    connection: Connection<IO, Bytes>,
}

impl<IO> Http2ServerConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Performs the HTTP/2 server handshake over the underlying I/O stream.
    pub async fn handshake(stream: IO) -> Result<Self, Http2Error> {
        let connection = handshake(stream).await?;
        Ok(Self { connection })
    }

    /// Accepts the next multiplexed request stream from the client.
    pub async fn accept_request(
        &mut self,
    ) -> Result<Option<(L7Request, Http2Responder)>, Http2Error> {
        let Some(res) = self.connection.accept().await else {
            return Ok(None);
        };

        let (request, respond) = res?;
        let (parts, mut body_stream) = request.into_parts();

        let mut body_buf = BytesMut::new();
        while let Some(chunk) = body_stream.data().await {
            let data = chunk?;
            body_buf.extend_from_slice(&data);
            let _ = body_stream.flow_control().release_capacity(data.len());
        }

        let body = if body_buf.is_empty() {
            Body::Empty
        } else {
            Body::Bytes(body_buf.freeze())
        };

        let l7_request = L7Request::new(
            parts.method,
            parts.uri,
            http::Version::HTTP_2,
            parts.headers,
            body,
        );

        Ok(Some((l7_request, Http2Responder::new(respond))))
    }
}
