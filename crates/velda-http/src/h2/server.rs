//! HTTP/2 server connection handler and multiplexed stream dispatcher.

use bytes::{Bytes, BytesMut};
use h2::server::{Connection, SendResponse, handshake};
use http::Response;
use tokio::io::{AsyncRead, AsyncWrite};
use velda_core::{Body, L7Request, L7Response};

use crate::error::HttpError;

/// Responder for an active HTTP/2 stream.
pub struct Http2Responder {
    respond: SendResponse<Bytes>,
}

impl Http2Responder {
    /// Sends an HTTP/2 response on this stream.
    pub fn send_response(mut self, response: &L7Response) -> Result<(), HttpError> {
        let mut builder = Response::builder()
            .status(response.status)
            .version(http::Version::HTTP_2);

        for (name, val) in &response.headers {
            builder = builder.header(name, val);
        }

        let is_end_of_stream = !response.has_body();
        let http_response = builder
            .body(())
            .map_err(|e| HttpError::Parse(e.to_string()))?;

        let mut send_stream = self
            .respond
            .send_response(http_response, is_end_of_stream)?;

        if let Body::Bytes(ref b) = response.body {
            send_stream.send_data(b.clone(), true)?;
        }

        Ok(())
    }
}

/// An active HTTP/2 multiplexed connection over an asynchronous stream.
pub struct Http2Connection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    connection: Connection<IO, Bytes>,
}

impl<IO> Http2Connection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Performs the HTTP/2 server handshake over the underlying I/O stream.
    pub async fn handshake(stream: IO) -> Result<Self, HttpError> {
        let connection = handshake(stream).await?;
        Ok(Self { connection })
    }

    /// Accepts the next multiplexed stream from the client.
    ///
    /// Reads the complete request body and returns `(L7Request, Http2Responder)`.
    /// Returns `Ok(None)` when the connection is gracefully closed.
    pub async fn accept_request(
        &mut self,
    ) -> Result<Option<(L7Request, Http2Responder)>, HttpError> {
        let Some(res) = self.connection.accept().await else {
            return Ok(None);
        };

        let (request, respond) = res?;
        let (parts, mut body_stream) = request.into_parts();

        // Read incoming request body
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

        Ok(Some((l7_request, Http2Responder { respond })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, StatusCode};
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_http2_handshake_and_request_response() {
        let (client_io, server_io) = duplex(4096);

        // Server accepts HTTP/2 handshake
        let server_task = tokio::spawn(async move {
            let mut server = Http2Connection::handshake(server_io).await.unwrap();
            let (req, responder) = server.accept_request().await.unwrap().unwrap();

            assert_eq!(req.method, Method::GET);
            assert_eq!(req.path(), "/h2-test");

            let res = L7Response::from_bytes(StatusCode::OK, b"hello h2".to_vec());
            responder.send_response(&res).unwrap();

            // Continue driving connection until stream completes
            let _ = server.connection.accept().await;
        });

        // Client performs client-side HTTP/2 handshake
        let client_task = tokio::spawn(async move {
            let (mut client, h2_conn) = h2::client::handshake(client_io).await.unwrap();
            tokio::spawn(async move {
                let _ = h2_conn.await;
            });

            let request = http::Request::builder()
                .method("GET")
                .uri("https://localhost/h2-test")
                .body(())
                .unwrap();

            let (response, _) = client.send_request(request, true).unwrap();
            let (head, mut body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, StatusCode::OK);

            let chunk = body.data().await.unwrap().unwrap();
            assert_eq!(&chunk[..], b"hello h2");
        });

        let (s_res, c_res) = tokio::join!(server_task, client_task);
        s_res.unwrap();
        c_res.unwrap();
    }
}
