//! Downstream Ingress parsing and stream extraction for `velda-composer`.
//!
//! Provides the primary boundary where raw/TLS client connections from Composer are
//! upgraded to HTTP/2, yielding parsed gRPC requests and full-duplex streams.

use bytes::{Bytes, BytesMut};
use h2::server::{Connection, handshake};
use http::{HeaderMap, Method};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::edge_response::GrpcResponder;
use crate::error::GrpcError;
use crate::frame::decode_grpc_frame;
use crate::status::GrpcStatus;

/// An active gRPC downstream server connection over an asynchronous I/O stream.
pub struct GrpcServerConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    connection: Connection<IO, Bytes>,
}

impl<IO> GrpcServerConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Performs HTTP/2 server handshake over the transport stream.
    pub async fn handshake(stream: IO) -> Result<Self, GrpcError> {
        let connection = handshake(stream).await.map_err(GrpcError::H2)?;
        Ok(Self { connection })
    }

    /// Accepts the next incoming gRPC stream from the client.
    ///
    /// Returns `Ok(Some(GrpcServerStream))` for new incoming streams without buffering the body,
    /// or `Ok(None)` when the connection is closed.
    pub async fn accept(&mut self) -> Result<Option<GrpcServerStream>, GrpcError> {
        let Some(res) = self.connection.accept().await else {
            return Ok(None);
        };

        let (request, respond) = res.map_err(GrpcError::H2)?;
        let (parts, recv_stream) = request.into_parts();

        Ok(Some(GrpcServerStream {
            parts,
            recv_stream,
            respond: GrpcResponder::new(respond),
        }))
    }
}

/// An accepted, active downstream gRPC stream.
pub struct GrpcServerStream {
    /// Initial HTTP/2 headers / parts (:path, :authority, metadata).
    pub parts: http::request::Parts,
    /// Streaming body reader receiving raw gRPC message frames.
    pub recv_stream: h2::RecvStream,
    /// Streaming response sender responding to client.
    pub respond: GrpcResponder,
}

impl GrpcServerStream {
    /// Request URI path (e.g. `/package.Service/Method`).
    #[inline]
    pub fn path(&self) -> &str {
        self.parts.uri.path()
    }

    /// Request HTTP method (always POST for gRPC).
    #[inline]
    pub fn method(&self) -> &Method {
        &self.parts.method
    }

    /// Request authority (:authority header or URI authority).
    #[inline]
    pub fn authority(&self) -> Option<&str> {
        self.parts
            .headers
            .get(":authority")
            .and_then(|v| v.to_str().ok())
            .or_else(|| self.parts.uri.authority().map(|a| a.as_str()))
    }

    /// Request metadata headers.
    #[inline]
    pub fn headers(&self) -> &HeaderMap {
        &self.parts.headers
    }

    /// Reads exactly one Length-Prefixed Message from the downstream stream (Unary / 1 chiều).
    ///
    /// Buffers incoming data chunks until the full 5-byte header and payload are received.
    /// Returns `Ok(Some(payload))` or `Ok(None)` if stream was empty.
    pub async fn read_unary_message(&mut self) -> Result<Option<Bytes>, GrpcError> {
        let mut buf = BytesMut::new();

        while let Some(chunk_res) = self.recv_stream.data().await {
            let chunk = chunk_res.map_err(GrpcError::H2)?;
            let len = chunk.len();
            buf.extend_from_slice(&chunk);
            let _ = self.recv_stream.flow_control().release_capacity(len);

            if let Some((_compressed, payload)) = decode_grpc_frame(&mut buf)? {
                return Ok(Some(payload));
            }
        }

        // Check if there was remaining data in buffer
        if let Some((_compressed, payload)) = decode_grpc_frame(&mut buf)? {
            Ok(Some(payload))
        } else if buf.is_empty() {
            Ok(None)
        } else {
            Err(GrpcError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Incomplete gRPC message frame received",
            )))
        }
    }

    /// Sends a standard 1 chiều (Unary) response: HTTP 200 + 1 LPM frame + trailers with grpc-status.
    ///
    /// Delegates to [`crate::edge_response::GrpcResponder::send_unary_response`].
    pub fn send_unary_response(
        &mut self,
        status: GrpcStatus,
        payload: Option<&[u8]>,
        extra_headers: Option<HeaderMap>,
    ) -> Result<(), GrpcError> {
        self.respond
            .send_unary_response(status, payload, extra_headers)
    }

    /// Responds immediately with a Trailers-Only gRPC response (e.g. for routing errors).
    ///
    /// Delegates to [`crate::edge_response::GrpcResponder::send_trailers_only`].
    pub fn send_trailers_only(
        self,
        status: GrpcStatus,
        message: Option<&str>,
    ) -> Result<(), GrpcError> {
        self.respond.send_trailers_only(status, message)
    }
}
