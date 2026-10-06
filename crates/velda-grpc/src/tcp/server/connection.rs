//! Downstream gRPC server connection lifecycle and stream accept loop.

use bytes::Bytes;
use h2::server::Connection;
use tokio::io::{AsyncRead, AsyncWrite};

use super::responder::GrpcResponder;
use super::stream::GrpcServerStream;
use crate::config::GrpcConfig;
use crate::error::GrpcError;

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
    /// Performs HTTP/2 server handshake over the transport stream configuring
    /// limits and stream concurrency from [`GrpcConfig`].
    pub async fn handshake(stream: IO, config: &GrpcConfig) -> Result<Self, GrpcError> {
        let mut builder = h2::server::Builder::default();
        builder
            .max_concurrent_streams(config.max_concurrent_streams)
            .max_header_list_size(config.max_header_size as u32);

        let connection = builder.handshake(stream).await.map_err(GrpcError::H2)?;
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
