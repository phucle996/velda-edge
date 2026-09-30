//! Upstream gRPC client connector for `velda-upstream` and connection pool.
//!
//! Owns connection establishment, HTTP/2 client framing, and both Unary (1 chiều)
//! and Streaming request dispatch to physical backend endpoints.

use bytes::{Bytes, BytesMut};
use h2::SendStream;
use h2::client::{Connection, ResponseFuture, SendRequest, handshake};
use http::Version;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use velda_core::{Body, IngressLimits, L7Request, L7Response};

use crate::error::GrpcError;

/// Active gRPC client connector to an upstream backend endpoint.
pub struct GrpcUpstreamConnector {
    send_request: SendRequest<Bytes>,
}

impl GrpcUpstreamConnector {
    /// Establishes a new HTTP/2 connection to the upstream target backend endpoint.
    pub async fn connect(target: SocketAddr) -> Result<Self, GrpcError> {
        let stream = TcpStream::connect(target).await.map_err(GrpcError::Io)?;
        let (send_request, connection): (SendRequest<Bytes>, Connection<TcpStream, Bytes>) =
            handshake(stream).await.map_err(GrpcError::H2)?;

        // Drive background H2 connection management
        tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(Self { send_request })
    }

    /// Submits a raw streaming gRPC request to the upstream backend.
    ///
    /// Returns the upstream `ResponseFuture` and an active `SendStream` for streaming data chunks.
    pub fn open_stream(
        &mut self,
        request: http::Request<()>,
        end_of_stream: bool,
    ) -> Result<(ResponseFuture, SendStream<Bytes>), GrpcError> {
        self.send_request
            .send_request(request, end_of_stream)
            .map_err(GrpcError::H2)
    }

    /// Invokes a 1 chiều (Unary) gRPC request and returns the upstream `L7Response`.
    ///
    /// Seamlessly forwards request payload, waits for response headers, reads response LPM frame,
    /// and collects trailers (`grpc-status`). Enforces `max_body_size` from [`IngressLimits`]
    /// on the upstream response body.
    pub async fn invoke_unary(
        &mut self,
        req: &L7Request,
        limits: &IngressLimits,
    ) -> Result<L7Response, GrpcError> {
        let mut request_builder = http::Request::builder()
            .method(req.method.clone())
            .uri(req.uri.clone())
            .version(Version::HTTP_2);

        for (name, val) in &req.headers {
            request_builder = request_builder.header(name, val);
        }

        let end_of_stream = !req.has_body();
        let http_req = request_builder.body(()).map_err(GrpcError::Http)?;

        let (response_future, mut send_stream) = self.open_stream(http_req, end_of_stream)?;

        if let Body::Bytes(ref data) = req.body {
            send_stream
                .send_data(data.clone(), true)
                .map_err(GrpcError::H2)?;
        }

        let response = response_future.await.map_err(GrpcError::H2)?;
        let (parts, mut body_stream) = response.into_parts();
        let max_body = limits.max_body_size;
        let mut resp_body = BytesMut::new();

        while let Some(chunk_res) = body_stream.data().await {
            let chunk = chunk_res.map_err(GrpcError::H2)?;
            resp_body.extend_from_slice(&chunk);
            let _ = body_stream.flow_control().release_capacity(chunk.len());
            if resp_body.len() > max_body {
                return Err(GrpcError::PayloadTooLarge(resp_body.len()));
            }
        }

        let mut headers = parts.headers;
        if let Some(trailers) = body_stream.trailers().await.map_err(GrpcError::H2)? {
            for (name, val) in trailers {
                if let Some(name) = name {
                    headers.append(name, val);
                }
            }
        }

        let body = if resp_body.is_empty() {
            Body::Empty
        } else {
            Body::Bytes(resp_body.freeze())
        };

        Ok(L7Response::new(
            parts.status,
            Version::HTTP_2,
            headers,
            body,
        ))
    }

    /// Helper that connects to `target` and executes a 1 chiều (Unary) gRPC request in one call.
    pub async fn forward_unary(
        req: &L7Request,
        target: SocketAddr,
        limits: &IngressLimits,
    ) -> Result<L7Response, GrpcError> {
        let mut connector = Self::connect(target).await?;
        connector.invoke_unary(req, limits).await
    }
}
