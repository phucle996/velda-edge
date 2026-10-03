//! Upstream gRPC client connector for `velda-upstream` and connection pool.
//!
//! Owns connection establishment, HTTP/2 client framing, and both Unary
//! and Streaming request dispatch to physical backend endpoints.

use bytes::{Bytes, BytesMut};
use h2::SendStream;
use h2::client::{Connection, ResponseFuture, SendRequest};
use http::Version;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use velda_core::{Body, L7Request, L7Response};

use crate::config::GrpcConfig;
use crate::error::GrpcError;

/// Active gRPC client connector to an upstream backend endpoint.
#[derive(Clone)]
pub struct GrpcUpstreamConnector {
    send_request: SendRequest<Bytes>,
}

impl GrpcUpstreamConnector {
    /// Polls or awaits readiness of the underlying HTTP/2 connection to accept a new request stream.
    pub async fn ready(&mut self) -> Result<(), GrpcError> {
        let ready_send = self
            .send_request
            .clone()
            .ready()
            .await
            .map_err(GrpcError::H2)?;
        self.send_request = ready_send;
        Ok(())
    }
    /// Establishes a new HTTP/2 connection to the upstream target backend endpoint
    /// applying limits from [`GrpcConfig`].
    pub async fn connect(target: SocketAddr, config: &GrpcConfig) -> Result<Self, GrpcError> {
        let stream = TcpStream::connect(target).await.map_err(GrpcError::Io)?;
        let mut builder = h2::client::Builder::default();
        builder.max_header_list_size(config.max_header_size as u32);

        let (send_request, connection): (SendRequest<Bytes>, Connection<TcpStream, Bytes>) =
            builder.handshake(stream).await.map_err(GrpcError::H2)?;

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

    /// Invokes a Unary gRPC request and returns the upstream `L7Response`.
    ///
    /// Seamlessly forwards request payload, waits for response headers, reads response LPM frame,
    /// and collects trailers (`grpc-status`). Enforces `max_message_size` from [`GrpcConfig`]
    /// on the upstream response body.
    pub async fn invoke_unary(
        &mut self,
        req: &L7Request,
        config: &GrpcConfig,
    ) -> Result<L7Response, GrpcError> {
        let mut request_builder = http::Request::builder()
            .method(&req.method)
            .uri(&req.uri)
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
        let max_body = config.max_message_size;

        // Zero-allocation fast path for single-chunk Unary responses
        let body = if body_stream.is_end_stream() {
            Body::Empty
        } else if let Some(first_chunk) = body_stream.data().await {
            let chunk = first_chunk.map_err(GrpcError::H2)?;
            let len = chunk.len();
            if len > max_body {
                let _ = body_stream.flow_control().release_capacity(len);
                return Err(GrpcError::PayloadTooLarge(len));
            }
            let _ = body_stream.flow_control().release_capacity(len);

            if body_stream.is_end_stream() {
                Body::Bytes(chunk)
            } else {
                let mut resp_body = BytesMut::with_capacity(len * 2);
                resp_body.extend_from_slice(&chunk);

                while let Some(chunk_res) = body_stream.data().await {
                    let chunk = chunk_res.map_err(GrpcError::H2)?;
                    let len = chunk.len();
                    if resp_body.len() + len > max_body {
                        let _ = body_stream.flow_control().release_capacity(len);
                        return Err(GrpcError::PayloadTooLarge(resp_body.len() + len));
                    }
                    resp_body.extend_from_slice(&chunk);
                    let _ = body_stream.flow_control().release_capacity(len);
                }

                Body::Bytes(resp_body.freeze())
            }
        } else {
            Body::Empty
        };

        let mut headers = parts.headers;
        if let Some(trailers) = body_stream.trailers().await.map_err(GrpcError::H2)? {
            for (name, val) in trailers {
                if let Some(name) = name {
                    headers.append(name, val);
                }
            }
        }

        Ok(L7Response::new(
            parts.status,
            Version::HTTP_2,
            headers,
            body,
        ))
    }
}
