//! Upstream gRPC Client Response Entity and Ingestion over [`HeaderWorkspace`].

use bytes::BytesMut;
use http::{StatusCode, Version};
use velda_core::{Body, GrpcStatus, HeaderWorkspace, L7Response};
use velda_http2::client::connection::{Http2ClientResponse, Http2ResponseFuture};

use crate::error::GrpcError;

/// Upstream gRPC client response head metadata.
#[derive(Debug, Clone)]
pub struct GrpcClientResponseHead {
    /// HTTP status code.
    pub status: StatusCode,
    /// Canonical gRPC status parsed from trailers or response headers.
    pub grpc_status: GrpcStatus,
    /// Response headers.
    pub headers: HeaderWorkspace,
    /// Response trailers.
    pub trailers: HeaderWorkspace,
}

impl GrpcClientResponseHead {
    /// Creates a new response head.
    #[inline]
    pub fn new(
        status: StatusCode,
        grpc_status: GrpcStatus,
        headers: HeaderWorkspace,
        trailers: HeaderWorkspace,
    ) -> Self {
        Self {
            status,
            grpc_status,
            headers,
            trailers,
        }
    }
}

/// Upstream protocol-owned gRPC client response.
#[derive(Debug, Clone)]
pub struct GrpcClientResponse {
    /// Response head metadata.
    pub head: GrpcClientResponseHead,
    /// Response payload (raw LPM frames or extracted payload).
    pub body: Body,
}

impl GrpcClientResponse {
    /// Creates a new client response.
    #[inline]
    pub fn new(head: GrpcClientResponseHead, body: Body) -> Self {
        Self { head, body }
    }

    /// Converts from canonical [`L7Response`].
    pub fn from_l7(resp: &L7Response) -> Self {
        let grpc_status = resp
            .headers
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
            .map(GrpcStatus::from_code)
            .unwrap_or(GrpcStatus::Ok);

        let head = GrpcClientResponseHead::new(
            resp.status,
            grpc_status,
            resp.headers.clone(),
            HeaderWorkspace::empty(0),
        );

        Self {
            head,
            body: resp.body.clone(),
        }
    }

    /// Converts into canonical [`L7Response`].
    pub fn into_l7(self) -> L7Response {
        let mut headers = self.head.headers;
        for (name, val) in self.head.trailers.iter() {
            headers.append(name, val.clone());
        }
        L7Response::new(self.head.status, Version::HTTP_2, headers, self.body)
    }

    /// Converts into downstream server response.
    pub fn into_server_response(self) -> crate::tcp::server::responder::GrpcServerResponse {
        crate::tcp::server::responder::GrpcServerResponse::new(
            self.head.grpc_status,
            self.head.headers,
            self.head.trailers,
            self.body,
        )
    }

    /// Decodes an upstream gRPC response from an active [`Http2ResponseFuture`],
    /// enforcing flow control and `max_body` limits sequentially.
    pub async fn decode_from_future(
        response_future: Http2ResponseFuture,
        max_body: usize,
    ) -> Result<Self, GrpcError> {
        let Http2ClientResponse {
            status,
            headers,
            body: mut body_stream,
        } = response_future.await?;

        // 1. Read data chunks
        let body = if body_stream.is_end_stream() {
            Body::Empty
        } else if let Some(chunk) = body_stream.recv_chunk().await? {
            let len = chunk.len();
            if len > max_body {
                return Err(GrpcError::PayloadTooLarge(len));
            }

            if body_stream.is_end_stream() {
                Body::Bytes(chunk)
            } else {
                let mut resp_body = BytesMut::with_capacity(len * 2);
                resp_body.extend_from_slice(&chunk);

                while let Some(chunk) = body_stream.recv_chunk().await? {
                    let len = chunk.len();
                    if resp_body.len() + len > max_body {
                        return Err(GrpcError::PayloadTooLarge(resp_body.len() + len));
                    }
                    resp_body.extend_from_slice(&chunk);
                }

                Body::Bytes(resp_body.freeze())
            }
        } else {
            Body::Empty
        };

        // 2. Read trailers
        let trailers = body_stream
            .trailers()
            .await?
            .unwrap_or_else(|| HeaderWorkspace::empty(0));

        let grpc_status = trailers
            .get("grpc-status")
            .or_else(|| headers.get("grpc-status"))
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
            .map(GrpcStatus::from_code)
            .unwrap_or(GrpcStatus::Ok);

        let head = GrpcClientResponseHead::new(status, grpc_status, headers, trailers);

        Ok(Self::new(head, body))
    }
}
