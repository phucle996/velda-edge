//! Upstream gRPC Client Response Entity and Ingestion (RFC 9113 / gRPC specification).

use bytes::BytesMut;
use h2::client::ResponseFuture;
use http::header::HeaderMap;
use http::{StatusCode, Version};
use velda_core::{Body, L7Response};

use crate::error::GrpcError;
use crate::status::GrpcStatus;

/// Upstream gRPC client response head metadata.
#[derive(Debug, Clone)]
pub struct GrpcClientResponseHead {
    /// HTTP status code.
    pub status: StatusCode,
    /// Canonical gRPC status parsed from trailers or response headers.
    pub grpc_status: GrpcStatus,
    /// Response headers.
    pub headers: HeaderMap,
    /// Response trailers.
    pub trailers: HeaderMap,
}

impl GrpcClientResponseHead {
    /// Creates a new response head.
    #[inline]
    pub fn new(
        status: StatusCode,
        grpc_status: GrpcStatus,
        headers: HeaderMap,
        trailers: HeaderMap,
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
            HeaderMap::new(),
        );

        Self {
            head,
            body: resp.body.clone(),
        }
    }

    /// Converts into canonical [`L7Response`].
    pub fn into_l7(self) -> L7Response {
        let mut headers = self.head.headers;
        for (name, val) in self.head.trailers {
            if let Some(name) = name {
                headers.append(name, val);
            }
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

    /// Decodes an upstream gRPC response from an active HTTP/2 `ResponseFuture`,
    /// enforcing flow control and `max_body` limits sequentially without arbitrary wrappers.
    pub async fn decode_from_future(
        response_future: ResponseFuture,
        max_body: usize,
    ) -> Result<Self, GrpcError> {
        let response = response_future.await.map_err(GrpcError::H2)?;
        let (parts, mut body_stream) = response.into_parts();

        // 1. Read data chunks
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

        // 2. Read trailers
        let trailers = body_stream
            .trailers()
            .await
            .map_err(GrpcError::H2)?
            .unwrap_or_default();

        let grpc_status = trailers
            .get("grpc-status")
            .or_else(|| parts.headers.get("grpc-status"))
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
            .map(GrpcStatus::from_code)
            .unwrap_or(GrpcStatus::Ok);

        let head = GrpcClientResponseHead::new(parts.status, grpc_status, parts.headers, trailers);

        Ok(Self::new(head, body))
    }
}
