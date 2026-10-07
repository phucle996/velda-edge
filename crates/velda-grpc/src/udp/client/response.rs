//! Upstream gRPC Client Response Entity over UDP / QUIC.

use http::header::HeaderMap;
use http::{StatusCode, Version};
use velda_core::{Body, L7Response};

use crate::status::GrpcStatus;

/// Upstream gRPC client response head metadata over UDP.
#[derive(Debug, Clone)]
pub struct GrpcUdpClientResponseHead {
    /// HTTP status code.
    pub status: StatusCode,
    /// Canonical gRPC status parsed from response headers/trailers.
    pub grpc_status: GrpcStatus,
    /// Response headers.
    pub headers: HeaderMap,
    /// Response trailers.
    pub trailers: HeaderMap,
}

impl GrpcUdpClientResponseHead {
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

/// Upstream protocol-owned gRPC client response over UDP.
#[derive(Debug, Clone)]
pub struct GrpcUdpClientResponse {
    /// Response head metadata.
    pub head: GrpcUdpClientResponseHead,
    /// Response payload (raw LPM frames or extracted payload).
    pub body: Body,
}

impl GrpcUdpClientResponse {
    /// Creates a new client response.
    #[inline]
    pub fn new(head: GrpcUdpClientResponseHead, body: Body) -> Self {
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

        let head = GrpcUdpClientResponseHead::new(
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
        L7Response::new(self.head.status, Version::HTTP_3, headers, self.body)
    }

    /// Converts into downstream server response.
    pub fn into_server_response(self) -> crate::udp::server::responder::GrpcUdpServerResponse {
        crate::udp::server::responder::GrpcUdpServerResponse::new(
            self.head.grpc_status,
            self.head.headers,
            self.head.trailers,
            self.body,
        )
    }
}
