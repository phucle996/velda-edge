//! Downstream gRPC response encoder and trailers serializer over UDP.

use bytes::{Bytes, BytesMut};
use http::StatusCode;
use http::header::HeaderMap;
use velda_core::{Body, L7Response};

use crate::status::GrpcStatus;
use crate::udp::wire::{UdpFrame, encode_qpack_response, encode_udp_frame};

/// Downstream gRPC server response head metadata over UDP.
#[derive(Debug, Clone)]
pub struct GrpcUdpServerResponseHead {
    /// HTTP status code.
    pub status: StatusCode,
    /// Canonical gRPC status.
    pub grpc_status: GrpcStatus,
    /// Response headers.
    pub headers: HeaderMap,
    /// Response trailers.
    pub trailers: HeaderMap,
}

impl GrpcUdpServerResponseHead {
    /// Creates a new response head.
    #[inline]
    pub fn new(grpc_status: GrpcStatus, headers: HeaderMap, trailers: HeaderMap) -> Self {
        Self {
            status: StatusCode::OK,
            grpc_status,
            headers,
            trailers,
        }
    }
}

/// Downstream protocol-owned gRPC server response over UDP.
#[derive(Debug, Clone)]
pub struct GrpcUdpServerResponse {
    /// Response head metadata.
    pub head: GrpcUdpServerResponseHead,
    /// Response payload.
    pub body: Body,
}

impl GrpcUdpServerResponse {
    /// Creates a new response.
    #[inline]
    pub fn new(
        grpc_status: GrpcStatus,
        headers: HeaderMap,
        trailers: HeaderMap,
        body: Body,
    ) -> Self {
        Self {
            head: GrpcUdpServerResponseHead::new(grpc_status, headers, trailers),
            body,
        }
    }

    /// Creates a unary response.
    pub fn unary(
        status: GrpcStatus,
        payload: Option<Bytes>,
        extra_headers: Option<HeaderMap>,
    ) -> Self {
        let headers = extra_headers.unwrap_or_default();
        let trailers = status.to_trailers(None);
        let body = match payload {
            Some(b) => Body::Bytes(b),
            None => Body::Empty,
        };
        Self::new(status, headers, trailers, body)
    }

    /// Creates a trailers-only gRPC response for errors or immediate completions.
    pub fn trailers_only(status: GrpcStatus, message: Option<&str>) -> Self {
        let trailers = status.to_trailers(message);
        Self::new(status, HeaderMap::new(), trailers, Body::Empty)
    }

    /// Converts into canonical [`L7Response`].
    pub fn into_l7_response(self) -> L7Response {
        let mut headers = self.head.headers;
        for (name, val) in self.head.trailers {
            if let Some(name) = name {
                headers.append(name, val);
            }
        }
        L7Response::new(self.head.status, http::Version::HTTP_3, headers, self.body)
    }

    /// Builds raw wire frame buffers for this gRPC response over UDP.
    pub fn encode_frames(&self) -> BytesMut {
        let mut header_payload = BytesMut::new();
        let mut headers = self.head.headers.clone();
        for (name, val) in &self.head.trailers {
            headers.append(name.clone(), val.clone());
        }
        encode_qpack_response(self.head.status, &headers, &mut header_payload);

        let mut write_buf = BytesMut::new();
        encode_udp_frame(&UdpFrame::Headers(header_payload.freeze()), &mut write_buf);

        if let Body::Bytes(ref b) = self.body
            && !b.is_empty()
        {
            encode_udp_frame(&UdpFrame::Data(b.clone()), &mut write_buf);
        }

        write_buf
    }
}

/// Responder for encoding gRPC response packets over UDP.
#[derive(Debug, Default)]
pub struct GrpcUdpResponder;

impl GrpcUdpResponder {
    /// Builds raw wire frame buffers for a gRPC response over UDP.
    pub fn build_response_frames(response: &L7Response) -> BytesMut {
        let mut header_payload = BytesMut::new();
        encode_qpack_response(response.status, &response.headers, &mut header_payload);

        let mut write_buf = BytesMut::new();
        encode_udp_frame(&UdpFrame::Headers(header_payload.freeze()), &mut write_buf);

        if let Body::Bytes(ref b) = response.body
            && !b.is_empty()
        {
            encode_udp_frame(&UdpFrame::Data(b.clone()), &mut write_buf);
        }

        write_buf
    }

    /// Formats a trailers-only gRPC response for rapid rejection or error signaling.
    pub fn build_trailers_only(status: GrpcStatus, message: Option<&str>) -> BytesMut {
        let resp = status.to_l7_response(message);
        Self::build_response_frames(&resp)
    }
}
