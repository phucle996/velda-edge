//! Upstream gRPC Client Request Entity over UDP / QUIC.

use bytes::{Bytes, BytesMut};
use http::header::CONTENT_TYPE;
use http::{HeaderMap, Method, Uri, Version};
use velda_core::{Body, L7Request};

use crate::frame::encode_grpc_frame;
use crate::udp::wire::{UdpFrame, encode_qpack_request, encode_udp_frame};
use crate::wire::GrpcWire;

/// Upstream gRPC client request head metadata over UDP.
#[derive(Debug, Clone)]
pub struct GrpcUdpClientRequestHead {
    /// Request URI.
    pub uri: Uri,
    /// Upstream target authority.
    pub authority: Option<String>,
    /// Request headers / metadata.
    pub headers: HeaderMap,
}

impl GrpcUdpClientRequestHead {
    /// Creates a new request head.
    #[inline]
    pub fn new(uri: Uri, authority: Option<String>, headers: HeaderMap) -> Self {
        Self {
            uri,
            authority,
            headers,
        }
    }
}

/// Upstream protocol-owned gRPC client request over UDP.
#[derive(Debug, Clone)]
pub struct GrpcUdpClientRequest {
    /// Request head metadata.
    pub head: GrpcUdpClientRequestHead,
    /// Request payload body (typically Length-Prefixed Message).
    pub body: Body,
}

impl GrpcUdpClientRequest {
    /// Creates a new client request.
    #[inline]
    pub fn new(head: GrpcUdpClientRequestHead, body: Body) -> Self {
        Self { head, body }
    }

    /// Creates a unary request with an uncompressed payload.
    pub fn unary(uri: Uri, authority: Option<String>, payload: Option<Bytes>) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, GrpcWire::CONTENT_TYPE_VALUE);
        if let Some(ref auth) = authority
            && let Ok(v) = auth.parse()
        {
            headers.insert(http::header::HOST, v);
        }

        let body = match payload {
            Some(data) => {
                let mut buf = BytesMut::new();
                encode_grpc_frame(&data, false, &mut buf);
                Body::Bytes(buf.freeze())
            }
            None => Body::Empty,
        };

        Self {
            head: GrpcUdpClientRequestHead::new(uri, authority, headers),
            body,
        }
    }

    /// Converts from canonical [`L7Request`].
    pub fn from_l7(req: &L7Request) -> Self {
        let authority = req
            .headers
            .get(":authority")
            .and_then(|v| v.to_str().ok().map(|s| s.to_string()))
            .or_else(|| {
                req.headers
                    .get(http::header::HOST)
                    .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
            })
            .or_else(|| req.uri.authority().map(|a| a.as_str().to_string()));

        Self {
            head: GrpcUdpClientRequestHead::new(req.uri.clone(), authority, req.headers.clone()),
            body: req.body.clone(),
        }
    }

    /// Converts into canonical [`L7Request`].
    pub fn into_l7(self) -> L7Request {
        L7Request::new(
            Method::POST,
            self.head.uri,
            Version::HTTP_3,
            self.head.headers,
            self.body,
        )
    }

    /// Encodes this request into UDP wire frames (QPACK HEADERS + optional DATA).
    pub fn encode_frames(&self) -> BytesMut {
        let mut header_buf = BytesMut::new();
        encode_qpack_request(
            &Method::POST,
            &self.head.uri,
            &self.head.headers,
            &mut header_buf,
        );
        let header_frame = UdpFrame::Headers(header_buf.freeze());

        let mut send_buf = BytesMut::new();
        encode_udp_frame(&header_frame, &mut send_buf);

        if let Body::Bytes(ref b) = self.body
            && !b.is_empty()
        {
            let data_frame = UdpFrame::Data(b.clone());
            encode_udp_frame(&data_frame, &mut send_buf);
        }

        send_buf
    }
}
