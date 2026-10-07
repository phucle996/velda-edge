//! Upstream HTTP/3 Client Request Entity and Wire Frame Serialization (RFC 9114).

use bytes::BytesMut;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::{Method, Uri, Version};
use velda_core::{Body, L7Request};

use crate::frame::{Http3Frame, encode_frame};
use crate::qpack::encode_qpack_request;
use crate::server::request::Http3ServerRequest;

/// Upstream HTTP/3 client request head metadata.
#[derive(Debug, Clone)]
pub struct Http3ClientRequestHead {
    /// HTTP method.
    pub method: Method,
    /// Request URI.
    pub uri: Uri,
    /// Protocol version (HTTP/3.0).
    pub version: Version,
    /// Request headers.
    pub headers: HeaderMap,
}

impl Http3ClientRequestHead {
    /// Creates a new HTTP/3 client request head.
    #[inline]
    pub fn new(method: Method, uri: Uri, headers: HeaderMap) -> Self {
        Self {
            method,
            uri,
            version: Version::HTTP_3,
            headers,
        }
    }
}

/// Upstream HTTP/3 client request entity.
#[derive(Debug, Clone)]
pub struct Http3ClientRequest {
    /// Request head metadata.
    pub head: Http3ClientRequestHead,
    /// Request payload body.
    pub body: Body,
}

impl Http3ClientRequest {
    /// Creates a new HTTP/3 client request.
    #[inline]
    pub fn new(method: Method, uri: Uri, headers: HeaderMap, body: Body) -> Self {
        Self {
            head: Http3ClientRequestHead::new(method, uri, headers),
            body,
        }
    }

    /// Creates a client request from head and body.
    #[inline]
    pub fn from_head_and_body(head: Http3ClientRequestHead, body: Body) -> Self {
        Self { head, body }
    }

    /// Converts from canonical [`L7Request`].
    #[inline]
    pub fn from_l7(req: L7Request) -> Self {
        Self {
            head: Http3ClientRequestHead::new(req.method, req.uri, req.headers),
            body: req.body,
        }
    }

    /// Converts into canonical [`L7Request`].
    #[inline]
    pub fn into_l7(self) -> L7Request {
        L7Request::new(
            self.head.method,
            self.head.uri,
            self.head.version,
            self.head.headers,
            self.body,
        )
    }

    /// Converts from downstream [`Http3ServerRequest`].
    #[inline]
    pub fn from_server_request(req: Http3ServerRequest) -> Self {
        Self {
            head: Http3ClientRequestHead::new(req.head.method, req.head.uri, req.head.headers),
            body: req.body,
        }
    }

    /// Appends a header using fluent builder pattern.
    #[inline]
    pub fn with_header(mut self, name: HeaderName, val: HeaderValue) -> Self {
        self.head.headers.insert(name, val);
        self
    }

    /// Serializes this client request into HTTP/3 QPACK HEADERS and DATA frames.
    pub fn encode_to_frames(&self) -> BytesMut {
        let mut header_buf = BytesMut::new();
        encode_qpack_request(
            &self.head.method,
            &self.head.uri,
            &self.head.headers,
            &mut header_buf,
        );
        let header_frame = Http3Frame::Headers(header_buf.freeze());

        let mut send_buf = BytesMut::new();
        encode_frame(&header_frame, &mut send_buf);

        if let Body::Bytes(ref b) = self.body
            && !b.is_empty()
        {
            let data_frame = Http3Frame::Data(b.clone());
            encode_frame(&data_frame, &mut send_buf);
        }

        send_buf
    }
}
