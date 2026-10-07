//! Upstream gRPC Client Request Entity and HTTP/2 framing.

use bytes::{Bytes, BytesMut};
use http::header::CONTENT_TYPE;
use http::{HeaderMap, Method, Request, Uri, Version};
use velda_core::{Body, L7Request};

use crate::error::GrpcError;
use crate::frame::encode_grpc_frame;
use crate::wire::GrpcWire;

/// Upstream gRPC client request head metadata.
#[derive(Debug, Clone)]
pub struct GrpcClientRequestHead {
    /// Request URI (e.g. `/package.Service/Method`).
    pub uri: Uri,
    /// Upstream target authority.
    pub authority: Option<String>,
    /// Request headers / metadata.
    pub headers: HeaderMap,
}

impl GrpcClientRequestHead {
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

/// Upstream protocol-owned gRPC client request.
#[derive(Debug, Clone)]
pub struct GrpcClientRequest {
    /// Request head metadata.
    pub head: GrpcClientRequestHead,
    /// Request payload body (typically Length-Prefixed Message).
    pub body: Body,
}

impl GrpcClientRequest {
    /// Creates a new client request.
    #[inline]
    pub fn new(head: GrpcClientRequestHead, body: Body) -> Self {
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
            head: GrpcClientRequestHead::new(uri, authority, headers),
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
            head: GrpcClientRequestHead::new(req.uri.clone(), authority, req.headers.clone()),
            body: req.body.clone(),
        }
    }

    /// Converts into canonical [`L7Request`].
    pub fn into_l7(self) -> L7Request {
        L7Request::new(
            Method::POST,
            self.head.uri,
            Version::HTTP_2,
            self.head.headers,
            self.body,
        )
    }

    /// Encodes this request into an `http::Request<()>` ready for transmission over an H2 send stream.
    /// Returns `(request, end_of_stream)`.
    pub fn encode_http_request(&self) -> Result<(Request<()>, bool), GrpcError> {
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(&self.head.uri)
            .version(Version::HTTP_2);

        for (name, val) in &self.head.headers {
            builder = builder.header(name, val);
        }

        let has_body = match &self.body {
            Body::Bytes(b) => !b.is_empty(),
            Body::Empty => false,
        };

        let req = builder.body(()).map_err(GrpcError::Http)?;
        Ok((req, !has_body))
    }
}
