//! Upstream HTTP/3 Client Response Entity and Ingestion (RFC 9114).

use bytes::BytesMut;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::{StatusCode, Version};
use velda_core::{Body, L7Response};

/// Upstream HTTP/3 client response head metadata.
#[derive(Debug, Clone)]
pub struct Http3ClientResponseHead {
    /// HTTP status code.
    pub status: StatusCode,
    /// Protocol version (HTTP/3.0).
    pub version: Version,
    /// Response headers.
    pub headers: HeaderMap,
}

impl Http3ClientResponseHead {
    /// Creates a new HTTP/3 client response head.
    #[inline]
    pub fn new(status: StatusCode, headers: HeaderMap) -> Self {
        Self {
            status,
            version: Version::HTTP_3,
            headers,
        }
    }
}

/// Upstream HTTP/3 client response entity.
#[derive(Debug, Clone)]
pub struct Http3ClientResponse {
    /// HTTP status code.
    pub status: StatusCode,
    /// Protocol version (HTTP/3.0).
    pub version: Version,
    /// Response headers.
    pub headers: HeaderMap,
    /// Response payload body.
    pub body: Body,
}

impl Http3ClientResponse {
    /// Creates a new HTTP/3 client response.
    #[inline]
    pub fn new(status: StatusCode, headers: HeaderMap, body: Body) -> Self {
        Self {
            status,
            version: Version::HTTP_3,
            headers,
            body,
        }
    }

    /// Fast constructor from status code and byte payload.
    #[inline]
    pub fn from_bytes(status: StatusCode, bytes: Vec<u8>) -> Self {
        let body = if bytes.is_empty() {
            Body::Empty
        } else {
            Body::Bytes(bytes.into())
        };
        Self::new(status, HeaderMap::new(), body)
    }

    /// Appends a header using fluent builder pattern.
    #[inline]
    pub fn with_header(mut self, name: HeaderName, val: HeaderValue) -> Self {
        self.headers.insert(name, val);
        self
    }

    /// Returns response head metadata.
    #[inline]
    pub fn head(&self) -> Http3ClientResponseHead {
        Http3ClientResponseHead::new(self.status, self.headers.clone())
    }

    /// Constructs response from raw decoded frames and method according to RFC 9114 rules.
    pub fn from_frames(
        method: &http::Method,
        headers_opt: Option<(Option<StatusCode>, HeaderMap)>,
        body_opt: Option<BytesMut>,
    ) -> Self {
        let (status, headers) = headers_opt.unwrap_or((Some(StatusCode::OK), HeaderMap::new()));
        let status_code = status.unwrap_or(StatusCode::OK);
        let is_no_body = *method == http::Method::HEAD
            || status_code.is_informational()
            || status_code == StatusCode::NO_CONTENT
            || status_code == StatusCode::NOT_MODIFIED;

        let body = if is_no_body {
            Body::Empty
        } else {
            body_opt
                .map(|b| {
                    if b.is_empty() {
                        Body::Empty
                    } else {
                        Body::Bytes(b.freeze())
                    }
                })
                .unwrap_or(Body::Empty)
        };

        Self::new(status_code, headers, body)
    }

    /// Converts from canonical [`L7Response`].
    #[inline]
    pub fn from_l7(resp: L7Response) -> Self {
        Self {
            status: resp.status,
            version: Version::HTTP_3,
            headers: resp.headers,
            body: resp.body,
        }
    }

    /// Converts into canonical [`L7Response`].
    #[inline]
    pub fn into_l7(self) -> L7Response {
        L7Response::new(self.status, self.version, self.headers, self.body)
    }
}
