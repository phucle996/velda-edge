use http::{HeaderMap, StatusCode, Version};

use super::request::Body;

/// HTTP response returned by the Velda L7 pipeline or upstream.
#[derive(Debug, Clone)]
pub struct L7Response {
    pub status: StatusCode,
    pub version: Version,
    pub headers: HeaderMap,
    pub body: Body,
}

impl L7Response {
    /// Creates a new HTTP response.
    #[inline]
    pub fn new(status: StatusCode, version: Version, headers: HeaderMap, body: Body) -> Self {
        Self {
            status,
            version,
            headers,
            body,
        }
    }

    /// Returns `true` when the response contains a non-empty body.
    #[inline]
    pub fn has_body(&self) -> bool {
        !self.body.is_empty()
    }

    /// Creates an HTTP response with no body (e.g. 204 No Content, 304 Not Modified, or fast 403).
    #[inline]
    pub fn empty(status: StatusCode) -> Self {
        Self {
            status,
            version: Version::HTTP_11,
            headers: HeaderMap::new(),
            body: Body::Empty,
        }
    }

    /// Creates an HTTP response with in-memory payload bytes.
    #[inline]
    pub fn from_bytes(status: StatusCode, bytes: impl Into<bytes::Bytes>) -> Self {
        Self {
            status,
            version: Version::HTTP_11,
            headers: HeaderMap::new(),
            body: Body::Bytes(bytes.into()),
        }
    }

    /// Appends a header to the response.
    #[inline]
    pub fn with_header(mut self, name: http::header::HeaderName, value: http::HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;
    use http::header::CONTENT_TYPE;

    #[test]
    fn test_l7_response_constructors() {
        let empty_404 = L7Response::empty(StatusCode::NOT_FOUND);
        assert_eq!(empty_404.status, StatusCode::NOT_FOUND);
        assert!(!empty_404.has_body());

        let ok_json = L7Response::from_bytes(StatusCode::OK, b"{\"status\":\"ok\"}".to_vec())
            .with_header(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        assert_eq!(ok_json.status, StatusCode::OK);
        assert!(ok_json.has_body());
        assert_eq!(
            ok_json.headers.get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }
}
