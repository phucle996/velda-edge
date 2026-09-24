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
}
