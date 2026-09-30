//! Generic ingress limits and safety thresholds for L7 protocols.
//!
//! Applies consistently across HTTP/1.1, HTTP/2, HTTP/3, and gRPC listeners.

/// Ingress safety limits applicable to all L7 application protocols (HTTP/1.1, HTTP/2, HTTP/3, gRPC).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressLimits {
    /// Maximum allowed request body / message payload size in bytes.
    /// Equivalent to NGINX `client_max_body_size` or gRPC `max_receive_message_length`.
    /// Default: 10 MB (10 * 1024 * 1024 bytes).
    pub max_body_size: usize,

    /// Maximum allowed size of the raw header section / HPACK / QPACK block in bytes.
    /// Equivalent to NGINX `large_client_header_buffers` or H2 `SETTINGS_MAX_HEADER_LIST_SIZE`.
    /// Default: 64 KB (64 * 1024 bytes).
    pub max_header_size: usize,

    /// Maximum number of header / metadata fields permitted per request.
    /// Default: 64 headers.
    pub max_headers: usize,

    /// Maximum stream / request read timeout in milliseconds.
    /// Default: 30,000 ms (30 seconds).
    pub request_timeout_ms: u64,
}

impl Default for IngressLimits {
    fn default() -> Self {
        Self::new()
    }
}

impl IngressLimits {
    /// Creates a new `IngressLimits` with production default values.
    pub const fn new() -> Self {
        Self {
            max_body_size: 10 * 1024 * 1024,
            max_header_size: 64 * 1024,
            max_headers: 64,
            request_timeout_ms: 30_000,
        }
    }

    /// Sets the maximum body size in bytes.
    pub const fn with_max_body_size(mut self, size: usize) -> Self {
        self.max_body_size = size;
        self
    }

    /// Sets the maximum header size in bytes.
    pub const fn with_max_header_size(mut self, size: usize) -> Self {
        self.max_header_size = size;
        self
    }

    /// Sets the maximum number of headers.
    pub const fn with_max_headers(mut self, count: usize) -> Self {
        self.max_headers = count;
        self
    }

    /// Sets the request timeout in milliseconds.
    pub const fn with_request_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.request_timeout_ms = timeout_ms;
        self
    }
}
