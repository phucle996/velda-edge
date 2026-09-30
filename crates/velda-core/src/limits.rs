//! Generic ingress limits and safety thresholds for L7 protocols.
//!
//! Applies consistently across HTTP/1.1, HTTP/2, HTTP/3, and gRPC listeners.

/// Ingress safety limits applicable to all L7 application protocols (HTTP/1.1, HTTP/2, HTTP/3, gRPC).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressLimits {
    /// Maximum allowed request body / message payload size in bytes.
    /// Equivalent to NGINX `client_max_body_size` or gRPC `max_receive_message_length`.
    pub max_body_size: usize,

    /// Maximum allowed size of the raw header section / HPACK / QPACK block in bytes.
    /// Equivalent to NGINX `large_client_header_buffers` or H2 `SETTINGS_MAX_HEADER_LIST_SIZE`.
    pub max_header_size: usize,

    /// Maximum number of header / metadata fields permitted per request.
    pub max_headers: usize,

    /// Maximum stream / request read timeout in milliseconds.
    pub request_timeout_ms: u64,
}

impl IngressLimits {
    /// Creates a new `IngressLimits` with explicitly specified limit thresholds.
    pub const fn new(
        max_body_size: usize,
        max_header_size: usize,
        max_headers: usize,
        request_timeout_ms: u64,
    ) -> Self {
        Self {
            max_body_size,
            max_header_size,
            max_headers,
            request_timeout_ms,
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
