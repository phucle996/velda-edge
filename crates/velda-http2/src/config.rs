//! Configuration parameters for HTTP/2 server and client connections (RFC 9113).

/// Tunable settings for HTTP/2 connection parameters, framing, and flow control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http2Config {
    /// Maximum number of concurrent streams allowed per connection.
    pub max_concurrent_streams: u32,
    /// Initial flow control window size (in bytes) for the overall connection.
    pub initial_connection_window_size: u32,
    /// Initial flow control window size (in bytes) for individual streams.
    pub initial_stream_window_size: u32,
    /// Maximum frame size (in bytes) that can be sent or received.
    pub max_frame_size: u32,
    /// Maximum size of the header list (in bytes) that is accepted.
    pub max_header_list_size: u32,
    /// Whether server push is enabled (default: false for security & simplicity).
    pub enable_push: bool,
    /// Maximum buffer size for sending data chunks.
    pub max_send_buffer_size: usize,
}

impl Default for Http2Config {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 1024,
            // 1MB connection window for high throughput multiplexing
            initial_connection_window_size: 1024 * 1024,
            // 1MB stream window for fast single-stream downloads
            initial_stream_window_size: 1024 * 1024,
            // 16KB standard RFC frame size
            max_frame_size: 16 * 1024,
            // 64KB maximum header list size
            max_header_list_size: 64 * 1024,
            enable_push: false,
            max_send_buffer_size: 1024 * 1024,
        }
    }
}

impl Http2Config {
    /// Creates a new [`Http2Config`] with default parameters.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum concurrent streams allowed on the connection.
    #[inline]
    pub const fn with_max_concurrent_streams(mut self, max: u32) -> Self {
        self.max_concurrent_streams = max;
        self
    }

    /// Sets the initial connection-level flow control window size.
    #[inline]
    pub const fn with_initial_connection_window_size(mut self, size: u32) -> Self {
        self.initial_connection_window_size = size;
        self
    }

    /// Sets the initial stream-level flow control window size.
    #[inline]
    pub const fn with_initial_stream_window_size(mut self, size: u32) -> Self {
        self.initial_stream_window_size = size;
        self
    }

    /// Sets the maximum HTTP/2 frame payload size.
    #[inline]
    pub const fn with_max_frame_size(mut self, size: u32) -> Self {
        self.max_frame_size = size;
        self
    }

    /// Sets the maximum allowed size of header lists.
    #[inline]
    pub const fn with_max_header_list_size(mut self, size: u32) -> Self {
        self.max_header_list_size = size;
        self
    }

    /// Enables or disables HTTP/2 server push.
    #[inline]
    pub const fn with_enable_push(mut self, enable: bool) -> Self {
        self.enable_push = enable;
        self
    }
}
