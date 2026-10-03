//! Configuration parameters for HTTP/2 server and client connections (RFC 9113).
//!
//! Owns protocol-specific settings, safety limits, and framing parameters.
//! Scaled by host [`MemoryTier`] with all 7 tiers explicitly defined,
//! keeping the hot path zero-IO and free from JSON parsing.

use velda_core::hardware::MemoryTier;

/// Tunable settings for HTTP/2 connection parameters, framing, flow control, and safety limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http2Config {
    /// Maximum request/response body size in bytes.
    pub max_body_size: usize,
    /// Maximum header size in bytes.
    pub max_header_size: usize,
    /// Maximum number of headers accepted.
    pub max_headers: usize,
    /// Connection idle timeout in milliseconds.
    pub idle_timeout_ms: u64,
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
    /// Maximum number of consecutive RST_STREAM frames accepted before terminating connection (Rapid Reset mitigation).
    pub max_consecutive_resets: u32,
    /// Maximum number of unacknowledged control frames (PING, SETTINGS) queued.
    pub max_pending_control_frames: u32,
    /// Maximum number of CONTINUATION frames permitted per header block (CONTINUATION flood mitigation).
    pub max_continuation_frames: u32,
}

impl Http2Config {
    /// Probes host hardware topology and derives the configuration for the active [`MemoryTier`].
    pub fn auto() -> Self {
        let hw = velda_core::global_hardware_topology();
        Self::for_tier(hw.memory_tier())
    }

    /// Derives an [`Http2Config`] scaled deterministically for the given [`MemoryTier`].
    ///
    /// Matches all 7 tiers explicitly to ensure unambiguous capacity planning without magic numbers.
    pub const fn for_tier(tier: MemoryTier) -> Self {
        match tier {
            MemoryTier::Constrained => Self {
                max_body_size: 2 * 1024 * 1024,
                max_header_size: 16 * 1024,
                max_headers: 32,
                idle_timeout_ms: 15_000,
                max_concurrent_streams: 64,
                initial_connection_window_size: 128 * 1024,
                initial_stream_window_size: 64 * 1024,
                max_frame_size: 16 * 1024,
                max_header_list_size: 16 * 1024,
                enable_push: false,
                max_send_buffer_size: 64 * 1024,
                max_consecutive_resets: 100,
                max_pending_control_frames: 100,
                max_continuation_frames: 8,
            },
            MemoryTier::Small => Self {
                max_body_size: 4 * 1024 * 1024,
                max_header_size: 32 * 1024,
                max_headers: 48,
                idle_timeout_ms: 30_000,
                max_concurrent_streams: 128,
                initial_connection_window_size: 256 * 1024,
                initial_stream_window_size: 64 * 1024,
                max_frame_size: 16 * 1024,
                max_header_list_size: 32 * 1024,
                enable_push: false,
                max_send_buffer_size: 64 * 1024,
                max_consecutive_resets: 200,
                max_pending_control_frames: 200,
                max_continuation_frames: 8,
            },
            MemoryTier::Medium => Self {
                max_body_size: 10 * 1024 * 1024,
                max_header_size: 64 * 1024,
                max_headers: 64,
                idle_timeout_ms: 30_000,
                max_concurrent_streams: 256,
                initial_connection_window_size: 256 * 1024,
                initial_stream_window_size: 64 * 1024,
                max_frame_size: 16 * 1024,
                max_header_list_size: 64 * 1024,
                enable_push: false,
                max_send_buffer_size: 64 * 1024,
                max_consecutive_resets: 500,
                max_pending_control_frames: 200,
                max_continuation_frames: 16,
            },
            MemoryTier::Large => Self {
                max_body_size: 16 * 1024 * 1024,
                max_header_size: 64 * 1024,
                max_headers: 96,
                idle_timeout_ms: 45_000,
                max_concurrent_streams: 256,
                initial_connection_window_size: 512 * 1024,
                initial_stream_window_size: 128 * 1024,
                max_frame_size: 16 * 1024,
                max_header_list_size: 64 * 1024,
                enable_push: false,
                max_send_buffer_size: 128 * 1024,
                max_consecutive_resets: 1_000,
                max_pending_control_frames: 500,
                max_continuation_frames: 16,
            },
            MemoryTier::XLarge => Self {
                max_body_size: 32 * 1024 * 1024,
                max_header_size: 128 * 1024,
                max_headers: 128,
                idle_timeout_ms: 60_000,
                max_concurrent_streams: 512,
                initial_connection_window_size: 512 * 1024,
                initial_stream_window_size: 128 * 1024,
                max_frame_size: 16 * 1024,
                max_header_list_size: 128 * 1024,
                enable_push: false,
                max_send_buffer_size: 128 * 1024,
                max_consecutive_resets: 2_000,
                max_pending_control_frames: 500,
                max_continuation_frames: 32,
            },
            MemoryTier::TwoXLarge => Self {
                max_body_size: 64 * 1024 * 1024,
                max_header_size: 128 * 1024,
                max_headers: 128,
                idle_timeout_ms: 60_000,
                max_concurrent_streams: 512,
                initial_connection_window_size: 1024 * 1024,
                initial_stream_window_size: 256 * 1024,
                max_frame_size: 16 * 1024,
                max_header_list_size: 128 * 1024,
                enable_push: false,
                max_send_buffer_size: 256 * 1024,
                max_consecutive_resets: 5_000,
                max_pending_control_frames: 1_000,
                max_continuation_frames: 32,
            },
            MemoryTier::Ultra => Self {
                max_body_size: 128 * 1024 * 1024,
                max_header_size: 256 * 1024,
                max_headers: 256,
                idle_timeout_ms: 120_000,
                max_concurrent_streams: 512,
                initial_connection_window_size: 1024 * 1024,
                initial_stream_window_size: 256 * 1024,
                max_frame_size: 16 * 1024,
                max_header_list_size: 256 * 1024,
                enable_push: false,
                max_send_buffer_size: 256 * 1024,
                max_consecutive_resets: 10_000,
                max_pending_control_frames: 1_000,
                max_continuation_frames: 32,
            },
        }
    }

    /// Sets the maximum body size in bytes.
    #[inline]
    pub const fn with_max_body_size(mut self, size: usize) -> Self {
        self.max_body_size = size;
        self
    }

    /// Sets the maximum header size in bytes.
    #[inline]
    pub const fn with_max_header_size(mut self, size: usize) -> Self {
        self.max_header_size = size;
        self.max_header_list_size = size as u32;
        self
    }

    /// Sets the maximum number of headers.
    #[inline]
    pub const fn with_max_headers(mut self, max: usize) -> Self {
        self.max_headers = max;
        self
    }

    /// Sets the connection idle timeout in milliseconds.
    #[inline]
    pub const fn with_idle_timeout_ms(mut self, ms: u64) -> Self {
        self.idle_timeout_ms = ms;
        self
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

    /// Sets the maximum buffer size for sending data chunks.
    #[inline]
    pub const fn with_max_send_buffer_size(mut self, size: usize) -> Self {
        self.max_send_buffer_size = size;
        self
    }

    /// Sets the maximum consecutive RST_STREAM frames before closing connection.
    #[inline]
    pub const fn with_max_consecutive_resets(mut self, n: u32) -> Self {
        self.max_consecutive_resets = n;
        self
    }

    /// Sets the maximum pending control frames.
    #[inline]
    pub const fn with_max_pending_control_frames(mut self, n: u32) -> Self {
        self.max_pending_control_frames = n;
        self
    }

    /// Sets the maximum CONTINUATION frames per header block.
    #[inline]
    pub const fn with_max_continuation_frames(mut self, n: u32) -> Self {
        self.max_continuation_frames = n;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_returns_valid_config() {
        let config = Http2Config::auto();
        assert!(config.max_concurrent_streams >= 64);
        assert!(config.max_header_size >= 16 * 1024);
        assert_eq!(config.max_frame_size, 16 * 1024);
    }

    #[test]
    fn test_for_tier_explicit_all_tiers() {
        let constrained = Http2Config::for_tier(MemoryTier::Constrained);
        assert_eq!(constrained.max_concurrent_streams, 64);
        assert_eq!(constrained.initial_connection_window_size, 128 * 1024);
        assert_eq!(constrained.initial_stream_window_size, 64 * 1024);
        assert_eq!(constrained.max_body_size, 2 * 1024 * 1024);

        let medium = Http2Config::for_tier(MemoryTier::Medium);
        assert_eq!(medium.max_concurrent_streams, 256);
        assert_eq!(medium.initial_connection_window_size, 256 * 1024);
        assert_eq!(medium.initial_stream_window_size, 64 * 1024);
        assert_eq!(medium.max_body_size, 10 * 1024 * 1024);

        let ultra = Http2Config::for_tier(MemoryTier::Ultra);
        assert_eq!(ultra.max_concurrent_streams, 512);
        assert_eq!(ultra.initial_connection_window_size, 1024 * 1024);
        assert_eq!(ultra.initial_stream_window_size, 256 * 1024);
        assert_eq!(ultra.max_body_size, 128 * 1024 * 1024);
    }
}
