//! Configuration parameters for HTTP/2 server and client connections (RFC 9113).
//!
//! Owns protocol-specific settings, safety limits, and framing parameters.
//! Continuously scaled by host hardware topology,
//! keeping the hot path zero-IO and free from JSON parsing.

/// Tunable settings for HTTP/2 connection parameters, framing, flow control, and safety limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http2Config {
    /// Maximum request/response body size in bytes for buffered requests.
    ///
    /// Note: Enforced strictly for `Buffered` strategy. Streaming strategies
    /// (`ServerStream`, `ClientStream`, `Duplex`) stream continuously with constant memory footprint.
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
    /// Maximum requests served per downstream HTTP/2 connection before graceful draining via GOAWAY (0 = unlimited).
    pub max_requests_per_connection: u32,
    /// Maximum lifetime of downstream HTTP/2 connection in seconds before graceful draining via GOAWAY (0 = unlimited).
    pub max_connection_duration_secs: u32,
}

#[inline]
const fn clamp_usize(val: usize, min: usize, max: usize) -> usize {
    if val < min {
        min
    } else if val > max {
        max
    } else {
        val
    }
}

#[inline]
const fn clamp_u32(val: u32, min: u32, max: u32) -> u32 {
    if val < min {
        min
    } else if val > max {
        max
    } else {
        val
    }
}

#[inline]
const fn max_usize(a: usize, b: usize) -> usize {
    if a > b { a } else { b }
}

impl Http2Config {
    /// Probes host hardware topology and derives the configuration smoothly from CPU cores and RAM bytes.
    pub fn auto() -> Self {
        let hw = velda_core::global_hardware_topology();
        Self::from_hardware(hw.available_cores, hw.memory.total_bytes)
    }

    /// Derives an [`Http2Config`] scaled continuously from host CPU cores and RAM bytes.
    ///
    /// Eliminates rigid discrete quantization boundaries, smoothly scaling flow-control windows and concurrency
    /// using proportional formulas with strict `[min, max]` safety clamps.
    pub const fn from_hardware(cores: usize, ram_bytes: usize) -> Self {
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;

        let cores = max_usize(cores, 1);

        // 1. Max concurrent streams: (cores * 64).clamp(128, 512)
        let max_concurrent_streams = clamp_u32((cores * 64) as u32, 128, 512);

        // 2. Initial stream window (receive): 65535 (RFC 9113 standard)
        let initial_stream_window_size = 65535;

        // 3. Initial connection window: (ram_bytes / 16384).clamp(1 MB, 2 MB)
        let initial_connection_window_size =
            clamp_u32((ram_bytes / 16384) as u32, 1024 * 1024, 2 * 1024 * 1024);

        // 4. Max send buffer size: (ram_bytes / 65536).clamp(64 KB, 128 KB)
        let max_send_buffer_size = clamp_usize(ram_bytes / 65536, 64 * KB, 128 * KB);

        // 5. Max body size for buffered requests: (ram_bytes / 256).clamp(8 MB, 64 MB)
        let max_body_size = clamp_usize(ram_bytes / 256, 8 * MB, 64 * MB);

        // 6. Max header size: (ram_bytes / 131072).clamp(16 KB, 64 KB)
        let max_header_size = clamp_usize(ram_bytes / 131_072, 16 * KB, 64 * KB);

        // 7. Max headers count: clamped to [32, 96]
        let max_headers = clamp_usize(cores * 16, 32, 96);

        // 8. Idle timeout and safety limits
        let idle_timeout_ms = 30_000;
        let max_frame_size = 64 * 1024;
        let max_header_list_size = max_header_size as u32;

        let max_consecutive_resets = clamp_u32((cores * 200) as u32, 200, 2000);
        let max_pending_control_frames = clamp_u32((cores * 100) as u32, 100, 1000);
        let max_continuation_frames = clamp_u32((cores * 4) as u32, 8, 32);

        Self {
            max_body_size,
            max_header_size,
            max_headers,
            idle_timeout_ms,
            max_concurrent_streams,
            initial_connection_window_size,
            initial_stream_window_size,
            max_frame_size,
            max_header_list_size,
            enable_push: false,
            max_send_buffer_size,
            max_consecutive_resets,
            max_pending_control_frames,
            max_continuation_frames,
            max_requests_per_connection: 10_000_000,
            max_connection_duration_secs: 7200,
        }
    }

    /// Derives an [`Http2Config`] scaled deterministically from host hardware topology.
    pub fn for_topology(topo: &velda_core::HardwareTopology) -> Self {
        Self::from_hardware(topo.available_cores, topo.memory_bytes)
    }

    /// Sets the maximum body size in bytes for buffered requests.
    ///
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

    /// Sets the maximum requests served per connection before graceful draining.
    #[inline]
    pub const fn with_max_requests_per_connection(mut self, n: u32) -> Self {
        self.max_requests_per_connection = n;
        self
    }

    /// Sets the maximum connection duration in seconds before graceful draining.
    #[inline]
    pub const fn with_max_connection_duration_secs(mut self, secs: u32) -> Self {
        self.max_connection_duration_secs = secs;
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
        assert!(config.max_frame_size >= 16 * 1024);
    }

    #[test]
    fn test_from_hardware_continuous_scaling() {
        let edge_2c_4g = Http2Config::from_hardware(2, 4 * 1024 * 1024 * 1024);
        assert_eq!(edge_2c_4g.max_concurrent_streams, 128);
        assert_eq!(edge_2c_4g.initial_connection_window_size, 1024 * 1024);
        assert_eq!(edge_2c_4g.initial_stream_window_size, 65535);

        let vm_4c_16g = Http2Config::from_hardware(4, 16 * 1024 * 1024 * 1024);
        assert_eq!(vm_4c_16g.max_concurrent_streams, 256);
        assert_eq!(vm_4c_16g.initial_connection_window_size, 1024 * 1024);
        assert_eq!(vm_4c_16g.initial_stream_window_size, 65535);

        let server_64c_128g = Http2Config::from_hardware(64, 128 * 1024 * 1024 * 1024);
        assert_eq!(server_64c_128g.max_concurrent_streams, 512);
        assert_eq!(
            server_64c_128g.initial_connection_window_size,
            2 * 1024 * 1024
        );
        assert_eq!(server_64c_128g.initial_stream_window_size, 65535);
    }

    #[test]
    fn test_for_topology() {
        let topo = velda_core::HardwareTopology::with_workers_and_memory(4, 8 * 1024 * 1024 * 1024);
        let config = Http2Config::for_topology(&topo);
        assert_eq!(config.max_concurrent_streams, 256);
        assert_eq!(config.initial_connection_window_size, 1024 * 1024);
        assert_eq!(config.initial_stream_window_size, 65535);
        assert_eq!(config.max_body_size, 32 * 1024 * 1024);
        assert_eq!(config.max_frame_size, 64 * 1024);
    }
}
