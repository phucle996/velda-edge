//! Configuration parameters for HTTP/3 server and client pipelines (RFC 9114).
//!
//! Scaled dynamically from actual host/container RAM bytes once at bootstrap.
//! Zero-IO, zero JSON parsing, zero allocation on hot paths.

/// Tunable settings for HTTP/3 framing, QPACK metadata limits, and stream concurrency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http3Config {
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
    /// Maximum number of concurrent bidirectional streams.
    pub max_concurrent_streams: u32,
    /// Maximum number of concurrent unidirectional streams (QPACK/control).
    pub max_concurrent_uni_streams: u32,
    /// Maximum dynamic table capacity for QPACK header decompression in bytes.
    pub max_qpack_table_capacity: usize,
    /// Maximum pending unauthenticated handshakes before activating defensive stateless Retry tokens (0 = unlimited).
    pub max_pending_handshakes: u32,
    /// Per-stream receive flow-control window (initial max stream data) in bytes.
    pub stream_receive_window: u32,
    /// Connection-level receive flow-control window (initial max data) in bytes.
    pub conn_receive_window: u64,
    /// Connection-level send window in bytes.
    pub conn_send_window: u64,
    /// Initial congestion window in bytes (for Cubic congestion controller).
    pub initial_congestion_window: u64,
    /// Maximum aggregate pending outbound response bytes across all active streams in an engine worker.
    ///
    /// Sized dynamically from host RAM. When slow readers or unacknowledged stream writes
    /// exceed this ceiling, the engine resets stalled streams with `H3_REQUEST_CANCELLED` to prevent
    /// memory exhaustion (defensive protection against Slowloris attacks).
    pub max_pending_bytes: usize,
}

impl Http3Config {
    /// Minimum connection send window (256 KB) ensuring high-throughput WAN streaming on low-RAM nodes.
    pub const MIN_CONN_SEND_WINDOW: u64 = 256 * 1024;

    /// Maximum connection send window (4 MB) preventing memory exhaustion under high concurrency.
    pub const MAX_CONN_SEND_WINDOW: u64 = 4 * 1024 * 1024;

    /// RAM divisor for connection send window (1/8192 of physical RAM).
    pub const SEND_WINDOW_RAM_DIVISOR: usize = 8192;

    /// Probes host hardware topology and derives the configuration smoothly from CPU cores and RAM bytes.
    pub fn auto() -> Self {
        let hw = velda_core::global_hardware_topology();
        Self::from_hardware(hw.available_cores, hw.memory.total_bytes)
    }

    /// Derives an [`Http3Config`] scaled smoothly from actual host CPU cores and RAM bytes.
    ///
    /// Calculated once during bootstrap and compiled into the runtime snapshot.
    /// Eliminates rigid discrete tier boundaries, smoothly scaling flow-control windows and concurrency
    /// using continuous mathematical formulas with strict `[min, max]` safety clamps.
    pub fn from_hardware(cores: usize, ram_bytes: usize) -> Self {
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;
        const PACKET: u64 = 1472;

        let cores = cores.max(1);

        // 1. Connection send window: 1/8192 of RAM, clamped strictly to [256 KB, 4 MB]
        let conn_send_window = ((ram_bytes / Self::SEND_WINDOW_RAM_DIVISOR) as u64)
            .clamp(Self::MIN_CONN_SEND_WINDOW, Self::MAX_CONN_SEND_WINDOW);

        // 2. Stream receive window (incoming client upload data): 1/16384 of RAM, clamped to [128 KB, 1 MB]
        let stream_receive_window = ((ram_bytes / 16384) as u32).clamp(128 * 1024, 1024 * 1024);

        // 3. Connection receive window (incoming client upload data): 1/4096 of RAM, clamped to [1 MB, 8 MB]
        let conn_receive_window = ((ram_bytes / 4096) as u64).clamp(1024 * 1024, 8 * 1024 * 1024);

        // 4. Maximum pending bytes across active streams in an engine worker: 1/64 of RAM, clamped to [16 MB, 128 MB]
        let max_pending_bytes = (ram_bytes / 64).clamp(16 * MB, 128 * MB);

        // 5. Max concurrent bidirectional streams: (cores * 64).clamp(128, 512)
        let max_concurrent_streams = ((cores * 64) as u32).clamp(128, 512);
        let max_concurrent_uni_streams = (max_concurrent_streams / 8).clamp(16, 128);

        // 6. Max body size for buffered requests: 1/256 of RAM, clamped to [2 MB, 32 MB]
        let max_body_size = (ram_bytes / 256).clamp(2 * MB, 32 * MB);

        // 7. Max header size: clamped to [16 KB, 64 KB]
        let max_header_size = (ram_bytes / 131_072).clamp(16 * KB, 64 * KB);

        // 8. Max headers count: clamped to [32, 96]
        let max_headers = (cores * 16).clamp(32, 96);

        // 9. QPACK dynamic table capacity: clamped to [4096, 32768]
        let max_qpack_table_capacity = (ram_bytes / MB).clamp(4096, 32768);

        // 10. Max pending handshakes before activating Retry tokens: clamped to [512, 16384]
        let max_pending_handshakes = ((cores * 512) as u32).clamp(512, 16384);

        // 11. Initial congestion window: clamped to [20 * 1472, 100 * 1472]
        let initial_congestion_window =
            (((ram_bytes / (160 * MB)) as u64) * PACKET).clamp(20 * PACKET, 100 * PACKET);

        Self {
            max_body_size,
            max_header_size,
            max_headers,
            idle_timeout_ms: 30_000,
            max_concurrent_streams,
            max_concurrent_uni_streams,
            max_qpack_table_capacity,
            max_pending_handshakes,
            stream_receive_window,
            conn_receive_window,
            conn_send_window,
            initial_congestion_window,
            max_pending_bytes,
        }
    }

    /// Derives an [`Http3Config`] scaled smoothly from actual host/container RAM bytes.
    #[inline]
    pub fn from_ram_bytes(ram_bytes: usize) -> Self {
        let cores = velda_core::global_hardware_topology().available_cores;
        Self::from_hardware(cores, ram_bytes)
    }

    /// Sets the maximum body size in bytes for buffered requests.
    ///
    /// *Note:* Enforced strictly for `Buffered` strategy; streaming strategies stream continuously via dynamic flow control.
    #[inline]
    pub const fn with_max_body_size(mut self, size: usize) -> Self {
        self.max_body_size = size;
        self
    }

    /// Sets the maximum header size in bytes.
    #[inline]
    pub const fn with_max_header_size(mut self, size: usize) -> Self {
        self.max_header_size = size;
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

    /// Sets the maximum concurrent streams allowed.
    #[inline]
    pub const fn with_max_concurrent_streams(mut self, max: u32) -> Self {
        self.max_concurrent_streams = max;
        self
    }

    /// Sets the maximum concurrent unidirectional streams allowed.
    #[inline]
    pub const fn with_max_concurrent_uni_streams(mut self, n: u32) -> Self {
        self.max_concurrent_uni_streams = n;
        self
    }

    /// Sets the maximum QPACK dynamic table capacity in bytes.
    #[inline]
    pub const fn with_max_qpack_table_capacity(mut self, cap: usize) -> Self {
        self.max_qpack_table_capacity = cap;
        self
    }

    /// Sets the maximum pending handshakes before activating defensive Retry tokens.
    #[inline]
    pub const fn with_max_pending_handshakes(mut self, n: u32) -> Self {
        self.max_pending_handshakes = n;
        self
    }

    /// Sets the per-stream receive flow-control window in bytes.
    #[inline]
    pub const fn with_stream_receive_window(mut self, w: u32) -> Self {
        self.stream_receive_window = w;
        self
    }

    /// Sets the connection-level receive flow-control window in bytes.
    #[inline]
    pub const fn with_conn_receive_window(mut self, w: u64) -> Self {
        self.conn_receive_window = w;
        self
    }

    /// Sets the connection-level send window in bytes.
    #[inline]
    pub const fn with_conn_send_window(mut self, w: u64) -> Self {
        self.conn_send_window = w;
        self
    }

    /// Sets the initial congestion window in bytes.
    #[inline]
    pub const fn with_initial_congestion_window(mut self, w: u64) -> Self {
        self.initial_congestion_window = w;
        self
    }

    /// Sets the maximum aggregate pending outbound response bytes across all streams in an engine worker.
    #[inline]
    pub const fn with_max_pending_bytes(mut self, bytes: usize) -> Self {
        self.max_pending_bytes = bytes;
        self
    }

    /// Fallback default per-stream receive window (1 MB).
    /// Used when hardware topology cannot determine the host memory tier.
    pub const DEFAULT_STREAM_WINDOW: u32 = 1024 * 1024; // 1 MB

    /// Fallback default connection-level receive window (16 MB).
    /// Sized to saturate high-latency BDP links without exhausting buffer pools.
    pub const DEFAULT_CONN_WINDOW: u32 = 16 * 1024 * 1024; // 16 MB

    /// Applies these HTTP/3 tuning parameters to Quinn [`quinn_proto::TransportConfig`].
    ///
    /// Flow control windows and congestion windows are deterministically scaled by host memory tier,
    /// protecting constrained VPS instances from buffer exhaustion while achieving maximum BDP
    /// and wire-speed throughput on hyperscale datacenter nodes.
    pub fn apply_to_transport(&self, transport: &mut quinn_proto::TransportConfig) {
        if let Ok(timeout) = quinn_proto::IdleTimeout::try_from(std::time::Duration::from_millis(
            self.idle_timeout_ms,
        )) {
            transport.max_idle_timeout(Some(timeout));
        }
        transport.max_concurrent_bidi_streams(quinn_proto::VarInt::from_u32(
            self.max_concurrent_streams,
        ));
        transport.max_concurrent_uni_streams(quinn_proto::VarInt::from_u32(
            self.max_concurrent_uni_streams,
        ));

        // 1. Stream-level flow control: Scaled per tier (128 KB for Constrained up to 4 MB for Ultra).
        // As data is read and consumed, Quinn automatically issues MAX_STREAM_DATA frames to replenish credit.
        transport.stream_receive_window(quinn_proto::VarInt::from_u32(self.stream_receive_window));

        // 2. Connection-level flow control: Scaled per tier (2 MB for Constrained up to 64 MB for Ultra).
        if let Ok(conn_win_varint) = quinn_proto::VarInt::try_from(self.conn_receive_window) {
            transport.receive_window(conn_win_varint);
        }

        // 3. Edge-to-Backend Low Latency & High Throughput:
        // - Initial RTT of 333ms (RFC 9002 Section 6.2.2 standard, preventing false-positive PTO loss under concurrency spikes)
        // - Send window matching connection window to prevent upload pipe starvation
        // - Cubic congestion controller with tier-scaled initial congestion window
        // - Explicit segmentation offload enablement
        transport.initial_rtt(std::time::Duration::from_millis(333));
        transport.send_window(self.conn_send_window);
        let mut cubic = quinn_proto::congestion::CubicConfig::default();
        cubic.initial_window(self.initial_congestion_window);
        transport.congestion_controller_factory(std::sync::Arc::new(cubic));
        transport.enable_segmentation_offload(true);
    }

    /// Builds a Quinn [`quinn_proto::TransportConfig`] tuned according to this configuration.
    pub fn build_transport_config(&self) -> quinn_proto::TransportConfig {
        let mut transport = quinn_proto::TransportConfig::default();
        self.apply_to_transport(&mut transport);
        transport
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_returns_valid_config() {
        let config = Http3Config::auto();
        assert!(config.max_body_size >= 2 * 1024 * 1024);
        assert!(config.max_header_size >= 16 * 1024);
        assert!(config.stream_receive_window >= 128 * 1024);
        assert!(config.conn_receive_window >= 1024 * 1024);
        assert!(config.conn_send_window >= Http3Config::MIN_CONN_SEND_WINDOW);
        assert!(config.conn_send_window <= Http3Config::MAX_CONN_SEND_WINDOW);
    }

    #[test]
    fn test_from_hardware_scaling() {
        const MB: usize = 1024 * 1024;
        const GB: usize = 1024 * MB;

        // 1 core, 256 MB (IoT / constrained)
        let c256 = Http3Config::from_hardware(1, 256 * MB);
        assert_eq!(c256.conn_send_window, 256 * 1024); // clamped min
        assert_eq!(c256.max_pending_bytes, 16 * MB); // clamped min
        assert_eq!(c256.max_concurrent_streams, 128); // clamped min

        // 4 cores, 8 GB (standard test VM)
        let c8g = Http3Config::from_hardware(4, 8 * GB);
        assert_eq!(c8g.conn_send_window, 1024 * 1024); // 8 GB / 8192 = 1 MB exactly
        assert_eq!(c8g.max_pending_bytes, 128 * MB); // 8 GB / 64 = 128 MB
        assert_eq!(c8g.stream_receive_window, 512 * 1024); // 8 GB / 16384 = 512 KB
        assert_eq!(c8g.conn_receive_window, 2 * MB as u64); // 8 GB / 4096 = 2 MB
        assert_eq!(c8g.max_concurrent_streams, 256); // 4 * 64 = 256

        // 64 cores, 128 GB (hyperscale datacenter server)
        let c128g = Http3Config::from_hardware(64, 128 * GB);
        assert_eq!(c128g.conn_send_window, 4 * 1024 * 1024); // clamped max (4 MB)
        assert_eq!(c128g.max_pending_bytes, 128 * MB); // clamped max (128 MB)
        assert_eq!(c128g.stream_receive_window, 1024 * 1024); // clamped max (1 MB)
        assert_eq!(c128g.conn_receive_window, 8 * MB as u64); // clamped max (8 MB)
        assert_eq!(c128g.max_concurrent_streams, 512); // clamped max (512)

        // Verify monotonic non-decreasing across range of memory sizes
        let ram_sizes = [
            128 * MB,
            256 * MB,
            512 * MB,
            GB,
            2 * GB,
            4 * GB,
            6 * GB,
            8 * GB,
            12 * GB,
            16 * GB,
            32 * GB,
            64 * GB,
            128 * GB,
            256 * GB,
        ];

        let configs: Vec<Http3Config> = ram_sizes
            .iter()
            .enumerate()
            .map(|(i, &r)| Http3Config::from_hardware(i + 1, r))
            .collect();

        for w in configs.windows(2) {
            assert!(w[0].conn_send_window <= w[1].conn_send_window);
            assert!(w[0].stream_receive_window <= w[1].stream_receive_window);
            assert!(w[0].conn_receive_window <= w[1].conn_receive_window);
            assert!(w[0].max_pending_bytes <= w[1].max_pending_bytes);
            assert!(w[0].max_concurrent_streams <= w[1].max_concurrent_streams);
            assert!(w[0].max_body_size <= w[1].max_body_size);
        }
    }

    #[test]
    fn test_transport_config_builder() {
        let config = Http3Config::from_ram_bytes(8 * 1024 * 1024 * 1024);
        let transport = config.build_transport_config();
        // Verifies no panics during building and applying to Quinn TransportConfig
        let _ = transport;
    }
}
