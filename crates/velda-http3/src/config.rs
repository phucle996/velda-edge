//! Configuration parameters for HTTP/3 server and client pipelines (RFC 9114).
//!
//! Scaled deterministically by host [`MemoryTier`] with all 7 tiers explicitly matched.
//! Zero-IO, zero JSON parsing on hot paths.

use velda_core::hardware::MemoryTier;

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
}

impl Http3Config {
    /// Probes host hardware topology and derives the configuration for the active [`MemoryTier`].
    pub fn auto() -> Self {
        let hw = velda_core::global_hardware_topology();
        Self::for_tier(hw.memory_tier())
    }

    /// Derives an [`Http3Config`] scaled deterministically for the given [`MemoryTier`].
    ///
    /// Across the 7 tiers (Constrained, Small, Medium, Large, XLarge, TwoXLarge, Ultra),
    /// flow-control windows scale steadily to protect low-RAM VPS nodes from buffer bloat
    /// while unlocking line-rate multi-gigabit throughput in datacenter environments.
    pub const fn for_tier(tier: MemoryTier) -> Self {
        const KB: u32 = 1024;
        const MB: u32 = 1024 * KB;
        const MB64: u64 = 1024 * 1024;
        const PACKET: u64 = 1472;

        match tier {
            MemoryTier::Constrained => Self {
                max_body_size: 2 * 1024 * 1024,
                max_header_size: 16 * 1024,
                max_headers: 32,
                idle_timeout_ms: 15_000,
                max_concurrent_streams: 128,
                max_concurrent_uni_streams: 16,
                max_qpack_table_capacity: 4096,
                max_pending_handshakes: 512,
                stream_receive_window: 128 * KB,
                conn_receive_window: 2 * MB64,
                conn_send_window: 2 * MB64,
                initial_congestion_window: 20 * PACKET, // 29,440 B
            },
            MemoryTier::Small => Self {
                max_body_size: 4 * 1024 * 1024,
                max_header_size: 32 * 1024,
                max_headers: 48,
                idle_timeout_ms: 30_000,
                max_concurrent_streams: 256,
                max_concurrent_uni_streams: 32,
                max_qpack_table_capacity: 4096,
                max_pending_handshakes: 1_024,
                stream_receive_window: 256 * KB,
                conn_receive_window: 4 * MB64,
                conn_send_window: 4 * MB64,
                initial_congestion_window: 32 * PACKET, // 47,104 B
            },
            MemoryTier::Medium => Self {
                max_body_size: 32 * 1024 * 1024,
                max_header_size: 64 * 1024,
                max_headers: 64,
                idle_timeout_ms: 30_000,
                max_concurrent_streams: 512,
                max_concurrent_uni_streams: 64,
                max_qpack_table_capacity: 8192,
                max_pending_handshakes: 2_048,
                stream_receive_window: 512 * KB,
                conn_receive_window: 8 * MB64,
                conn_send_window: 8 * MB64,
                initial_congestion_window: 50 * PACKET, // 73,600 B
            },
            MemoryTier::Large => Self {
                max_body_size: 16 * 1024 * 1024,
                max_header_size: 64 * 1024,
                max_headers: 96,
                idle_timeout_ms: 45_000,
                max_concurrent_streams: 1_024,
                max_concurrent_uni_streams: 128,
                max_qpack_table_capacity: 16384,
                max_pending_handshakes: 4_096,
                stream_receive_window: MB,
                conn_receive_window: 16 * MB64,
                conn_send_window: 16 * MB64,
                initial_congestion_window: 80 * PACKET, // 117,760 B
            },
            MemoryTier::XLarge => Self {
                max_body_size: 32 * 1024 * 1024,
                max_header_size: 128 * 1024,
                max_headers: 128,
                idle_timeout_ms: 60_000,
                max_concurrent_streams: 2_048,
                max_concurrent_uni_streams: 256,
                max_qpack_table_capacity: 32768,
                max_pending_handshakes: 8_192,
                stream_receive_window: 1536 * KB, // 1.5 MB
                conn_receive_window: 32 * MB64,
                conn_send_window: 32 * MB64,
                initial_congestion_window: 100 * PACKET, // 147,200 B
            },
            MemoryTier::TwoXLarge => Self {
                max_body_size: 64 * 1024 * 1024,
                max_header_size: 128 * 1024,
                max_headers: 128,
                idle_timeout_ms: 60_000,
                max_concurrent_streams: 4_096,
                max_concurrent_uni_streams: 256,
                max_qpack_table_capacity: 65536,
                max_pending_handshakes: 16_384,
                stream_receive_window: 2 * MB,
                conn_receive_window: 48 * MB64,
                conn_send_window: 48 * MB64,
                initial_congestion_window: 150 * PACKET, // 220,800 B
            },
            MemoryTier::Ultra => Self {
                max_body_size: 128 * 1024 * 1024,
                max_header_size: 256 * 1024,
                max_headers: 256,
                idle_timeout_ms: 120_000,
                max_concurrent_streams: 8_192,
                max_concurrent_uni_streams: 512,
                max_qpack_table_capacity: 65536,
                max_pending_handshakes: 32_768,
                stream_receive_window: 4 * MB,
                conn_receive_window: 64 * MB64,
                conn_send_window: 64 * MB64,
                initial_congestion_window: 200 * PACKET, // 294,400 B
            },
        }
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

    /// Fallback default per-stream receive window (1 MB).
    pub const DEFAULT_STREAM_WINDOW: u32 = 1024 * 1024; // 1 MB

    /// Fallback default connection-level receive window (16 MB).
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
        // - Initial RTT of 10ms (low latency while preventing false-positive PTO loss under concurrency spikes)
        // - Send window matching connection window to prevent upload pipe starvation
        // - Cubic congestion controller with tier-scaled initial congestion window
        // - Explicit segmentation offload enablement
        transport.initial_rtt(std::time::Duration::from_millis(10));
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
        assert!(config.conn_receive_window >= 2 * 1024 * 1024);
    }

    #[test]
    fn test_for_tier_explicit_all_tiers() {
        let constrained = Http3Config::for_tier(MemoryTier::Constrained);
        assert_eq!(constrained.max_concurrent_streams, 128);
        assert_eq!(constrained.max_body_size, 2 * 1024 * 1024);
        assert_eq!(constrained.max_pending_handshakes, 512);
        assert_eq!(constrained.stream_receive_window, 128 * 1024);
        assert_eq!(constrained.conn_receive_window, 2 * 1024 * 1024);
        assert_eq!(constrained.conn_send_window, 2 * 1024 * 1024);

        let medium = Http3Config::for_tier(MemoryTier::Medium);
        assert_eq!(medium.max_concurrent_streams, 512);
        assert_eq!(medium.max_body_size, 32 * 1024 * 1024);
        assert_eq!(medium.max_pending_handshakes, 2_048);
        assert_eq!(medium.stream_receive_window, 512 * 1024);
        assert_eq!(medium.conn_receive_window, 8 * 1024 * 1024);

        let ultra = Http3Config::for_tier(MemoryTier::Ultra);
        assert_eq!(ultra.max_concurrent_streams, 8_192);
        assert_eq!(ultra.max_body_size, 128 * 1024 * 1024);
        assert_eq!(ultra.max_pending_handshakes, 32_768);
        assert_eq!(ultra.stream_receive_window, 4 * 1024 * 1024);
        assert_eq!(ultra.conn_receive_window, 64 * 1024 * 1024);
    }

    #[test]
    fn test_tiered_flow_control_windows_strictly_increasing() {
        let tiers = [
            MemoryTier::Constrained,
            MemoryTier::Small,
            MemoryTier::Medium,
            MemoryTier::Large,
            MemoryTier::XLarge,
            MemoryTier::TwoXLarge,
            MemoryTier::Ultra,
        ];

        let configs: Vec<Http3Config> = tiers.iter().map(|&t| Http3Config::for_tier(t)).collect();

        // All 7 tiers must strictly increase:
        // Stream: 128KB -> 256KB -> 512KB -> 1MB -> 1.5MB -> 2MB -> 4MB
        // Conn:   2MB   -> 4MB   -> 8MB   -> 16MB-> 32MB  -> 48MB -> 64MB
        for window in configs.windows(2) {
            assert!(
                window[0].stream_receive_window < window[1].stream_receive_window,
                "stream_receive_window must strictly increase across tiers"
            );
            assert!(
                window[0].conn_receive_window < window[1].conn_receive_window,
                "conn_receive_window must strictly increase across tiers"
            );
            assert!(
                window[0].conn_send_window < window[1].conn_send_window,
                "conn_send_window must strictly increase across tiers"
            );
            assert!(
                window[0].initial_congestion_window < window[1].initial_congestion_window,
                "initial_congestion_window must strictly increase across tiers"
            );
            assert!(
                window[0].max_concurrent_streams < window[1].max_concurrent_streams,
                "max_concurrent_streams must strictly increase across tiers"
            );
        }

        assert_eq!(configs[0].stream_receive_window, 128 * 1024); // 128 KB
        assert_eq!(configs[1].stream_receive_window, 256 * 1024); // 256 KB
        assert_eq!(configs[2].stream_receive_window, 512 * 1024); // 512 KB
        assert_eq!(configs[3].stream_receive_window, 1024 * 1024); // 1 MB
        assert_eq!(configs[4].stream_receive_window, 1536 * 1024); // 1.5 MB
        assert_eq!(configs[5].stream_receive_window, 2 * 1024 * 1024); // 2 MB
        assert_eq!(configs[6].stream_receive_window, 4 * 1024 * 1024); // 4 MB

        assert_eq!(configs[0].conn_receive_window, 2 * 1024 * 1024); // 2 MB
        assert_eq!(configs[1].conn_receive_window, 4 * 1024 * 1024); // 4 MB
        assert_eq!(configs[2].conn_receive_window, 8 * 1024 * 1024); // 8 MB
        assert_eq!(configs[3].conn_receive_window, 16 * 1024 * 1024); // 16 MB
        assert_eq!(configs[4].conn_receive_window, 32 * 1024 * 1024); // 32 MB
        assert_eq!(configs[5].conn_receive_window, 48 * 1024 * 1024); // 48 MB
        assert_eq!(configs[6].conn_receive_window, 64 * 1024 * 1024); // 64 MB
    }

    #[test]
    fn test_transport_config_builder() {
        let config = Http3Config::for_tier(MemoryTier::Medium);
        let transport = config.build_transport_config();
        // Verifies no panics during building and applying to Quinn TransportConfig
        let _ = transport;
    }
}
