//! Configuration parameters for HTTP/3 server and client pipelines (RFC 9114).
//!
//! Scaled deterministically by host [`MemoryTier`] with all 7 tiers explicitly matched.
//! Zero-IO, zero JSON parsing on hot paths.

use velda_core::hardware::MemoryTier;

/// Tunable settings for HTTP/3 framing, QPACK metadata limits, and stream concurrency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http3Config {
    /// Maximum request/response body size in bytes.
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
}

impl Http3Config {
    /// Probes host hardware topology and derives the configuration for the active [`MemoryTier`].
    pub fn auto() -> Self {
        let hw = velda_core::global_hardware_topology();
        Self::for_tier(hw.memory_tier())
    }

    /// Derives an [`Http3Config`] scaled deterministically for the given [`MemoryTier`].
    pub const fn for_tier(tier: MemoryTier) -> Self {
        match tier {
            MemoryTier::Constrained => Self {
                max_body_size: 2 * 1024 * 1024,
                max_header_size: 16 * 1024,
                max_headers: 32,
                idle_timeout_ms: 15_000,
                max_concurrent_streams: 64,
                max_concurrent_uni_streams: 16,
                max_qpack_table_capacity: 4096,
                max_pending_handshakes: 256,
            },
            MemoryTier::Small => Self {
                max_body_size: 4 * 1024 * 1024,
                max_header_size: 32 * 1024,
                max_headers: 48,
                idle_timeout_ms: 30_000,
                max_concurrent_streams: 128,
                max_concurrent_uni_streams: 32,
                max_qpack_table_capacity: 4096,
                max_pending_handshakes: 512,
            },
            MemoryTier::Medium => Self {
                max_body_size: 10 * 1024 * 1024,
                max_header_size: 64 * 1024,
                max_headers: 64,
                idle_timeout_ms: 30_000,
                max_concurrent_streams: 256,
                max_concurrent_uni_streams: 64,
                max_qpack_table_capacity: 8192,
                max_pending_handshakes: 1_024,
            },
            MemoryTier::Large => Self {
                max_body_size: 16 * 1024 * 1024,
                max_header_size: 64 * 1024,
                max_headers: 96,
                idle_timeout_ms: 45_000,
                max_concurrent_streams: 256,
                max_concurrent_uni_streams: 64,
                max_qpack_table_capacity: 16384,
                max_pending_handshakes: 2_048,
            },
            MemoryTier::XLarge => Self {
                max_body_size: 32 * 1024 * 1024,
                max_header_size: 128 * 1024,
                max_headers: 128,
                idle_timeout_ms: 60_000,
                max_concurrent_streams: 512,
                max_concurrent_uni_streams: 128,
                max_qpack_table_capacity: 32768,
                max_pending_handshakes: 4_096,
            },
            MemoryTier::TwoXLarge => Self {
                max_body_size: 64 * 1024 * 1024,
                max_header_size: 128 * 1024,
                max_headers: 128,
                idle_timeout_ms: 60_000,
                max_concurrent_streams: 512,
                max_concurrent_uni_streams: 128,
                max_qpack_table_capacity: 65536,
                max_pending_handshakes: 8_192,
            },
            MemoryTier::Ultra => Self {
                max_body_size: 128 * 1024 * 1024,
                max_header_size: 256 * 1024,
                max_headers: 256,
                idle_timeout_ms: 120_000,
                max_concurrent_streams: 512,
                max_concurrent_uni_streams: 256,
                max_qpack_table_capacity: 65536,
                max_pending_handshakes: 16_384,
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

    /// Applies these HTTP/3 tuning parameters to Quinn [`quinn_proto::TransportConfig`].
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
        let stream_window = (self.max_body_size as u32).clamp(65536, 16 * 1024 * 1024);
        transport.stream_receive_window(quinn_proto::VarInt::from_u32(stream_window));
        let conn_window = (stream_window as u64 * 8).clamp(512 * 1024, 64 * 1024 * 1024);
        transport.receive_window(
            quinn_proto::VarInt::from_u64(conn_window)
                .unwrap_or(quinn_proto::VarInt::from_u32(1024 * 1024)),
        );
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
    }

    #[test]
    fn test_for_tier_explicit_all_tiers() {
        let constrained = Http3Config::for_tier(MemoryTier::Constrained);
        assert_eq!(constrained.max_concurrent_streams, 64);
        assert_eq!(constrained.max_body_size, 2 * 1024 * 1024);
        assert_eq!(constrained.max_pending_handshakes, 256);

        let medium = Http3Config::for_tier(MemoryTier::Medium);
        assert_eq!(medium.max_concurrent_streams, 256);
        assert_eq!(medium.max_body_size, 10 * 1024 * 1024);
        assert_eq!(medium.max_pending_handshakes, 1_024);

        let ultra = Http3Config::for_tier(MemoryTier::Ultra);
        assert_eq!(ultra.max_concurrent_streams, 512);
        assert_eq!(ultra.max_body_size, 128 * 1024 * 1024);
        assert_eq!(ultra.max_pending_handshakes, 16_384);
    }
}
