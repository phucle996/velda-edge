//! Configuration parameters for gRPC server and client pipelines.
//!
//! Scaled deterministically by host [`MemoryTier`] with all 7 tiers explicitly matched.
//! Zero-IO, zero JSON parsing on hot paths.

use velda_core::hardware::MemoryTier;

/// Tunable settings for gRPC framing, metadata limits, and stream concurrency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcConfig {
    /// Maximum length-prefixed message size in bytes.
    pub max_message_size: usize,
    /// Maximum metadata / header size in bytes.
    pub max_header_size: usize,
    /// Maximum number of metadata headers accepted.
    pub max_headers: usize,
    /// Connection idle timeout in milliseconds.
    pub idle_timeout_ms: u64,
    /// Maximum number of concurrent streams allowed per connection.
    pub max_concurrent_streams: u32,
    /// Keepalive PING interval in milliseconds for long-lived idle connections.
    pub keepalive_ping_interval_ms: u64,
    /// Timeout in milliseconds waiting for keepalive PING acknowledgement.
    pub keepalive_ping_timeout_ms: u64,
    /// Maximum duration allowed for an individual RPC call in milliseconds.
    pub max_call_duration_ms: u64,
}

impl GrpcConfig {
    /// Probes host hardware topology and derives the configuration for the active [`MemoryTier`].
    pub fn auto() -> Self {
        let hw = velda_core::global_hardware_topology();
        Self::for_tier(hw.memory_tier())
    }

    /// Derives a [`GrpcConfig`] scaled deterministically for the given [`MemoryTier`].
    pub const fn for_tier(tier: MemoryTier) -> Self {
        match tier {
            MemoryTier::Constrained => Self {
                max_message_size: 4 * 1024 * 1024,
                max_header_size: 16 * 1024,
                max_headers: 32,
                idle_timeout_ms: 15_000,
                max_concurrent_streams: 64,
                keepalive_ping_interval_ms: 30_000,
                keepalive_ping_timeout_ms: 5_000,
                max_call_duration_ms: 60_000,
            },
            MemoryTier::Small => Self {
                max_message_size: 4 * 1024 * 1024,
                max_header_size: 32 * 1024,
                max_headers: 48,
                idle_timeout_ms: 30_000,
                max_concurrent_streams: 128,
                keepalive_ping_interval_ms: 30_000,
                keepalive_ping_timeout_ms: 5_000,
                max_call_duration_ms: 60_000,
            },
            MemoryTier::Medium => Self {
                max_message_size: 8 * 1024 * 1024,
                max_header_size: 64 * 1024,
                max_headers: 64,
                idle_timeout_ms: 30_000,
                max_concurrent_streams: 256,
                keepalive_ping_interval_ms: 45_000,
                keepalive_ping_timeout_ms: 10_000,
                max_call_duration_ms: 120_000,
            },
            MemoryTier::Large => Self {
                max_message_size: 16 * 1024 * 1024,
                max_header_size: 64 * 1024,
                max_headers: 96,
                idle_timeout_ms: 45_000,
                max_concurrent_streams: 256,
                keepalive_ping_interval_ms: 60_000,
                keepalive_ping_timeout_ms: 10_000,
                max_call_duration_ms: 180_000,
            },
            MemoryTier::XLarge => Self {
                max_message_size: 32 * 1024 * 1024,
                max_header_size: 128 * 1024,
                max_headers: 128,
                idle_timeout_ms: 60_000,
                max_concurrent_streams: 512,
                keepalive_ping_interval_ms: 60_000,
                keepalive_ping_timeout_ms: 10_000,
                max_call_duration_ms: 300_000,
            },
            MemoryTier::TwoXLarge => Self {
                max_message_size: 64 * 1024 * 1024,
                max_header_size: 128 * 1024,
                max_headers: 128,
                idle_timeout_ms: 60_000,
                max_concurrent_streams: 512,
                keepalive_ping_interval_ms: 120_000,
                keepalive_ping_timeout_ms: 20_000,
                max_call_duration_ms: 300_000,
            },
            MemoryTier::Ultra => Self {
                max_message_size: 128 * 1024 * 1024,
                max_header_size: 256 * 1024,
                max_headers: 256,
                idle_timeout_ms: 120_000,
                max_concurrent_streams: 512,
                keepalive_ping_interval_ms: 120_000,
                keepalive_ping_timeout_ms: 20_000,
                max_call_duration_ms: 600_000,
            },
        }
    }

    /// Sets the maximum message size in bytes.
    #[inline]
    pub const fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
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

    /// Sets the keepalive PING interval in milliseconds.
    #[inline]
    pub const fn with_keepalive_ping_interval_ms(mut self, ms: u64) -> Self {
        self.keepalive_ping_interval_ms = ms;
        self
    }

    /// Sets the keepalive PING timeout in milliseconds.
    #[inline]
    pub const fn with_keepalive_ping_timeout_ms(mut self, ms: u64) -> Self {
        self.keepalive_ping_timeout_ms = ms;
        self
    }

    /// Sets the maximum RPC call duration in milliseconds.
    #[inline]
    pub const fn with_max_call_duration_ms(mut self, ms: u64) -> Self {
        self.max_call_duration_ms = ms;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_returns_valid_config() {
        let config = GrpcConfig::auto();
        assert!(config.max_message_size >= 4 * 1024 * 1024);
        assert!(config.max_header_size >= 16 * 1024);
    }

    #[test]
    fn test_for_tier_explicit_all_tiers() {
        let constrained = GrpcConfig::for_tier(MemoryTier::Constrained);
        assert_eq!(constrained.max_concurrent_streams, 64);
        assert_eq!(constrained.max_message_size, 4 * 1024 * 1024);

        let medium = GrpcConfig::for_tier(MemoryTier::Medium);
        assert_eq!(medium.max_concurrent_streams, 256);
        assert_eq!(medium.max_message_size, 8 * 1024 * 1024);

        let ultra = GrpcConfig::for_tier(MemoryTier::Ultra);
        assert_eq!(ultra.max_concurrent_streams, 512);
        assert_eq!(ultra.max_message_size, 128 * 1024 * 1024);
    }
}
