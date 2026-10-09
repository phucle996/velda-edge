//! HTTP/1.1 protocol engine configuration and limits, scaled by hardware topology.
//!
//! Follows the `for_tier(MemoryTier)` pattern. Each subsystem owns its own tier-driven
//! sizing and safety limits — `velda-core::hardware` only probes the host, it does not
//! dictate subsystem parameters.

use velda_core::hardware::MemoryTier;

/// Complete configuration and limits for the HTTP/1.1 protocol engine.
///
/// Controls security limits (body, header, header count, inactivity timeout)
/// and per-connection memory allocation on downstream and upstream paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http1Config {
    /// Maximum allowed request body size in bytes.
    pub max_body_size: usize,
    /// Maximum allowed size of the raw header block in bytes.
    pub max_header_size: usize,
    /// Maximum number of header fields permitted per request.
    pub max_headers: usize,
    /// Maximum idle inactivity timeout in milliseconds between incoming/outgoing data chunks.
    pub idle_timeout_ms: u64,
    /// Maximum number of requests allowed per TCP keep-alive connection before closing.
    pub max_keepalive_requests: u32,
    /// Maximum duration (in milliseconds) allowed to read request line and headers (mitigates Slowloris).
    pub header_read_timeout_ms: u64,
    /// Initial read/write buffer capacity per downstream connection (bytes).
    pub initial_buffer_capacity: usize,
    /// Capacity threshold above which idle buffers are shrunk back to `initial_buffer_capacity`.
    pub shrink_threshold: usize,
    /// Read buffer size for client upload bodies (equivalent to Nginx's `client_body_buffer_size`).
    /// Determines the capacity reserved for reading payload chunks from the downstream socket into RAM.
    pub client_body_buffer_size: usize,
    /// Initial read buffer capacity for upstream response reading (bytes).
    pub upstream_read_capacity: usize,
    /// Base write buffer capacity for upstream request encoding (bytes).
    pub upstream_write_base: usize,
}

impl Http1Config {
    /// Creates configuration sized appropriately for the host's [`MemoryTier`].
    pub const fn for_tier(tier: MemoryTier) -> Self {
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;
        match tier {
            MemoryTier::Constrained => Self {
                max_body_size: 2 * MB,
                max_header_size: 8 * KB,
                max_headers: 32,
                idle_timeout_ms: 15_000,
                max_keepalive_requests: 1_000,
                header_read_timeout_ms: 10_000,
                initial_buffer_capacity: 2 * KB,
                shrink_threshold: 8 * KB,
                client_body_buffer_size: 32 * KB,
                upstream_read_capacity: 2 * KB,
                upstream_write_base: 512,
            },
            MemoryTier::Small => Self {
                max_body_size: 10 * MB,
                max_header_size: 32 * KB,
                max_headers: 64,
                idle_timeout_ms: 30_000,
                max_keepalive_requests: 5_000,
                header_read_timeout_ms: 10_000,
                initial_buffer_capacity: 4 * KB,
                shrink_threshold: 16 * KB,
                client_body_buffer_size: 64 * KB,
                upstream_read_capacity: 4 * KB,
                upstream_write_base: KB,
            },
            MemoryTier::Medium => Self {
                max_body_size: 10 * MB,
                max_header_size: 64 * KB,
                max_headers: 64,
                idle_timeout_ms: 30_000,
                max_keepalive_requests: 10_000,
                header_read_timeout_ms: 10_000,
                initial_buffer_capacity: 4 * KB,
                shrink_threshold: 16 * KB,
                client_body_buffer_size: 128 * KB,
                upstream_read_capacity: 4 * KB,
                upstream_write_base: KB,
            },
            MemoryTier::Large => Self {
                max_body_size: 20 * MB,
                max_header_size: 64 * KB,
                max_headers: 96,
                idle_timeout_ms: 30_000,
                max_keepalive_requests: 20_000,
                header_read_timeout_ms: 15_000,
                initial_buffer_capacity: 8 * KB,
                shrink_threshold: 32 * KB,
                client_body_buffer_size: 128 * KB,
                upstream_read_capacity: 8 * KB,
                upstream_write_base: 2 * KB,
            },
            MemoryTier::XLarge => Self {
                max_body_size: 50 * MB,
                max_header_size: 64 * KB,
                max_headers: 128,
                idle_timeout_ms: 60_000,
                max_keepalive_requests: 50_000,
                header_read_timeout_ms: 15_000,
                initial_buffer_capacity: 8 * KB,
                shrink_threshold: 64 * KB,
                client_body_buffer_size: 256 * KB,
                upstream_read_capacity: 8 * KB,
                upstream_write_base: 2 * KB,
            },
            MemoryTier::TwoXLarge => Self {
                max_body_size: 100 * MB,
                max_header_size: 64 * KB,
                max_headers: 128,
                idle_timeout_ms: 60_000,
                max_keepalive_requests: 100_000,
                header_read_timeout_ms: 20_000,
                initial_buffer_capacity: 16 * KB,
                shrink_threshold: 64 * KB,
                client_body_buffer_size: 256 * KB,
                upstream_read_capacity: 16 * KB,
                upstream_write_base: 4 * KB,
            },
            MemoryTier::Ultra => Self {
                max_body_size: 100 * MB,
                max_header_size: 128 * KB,
                max_headers: 256,
                idle_timeout_ms: 60_000,
                max_keepalive_requests: 100_000,
                header_read_timeout_ms: 20_000,
                initial_buffer_capacity: 16 * KB,
                shrink_threshold: 128 * KB,
                client_body_buffer_size: 256 * KB,
                upstream_read_capacity: 16 * KB,
                upstream_write_base: 4 * KB,
            },
        }
    }

    /// Auto-probes the host hardware topology and returns the appropriate config.
    pub fn auto() -> Self {
        let hw = velda_core::global_hardware_topology();
        Self::for_tier(hw.memory_tier())
    }

    /// Sets the maximum request body size in bytes.
    #[inline]
    pub const fn with_max_body_size(mut self, size: usize) -> Self {
        self.max_body_size = size;
        self
    }

    /// Sets the maximum header block size in bytes.
    #[inline]
    pub const fn with_max_header_size(mut self, size: usize) -> Self {
        self.max_header_size = size;
        self
    }

    /// Sets the maximum number of headers permitted.
    #[inline]
    pub const fn with_max_headers(mut self, count: usize) -> Self {
        self.max_headers = count;
        self
    }

    /// Sets the idle timeout in milliseconds.
    #[inline]
    pub const fn with_idle_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.idle_timeout_ms = timeout_ms;
        self
    }

    /// Sets the maximum keep-alive requests per connection.
    #[inline]
    pub const fn with_max_keepalive_requests(mut self, n: u32) -> Self {
        self.max_keepalive_requests = n;
        self
    }

    /// Sets the maximum duration allowed to read headers (mitigating Slowloris).
    #[inline]
    pub const fn with_header_read_timeout_ms(mut self, ms: u64) -> Self {
        self.header_read_timeout_ms = ms;
        self
    }

    /// Sets the initial buffer capacity.
    #[inline]
    pub const fn with_initial_buffer_capacity(mut self, capacity: usize) -> Self {
        self.initial_buffer_capacity = capacity;
        self
    }

    /// Sets the buffer shrink threshold.
    #[inline]
    pub const fn with_shrink_threshold(mut self, threshold: usize) -> Self {
        self.shrink_threshold = threshold;
        self
    }

    /// Sets the client body buffer window capacity in bytes.
    #[inline]
    pub const fn with_client_body_buffer_size(mut self, size: usize) -> Self {
        self.client_body_buffer_size = size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constrained_tier_bounds() {
        let cfg = Http1Config::for_tier(MemoryTier::Constrained);
        assert_eq!(cfg.max_body_size, 2 * 1024 * 1024);
        assert_eq!(cfg.max_header_size, 8 * 1024);
        assert_eq!(cfg.max_headers, 32);
        assert_eq!(cfg.idle_timeout_ms, 15_000);
        assert_eq!(cfg.initial_buffer_capacity, 2 * 1024);
        assert_eq!(cfg.shrink_threshold, 8 * 1024);
        assert_eq!(cfg.client_body_buffer_size, 32 * 1024);
    }

    #[test]
    fn test_medium_tier_defaults() {
        let cfg = Http1Config::for_tier(MemoryTier::Medium);
        assert_eq!(cfg.max_body_size, 10 * 1024 * 1024);
        assert_eq!(cfg.max_header_size, 64 * 1024);
        assert_eq!(cfg.max_headers, 64);
        assert_eq!(cfg.idle_timeout_ms, 30_000);
        assert_eq!(cfg.initial_buffer_capacity, 4 * 1024);
        assert_eq!(cfg.shrink_threshold, 16 * 1024);
        assert_eq!(cfg.client_body_buffer_size, 128 * 1024);
    }

    #[test]
    fn test_ultra_tier_limits() {
        let cfg = Http1Config::for_tier(MemoryTier::Ultra);
        assert_eq!(cfg.max_body_size, 100 * 1024 * 1024);
        assert_eq!(cfg.max_header_size, 128 * 1024);
        assert_eq!(cfg.max_headers, 256);
        assert_eq!(cfg.idle_timeout_ms, 60_000);
        assert_eq!(cfg.initial_buffer_capacity, 16 * 1024);
        assert_eq!(cfg.shrink_threshold, 128 * 1024);
        assert_eq!(cfg.client_body_buffer_size, 256 * 1024);
    }

    #[test]
    fn test_tiers_monotonically_increase() {
        let tiers = [
            MemoryTier::Constrained,
            MemoryTier::Small,
            MemoryTier::Medium,
            MemoryTier::Large,
            MemoryTier::XLarge,
            MemoryTier::TwoXLarge,
            MemoryTier::Ultra,
        ];
        for window in tiers.windows(2) {
            let prev = Http1Config::for_tier(window[0]);
            let next = Http1Config::for_tier(window[1]);
            assert!(
                next.initial_buffer_capacity >= prev.initial_buffer_capacity,
                "initial buffer capacity: {:?} ({}) < {:?} ({})",
                window[1],
                next.initial_buffer_capacity,
                window[0],
                prev.initial_buffer_capacity,
            );
            assert!(
                next.max_body_size >= prev.max_body_size,
                "max body size: {:?} ({}) < {:?} ({})",
                window[1],
                next.max_body_size,
                window[0],
                prev.max_body_size,
            );
            assert!(
                next.client_body_buffer_size >= prev.client_body_buffer_size,
                "client body buffer size: {:?} ({}) < {:?} ({})",
                window[1],
                next.client_body_buffer_size,
                window[0],
                prev.client_body_buffer_size,
            );
        }
    }

    #[test]
    fn test_shrink_threshold() {
        let tiers = [
            MemoryTier::Constrained,
            MemoryTier::Small,
            MemoryTier::Medium,
            MemoryTier::Large,
            MemoryTier::XLarge,
            MemoryTier::TwoXLarge,
            MemoryTier::Ultra,
        ];
        for tier in tiers {
            let cfg = Http1Config::for_tier(tier);
            assert!(
                cfg.shrink_threshold >= cfg.initial_buffer_capacity * 4,
                "shrink_threshold must be at least 4x initial_buffer_capacity for {tier:?}"
            );
        }
    }

    #[test]
    fn test_auto_returns_valid_config() {
        let cfg = Http1Config::auto();
        assert!(cfg.initial_buffer_capacity >= 2048);
        assert!(cfg.max_body_size >= 2 * 1024 * 1024);
    }
}
