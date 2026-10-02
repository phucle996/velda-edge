//! HTTP/1.1 protocol engine buffer sizing, scaled by hardware topology.
//!
//! Follows the same `for_tiers(CpuTier, MemoryTier)` pattern as `velda-transport::tcp::config`
//! and `velda-tls::server::config`. Each subsystem owns its own tier-driven sizing —
//! `velda-core::hardware` only probes the host, it does not dictate subsystem parameters.

use velda_core::hardware::MemoryTier;

/// Buffer sizing parameters for the HTTP/1.1 protocol engine.
///
/// Controls per-connection memory allocation on both the downstream (ingress)
/// and upstream (egress) paths. Sized by [`MemoryTier`] to match the host's
/// available RAM — smaller tiers use tighter buffers to support high connection
/// counts, while larger tiers use bigger buffers to reduce `read()` syscall frequency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http1BufferConfig {
    /// Initial read/write buffer capacity per downstream connection (bytes).
    ///
    /// This is the starting size of `BytesMut` allocated when a new connection
    /// is accepted. Buffers grow on demand but start here to avoid over-allocation
    /// on connections that only exchange small requests.
    pub initial_buffer_capacity: usize,

    /// Capacity threshold above which idle buffers are shrunk back to `initial_buffer_capacity`.
    ///
    /// After processing a large request, the buffer retains its expanded capacity.
    /// Without compaction, 100K idle keep-alive connections each holding 64KB would
    /// waste ~6.4 GB of RAM. This threshold triggers shrinkage between request cycles
    /// when buffers are empty and exceed this size.
    pub shrink_threshold: usize,

    /// Initial read buffer capacity for upstream response reading (bytes).
    ///
    /// Allocated per outbound request in `Http1UpstreamConnector::forward_on_stream()`.
    /// Larger values reduce the number of `read()` syscalls for typical responses.
    pub upstream_read_capacity: usize,

    /// Base write buffer capacity for upstream request encoding (bytes).
    ///
    /// The actual allocation is `upstream_write_base + request_body_len` to ensure
    /// the entire encoded request fits without reallocation.
    pub upstream_write_base: usize,
}

impl Http1BufferConfig {
    /// Creates buffer parameters sized appropriately for the host's [`MemoryTier`].
    ///
    /// # Sizing Rationale
    ///
    /// | Tier | Initial | Shrink | Upstream Read | Upstream Write Base |
    /// | :--- | :--- | :--- | :--- | :--- |
    /// | Constrained | 2 KB | 8 KB | 2 KB | 512 B |
    /// | Small | 4 KB | 16 KB | 4 KB | 1 KB |
    /// | Medium | 4 KB | 16 KB | 4 KB | 1 KB |
    /// | Large | 8 KB | 32 KB | 8 KB | 2 KB |
    /// | XLarge | 8 KB | 64 KB | 8 KB | 2 KB |
    /// | TwoXLarge | 16 KB | 64 KB | 16 KB | 4 KB |
    /// | Ultra | 16 KB | 128 KB | 16 KB | 4 KB |
    pub fn for_tier(tier: MemoryTier) -> Self {
        const KB: usize = 1024;
        match tier {
            MemoryTier::Constrained => Self {
                initial_buffer_capacity: 2 * KB,
                shrink_threshold: 8 * KB,
                upstream_read_capacity: 2 * KB,
                upstream_write_base: 512,
            },
            MemoryTier::Small => Self {
                initial_buffer_capacity: 4 * KB,
                shrink_threshold: 16 * KB,
                upstream_read_capacity: 4 * KB,
                upstream_write_base: KB,
            },
            MemoryTier::Medium => Self {
                initial_buffer_capacity: 4 * KB,
                shrink_threshold: 16 * KB,
                upstream_read_capacity: 4 * KB,
                upstream_write_base: KB,
            },
            MemoryTier::Large => Self {
                initial_buffer_capacity: 8 * KB,
                shrink_threshold: 32 * KB,
                upstream_read_capacity: 8 * KB,
                upstream_write_base: 2 * KB,
            },
            MemoryTier::XLarge => Self {
                initial_buffer_capacity: 8 * KB,
                shrink_threshold: 64 * KB,
                upstream_read_capacity: 8 * KB,
                upstream_write_base: 2 * KB,
            },
            MemoryTier::TwoXLarge => Self {
                initial_buffer_capacity: 16 * KB,
                shrink_threshold: 64 * KB,
                upstream_read_capacity: 16 * KB,
                upstream_write_base: 4 * KB,
            },
            MemoryTier::Ultra => Self {
                initial_buffer_capacity: 16 * KB,
                shrink_threshold: 128 * KB,
                upstream_read_capacity: 16 * KB,
                upstream_write_base: 4 * KB,
            },
        }
    }

    /// Auto-probes the host hardware topology and returns the appropriate buffer config.
    ///
    /// Convenience wrapper over [`for_tier`](Self::for_tier) using the global
    /// [`HardwareTopology`](velda_core::HardwareTopology) cached in RAM.
    pub fn auto() -> Self {
        let hw = velda_core::global_hardware_topology();
        Self::for_tier(hw.memory_tier())
    }
}

impl Default for Http1BufferConfig {
    /// Returns Medium-tier defaults (4 KB initial, 16 KB shrink threshold).
    fn default() -> Self {
        Self::for_tier(MemoryTier::Medium)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constrained_tier_uses_smallest_buffers() {
        let cfg = Http1BufferConfig::for_tier(MemoryTier::Constrained);
        assert_eq!(cfg.initial_buffer_capacity, 2 * 1024);
        assert_eq!(cfg.shrink_threshold, 8 * 1024);
        assert_eq!(cfg.upstream_read_capacity, 2 * 1024);
        assert_eq!(cfg.upstream_write_base, 512);
    }

    #[test]
    fn test_medium_tier_matches_previous_hardcoded_defaults() {
        let cfg = Http1BufferConfig::for_tier(MemoryTier::Medium);
        // These match the original hardcoded constants (4KB initial, 16KB shrink)
        assert_eq!(cfg.initial_buffer_capacity, 4096);
        assert_eq!(cfg.shrink_threshold, 16 * 1024);
    }

    #[test]
    fn test_ultra_tier_uses_largest_buffers() {
        let cfg = Http1BufferConfig::for_tier(MemoryTier::Ultra);
        assert_eq!(cfg.initial_buffer_capacity, 16 * 1024);
        assert_eq!(cfg.shrink_threshold, 128 * 1024);
        assert_eq!(cfg.upstream_read_capacity, 16 * 1024);
        assert_eq!(cfg.upstream_write_base, 4 * 1024);
    }

    #[test]
    fn test_shrink_threshold_always_at_least_4x_initial() {
        for tier in [
            MemoryTier::Constrained,
            MemoryTier::Small,
            MemoryTier::Medium,
            MemoryTier::Large,
            MemoryTier::XLarge,
            MemoryTier::TwoXLarge,
            MemoryTier::Ultra,
        ] {
            let cfg = Http1BufferConfig::for_tier(tier);
            assert!(
                cfg.shrink_threshold >= cfg.initial_buffer_capacity * 4,
                "{tier:?}: shrink_threshold ({}) must be >= 4x initial_buffer_capacity ({})",
                cfg.shrink_threshold,
                cfg.initial_buffer_capacity
            );
        }
    }

    #[test]
    fn test_default_matches_medium() {
        assert_eq!(
            Http1BufferConfig::default(),
            Http1BufferConfig::for_tier(MemoryTier::Medium)
        );
    }

    #[test]
    fn test_auto_returns_valid_config() {
        let cfg = Http1BufferConfig::auto();
        assert!(cfg.initial_buffer_capacity >= 2 * 1024);
        assert!(cfg.shrink_threshold >= cfg.initial_buffer_capacity);
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
            let lower = Http1BufferConfig::for_tier(window[0]);
            let upper = Http1BufferConfig::for_tier(window[1]);
            assert!(
                upper.initial_buffer_capacity >= lower.initial_buffer_capacity,
                "{:?} → {:?}: initial_buffer_capacity should be non-decreasing",
                window[0],
                window[1]
            );
            assert!(
                upper.shrink_threshold >= lower.shrink_threshold,
                "{:?} → {:?}: shrink_threshold should be non-decreasing",
                window[0],
                window[1]
            );
        }
    }
}
