//! Pool configuration parameters and operational metrics accounting.

use std::time::Duration;

use crate::container::probed_max_idle_per_key;

/// Default idle timeout before an idle connection is considered expired (30 seconds).
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default maximum connection lifetime before forced retirement (1 hour).
pub const DEFAULT_MAX_LIFETIME: Duration = Duration::from_secs(3600);

/// Configuration defining capacity, idle timeouts, and maximum connection lifetimes.
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    /// Maximum number of idle connections retained per key container (default: dynamically probed).
    /// Excess connections returned to the pool are closed immediately to prevent FD exhaustion.
    pub max_idle_per_key: usize,
    /// Default idle timeout before an idle connection is considered expired (default: 30s).
    pub idle_timeout: Duration,
    /// Maximum connection lifetime from creation before forced retirement (default: 1 hour).
    /// Prevents connection reset (RST) races when backend upstream servers enforce keepalive limits.
    pub max_lifetime: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_key: probed_max_idle_per_key(),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_lifetime: Some(DEFAULT_MAX_LIFETIME),
        }
    }
}

/// Real-time metrics and accounting of pool operations.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    /// Number of successful pool hits (reused idle connection).
    pub hits: u64,
    /// Number of pool misses (required creating new connection).
    pub misses: u64,
    /// Total connections successfully returned and accepted into the pool.
    pub releases: u64,
    /// Total connections closed during idle expiration sweeps.
    pub evictions: u64,
}

impl PoolStats {
    /// Returns the cache hit ratio as a floating-point value in `[0.0, 1.0]`.
    #[inline]
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}
