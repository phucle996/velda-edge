//! Rate Limiting Pre-Upstream Subsystem.
//!
//! Provides multi-algorithm traffic shaping, distributed Redis synchronization via
//! auxiliary cores, flexible key resolution strategies, and zero-allocation hot paths.

pub mod algorithm;
pub mod plugin;
pub mod redis;

pub use algorithm::{KeyState, RateLimitAlgorithm, ShardedState};
pub use plugin::{
    FallbackBehavior, KeyStrategy, RateLimitBackend, RateLimitConfig, RateLimitPlugin,
};
pub use redis::{RedisConfig, RedisConnectionPool, TlsSource};
