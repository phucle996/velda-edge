//! Connection profile definition and protocol reuse policies.
//!
//! # Strict Invariants
//! - **Explicit Configuration Only**: Timeouts and concurrency limits MUST be explicitly
//!   specified by the caller (Upstream / Protocol setup). Zero silent or guessed fallbacks.
//! - **SRP Isolation**: Profiles declare intent and reuse policy; they do NOT own sockets or pool state.

use std::fmt;
use std::time::Duration;

use crate::key::ConnectionKey;

/// Protocol reuse semantics supported by the connection pool.
///
/// Dictates how physical connections are checked out, shared, and returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReuseMode {
    /// Exclusive ownership (Raw TCP / L4 Tunnel).
    ///
    /// The connection is checked out exclusively for one session or tunnel.
    /// No other caller can borrow or share it concurrently.
    Exclusive,

    /// Sequential transaction reuse (HTTP/1.1 Keep-Alive).
    ///
    /// One request/response transaction borrows the connection exclusively.
    /// When finished, the connection is returned to the pool for the next transaction.
    Sequential,

    /// Concurrent multiplexed reuse (HTTP/2, HTTP/3).
    ///
    /// The physical connection stays inside the pool and serves up to
    /// `max_concurrent_streams` in parallel across independent callers.
    Multiplexed,
}

impl fmt::Display for ReuseMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exclusive => write!(f, "Exclusive"),
            Self::Sequential => write!(f, "Sequential"),
            Self::Multiplexed => write!(f, "Multiplexed"),
        }
    }
}

/// Profile describing the connection identity and reuse policy for a backend resource.
///
/// Upstream compiles its protocol definition (`transport`, `application`, `version`)
/// into this connection-oriented profile before requesting or registering connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionProfile {
    /// Identity key for pool sharding and grouping.
    pub key: ConnectionKey,
    /// Reuse policy (Exclusive, Sequential, or Multiplexed).
    pub reuse_mode: ReuseMode,
    /// Maximum concurrent streams allowed per connection (used by `Multiplexed`).
    pub max_concurrent_streams: u32,
    /// Maximum idle duration before a pooled connection expires (must be > 0).
    pub idle_timeout: Duration,
    /// Maximum lifetime of a physical connection before retirement.
    pub max_lifetime: Option<Duration>,
}

impl ConnectionProfile {
    /// Creates a fully explicit connection profile.
    ///
    /// # Panics
    /// Panics if `idle_timeout` is zero or `max_concurrent_streams` is zero.
    #[inline]
    pub fn new(
        key: ConnectionKey,
        reuse_mode: ReuseMode,
        idle_timeout: Duration,
        max_concurrent_streams: u32,
    ) -> Self {
        assert!(
            !idle_timeout.is_zero(),
            "ConnectionProfile idle_timeout must be > 0"
        );
        assert!(
            max_concurrent_streams > 0,
            "ConnectionProfile max_concurrent_streams must be > 0"
        );

        Self {
            key,
            reuse_mode,
            max_concurrent_streams,
            idle_timeout,
            max_lifetime: None,
        }
    }

    /// Creates an exclusive profile for Raw TCP or L4 byte forwarding with an explicit idle timeout.
    #[inline]
    pub fn exclusive(key: ConnectionKey, idle_timeout: Duration) -> Self {
        Self::new(key, ReuseMode::Exclusive, idle_timeout, 1)
    }

    /// Creates a sequential profile for HTTP/1.1 transactional keep-alive with an explicit idle timeout.
    #[inline]
    pub fn sequential(key: ConnectionKey, idle_timeout: Duration) -> Self {
        Self::new(key, ReuseMode::Sequential, idle_timeout, 1)
    }

    /// Creates a multiplexed profile for HTTP/2 or HTTP/3 stream sharing with explicit parameters.
    #[inline]
    pub fn multiplexed(
        key: ConnectionKey,
        idle_timeout: Duration,
        max_concurrent_streams: u32,
    ) -> Self {
        Self::new(
            key,
            ReuseMode::Multiplexed,
            idle_timeout,
            max_concurrent_streams,
        )
    }

    /// Builder: Sets the maximum lifetime of the physical connection before retirement.
    #[inline]
    pub fn with_max_lifetime(mut self, lifetime: Option<Duration>) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    /// Returns `true` if this profile uses multiplexed stream leasing.
    #[inline]
    pub fn is_multiplexed(&self) -> bool {
        self.reuse_mode == ReuseMode::Multiplexed
    }

    /// Returns `true` if this profile uses sequential transactional leasing.
    #[inline]
    pub fn is_sequential(&self) -> bool {
        self.reuse_mode == ReuseMode::Sequential
    }

    /// Returns `true` if this profile uses exclusive single-owner leasing.
    #[inline]
    pub fn is_exclusive(&self) -> bool {
        self.reuse_mode == ReuseMode::Exclusive
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_profile_explicit_constructors() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let key = ConnectionKey::tcp(addr);
        let timeout = Duration::from_secs(30);

        let excl = ConnectionProfile::exclusive(key.clone(), timeout);
        assert_eq!(excl.reuse_mode, ReuseMode::Exclusive);
        assert!(excl.is_exclusive());
        assert_eq!(excl.max_concurrent_streams, 1);
        assert_eq!(excl.idle_timeout, timeout);

        let seq = ConnectionProfile::sequential(key.clone(), timeout);
        assert_eq!(seq.reuse_mode, ReuseMode::Sequential);
        assert!(seq.is_sequential());
        assert_eq!(seq.max_concurrent_streams, 1);
        assert_eq!(seq.idle_timeout, timeout);

        let mux = ConnectionProfile::multiplexed(key.clone(), timeout, 100);
        assert_eq!(mux.reuse_mode, ReuseMode::Multiplexed);
        assert!(mux.is_multiplexed());
        assert_eq!(mux.max_concurrent_streams, 100);
        assert_eq!(mux.idle_timeout, timeout);
    }

    #[test]
    #[should_panic(expected = "idle_timeout must be > 0")]
    fn test_profile_rejects_zero_idle_timeout() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let key = ConnectionKey::tcp(addr);
        ConnectionProfile::new(key, ReuseMode::Sequential, Duration::ZERO, 1);
    }

    #[test]
    #[should_panic(expected = "max_concurrent_streams must be > 0")]
    fn test_profile_rejects_zero_max_streams() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let key = ConnectionKey::tcp(addr);
        ConnectionProfile::new(key, ReuseMode::Multiplexed, Duration::from_secs(30), 0);
    }

    #[test]
    fn test_reuse_mode_display() {
        assert_eq!(ReuseMode::Exclusive.to_string(), "Exclusive");
        assert_eq!(ReuseMode::Sequential.to_string(), "Sequential");
        assert_eq!(ReuseMode::Multiplexed.to_string(), "Multiplexed");
    }
}
