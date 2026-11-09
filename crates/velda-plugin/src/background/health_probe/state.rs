//! Lock-Free Atomic Health & Readiness State Entity.
//!
//! Tracks Data Plane lifecycle states (`is_live`, `is_ready`, `is_draining`) using
//! cacheline-isolated atomic flags (`AtomicBool`), guaranteeing $\mathcal{O}(1)$ nanosecond
//! query times with zero mutex contention across worker and shadow threads.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Shared lock-free health and readiness state of the Velda Edge Data Plane.
#[derive(Debug, Clone)]
pub struct HealthState {
    is_live: Arc<AtomicBool>,
    is_ready: Arc<AtomicBool>,
    is_draining: Arc<AtomicBool>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthState {
    /// Creates a new health state with liveness enabled and readiness pending.
    pub fn new() -> Self {
        Self {
            is_live: Arc::new(AtomicBool::new(true)),
            is_ready: Arc::new(AtomicBool::new(false)),
            is_draining: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Creates a health state with both liveness and readiness initialized to `true`.
    pub fn live_and_ready() -> Self {
        let state = Self::new();
        state.set_ready(true);
        state
    }

    /// Marks the Data Plane readiness state.
    #[inline]
    pub fn set_ready(&self, ready: bool) {
        self.is_ready.store(ready, Ordering::Release);
    }

    /// Marks the Data Plane liveness state.
    #[inline]
    pub fn set_live(&self, live: bool) {
        self.is_live.store(live, Ordering::Release);
    }

    /// Marks whether the Data Plane has entered the graceful draining phase.
    #[inline]
    pub fn set_draining(&self, draining: bool) {
        self.is_draining.store(draining, Ordering::Release);
    }

    /// Returns `true` if the process is live (event loop responsive, not deadlocked).
    #[inline]
    pub fn is_live(&self) -> bool {
        self.is_live.load(Ordering::Acquire)
    }

    /// Returns `true` if the Data Plane is ready to accept incoming user traffic.
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::Acquire)
    }

    /// Returns `true` if the process has received a shutdown signal and is actively draining connections.
    #[inline]
    pub fn is_draining(&self) -> bool {
        self.is_draining.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_state_transitions() {
        let state = HealthState::new();
        assert!(state.is_live());
        assert!(!state.is_ready());
        assert!(!state.is_draining());

        state.set_ready(true);
        assert!(state.is_ready());

        state.set_draining(true);
        assert!(state.is_draining());
    }

    #[test]
    fn test_live_and_ready_constructor() {
        let state = HealthState::live_and_ready();
        assert!(state.is_live());
        assert!(state.is_ready());
        assert!(!state.is_draining());
    }
}
