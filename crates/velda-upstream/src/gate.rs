//! Adaptive Concurrency Gate for upstream connection regulation.
//!
//! Implements a gradient-based Vegas/CoDel concurrency controller:
//! - Smoothly expands permits during low-latency operations up to `max_limit`.
//! - Automatically constricts permits down to `min_limit` when queueing delay is detected.
//! - Uses lock-free atomics and amortizes limit evaluation every 64 samples to eliminate
//!   synchronization overhead from the request hot path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Lock-free Adaptive Concurrency Gate regulating upstream connection saturation.
#[derive(Debug)]
pub struct AdaptiveConcurrencyGate {
    semaphore: Arc<Semaphore>,
    min_limit: usize,
    max_limit: usize,
    current_limit: AtomicUsize,
    sample_count: AtomicU64,
    base_rtt_us: AtomicU64,
    ewma_rtt_us: AtomicU64,
}

impl AdaptiveConcurrencyGate {
    /// Creates a new [`AdaptiveConcurrencyGate`] with initial, minimum, and maximum permit thresholds.
    pub fn new(initial: usize, min_limit: usize, max_limit: usize) -> Self {
        let min_limit = min_limit.max(16);
        let max_limit = max_limit.max(min_limit);
        let initial = initial.clamp(min_limit, max_limit);

        Self {
            semaphore: Arc::new(Semaphore::new(initial)),
            min_limit,
            max_limit,
            current_limit: AtomicUsize::new(initial),
            sample_count: AtomicU64::new(0),
            base_rtt_us: AtomicU64::new(u64::MAX),
            ewma_rtt_us: AtomicU64::new(0),
        }
    }

    /// Asynchronously acquires an owned permit from the gate.
    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        Arc::clone(&self.semaphore).acquire_owned().await
    }

    /// Records an upstream round-trip / service time sample and updates dynamic limit heuristics.
    pub fn record_sample(&self, rtt: Duration) {
        let rtt_us = rtt.as_micros().max(1) as u64;

        // 1. Track minimum baseline RTT using lock-free atomic min
        self.base_rtt_us.fetch_min(rtt_us, Ordering::Relaxed);

        // 2. Exponentially Weighted Moving Average (EWMA): alpha = 1/8
        let old_ewma = self.ewma_rtt_us.load(Ordering::Relaxed);
        let new_ewma = if old_ewma == 0 {
            rtt_us
        } else {
            (old_ewma * 7 + rtt_us) / 8
        };
        self.ewma_rtt_us.store(new_ewma, Ordering::Relaxed);

        // 3. Amortized limit adaptation: evaluate every 64 completed requests
        let count = self.sample_count.fetch_add(1, Ordering::Relaxed);
        if count & 0x3F == 0 {
            self.adjust_limit();
        }
    }

    /// Evaluates gradient and smoothly adjusts permit allocation.
    fn adjust_limit(&self) {
        let base = self.base_rtt_us.load(Ordering::Relaxed);
        let ewma = self.ewma_rtt_us.load(Ordering::Relaxed);

        if base == 0 || base == u64::MAX || ewma == 0 {
            return;
        }

        let curr = self.current_limit.load(Ordering::Relaxed);

        // Healthy backend: EWMA is within 15% of baseline -> Additive Increase (+2 permits)
        if ewma <= base + (base / 7) {
            if curr < self.max_limit {
                let step = 2.min(self.max_limit - curr);
                self.current_limit.store(curr + step, Ordering::Relaxed);
                self.semaphore.add_permits(step);
            }
        }
        // Queueing detected: EWMA backed up > 30% above baseline -> Multiplicative Decrease (-10%)
        else if ewma > base + (base / 3) {
            let target = (curr * 9 / 10).max(self.min_limit);
            if target < curr {
                let delta = curr - target;
                self.current_limit.store(target, Ordering::Relaxed);
                if let Ok(permit) = self.semaphore.try_acquire_many(delta as u32) {
                    permit.forget();
                }
            }
        }
    }

    /// Returns the current dynamic concurrency limit.
    #[inline]
    pub fn current_limit(&self) -> usize {
        self.current_limit.load(Ordering::Relaxed)
    }

    /// Returns the number of currently available permits in the semaphore.
    #[inline]
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Returns the baseline RTT in microseconds.
    #[inline]
    pub fn base_rtt_us(&self) -> u64 {
        self.base_rtt_us.load(Ordering::Relaxed)
    }

    /// Returns the EWMA RTT in microseconds.
    #[inline]
    pub fn ewma_rtt_us(&self) -> u64 {
        self.ewma_rtt_us.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_gate_initialization_and_acquire() {
        let gate = AdaptiveConcurrencyGate::new(64, 32, 128);
        assert_eq!(gate.current_limit(), 64);
        assert_eq!(gate.available_permits(), 64);

        let permit = gate.acquire().await.unwrap();
        assert_eq!(gate.available_permits(), 63);
        drop(permit);
        assert_eq!(gate.available_permits(), 64);
    }

    #[test]
    fn test_gate_additive_increase_under_stable_rtt() {
        let gate = AdaptiveConcurrencyGate::new(32, 16, 64);
        // Base RTT = 1000us (1ms)
        for _ in 0..100 {
            gate.record_sample(Duration::from_micros(1000));
        }
        // Should have increased limits
        assert!(gate.current_limit() > 32);
        assert_eq!(gate.base_rtt_us(), 1000);
    }

    #[test]
    fn test_gate_multiplicative_decrease_under_queueing() {
        let gate = AdaptiveConcurrencyGate::new(32, 16, 64);
        // Establish baseline of 1000us
        for _ in 0..64 {
            gate.record_sample(Duration::from_micros(1000));
        }
        let baseline_limit = gate.current_limit();

        // Sudden 5x latency spike (5000us) -> queueing
        for _ in 0..64 {
            gate.record_sample(Duration::from_micros(5000));
        }
        assert!(gate.current_limit() < baseline_limit);
    }
}
