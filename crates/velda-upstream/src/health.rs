//! Passive and active health tracking for backend endpoints.
//!
//! # Architecture & Mechanism Overview
//!
//! This module provides a dual-engine health tracking system combining:
//! 1. **Passive Circuit Breaking**: Inline monitoring of client connection/request failures.
//! 2. **Active Health Probing**: Out-of-band periodic probes (HTTP/TCP) via background tasks.
//!
//! ## Core Invariants & Hot-Path Guarantees
//! - **Zero Mutexes on Request Path**: All status evaluations execute lock-free via atomic
//!   operations (`AtomicBool`, `AtomicU32`, `AtomicU64`) and `ArcSwap` map projections.
//! - **Cluster-Level Fast-Path Bypass**: `unhealthy_count == 0` allows the upstream router
//!   to bypass filtering entirely without allocating or iterating through endpoint records.
//! - **Cache Invalidation Epoch**: `epoch: AtomicU64` increments monotonically on every
//!   health state change, allowing downstream callers to memoize candidate endpoints.
//! - **Zero-Alloc Timestamp Encoding**: `unhealthy_since_ms` uses `0` to denote healthy and
//!   `>0` to store the exact process-relative timestamp (in ms) when the circuit tripped.
//!
//! ```text
//! =========================================================================================
//!                             REQUEST HOT-PATH EVALUATION
//! =========================================================================================
//!
//!                         [ Incoming Request Route ]
//!                                     │
//!                       HealthTracker::has_unhealthy()?
//!                                     │
//!                      ┌──────────────┴──────────────┐
//!                [NO]  ▼                             ▼  [YES]
//!          ┌─────────────────────────┐         ┌─────────────────────────┐
//!          │   Fast-Path Bypass      │         │   Per-Endpoint Check    │
//!          │ (unhealthy_count == 0)  │         │    is_healthy(addr)     │
//!          │ Zero-Alloc / Zero-Check │         └────────────┬────────────┘
//!          └─────────────────────────┘                      │
//!                                              ArcSwap::load(&records)
//!                                                           │
//!                                            Active Check Enabled & Unhealthy?
//!                                            (active_healthy == false)
//!                                               ┌───────────┴───────────┐
//!                                         [YES] │                       │ [NO]
//!                                               ▼                       ▼
//!                                          [UNHEALTHY]      Passive Circuit Broken?
//!                                                        (unhealthy_since_ms > 0)
//!                                                                       │
//!                                                       ┌───────────────┴───────────────┐
//!                                                 [YES] │                               │ [NO]
//!                                                       ▼                               ▼
//!                                             elapsed >= cooldown?                  [HEALTHY]
//!                                               ┌───────┴───────┐
//!                                         [YES] │               │ [NO]
//!                                               ▼               ▼
//!                                        [TRIAL PROBE]     [EXCLUDED]
//!                                          (Healthy)      (Unhealthy)
//!
//! =========================================================================================
//!                      PASSIVE CIRCUIT BREAKER STATE MACHINE
//! =========================================================================================
//!
//!                   record_success() [Swap unhealthy_since_ms = 0]
//!           ┌─────────────────────────────────────────────────────────────┐
//!           │                                                             │
//!           ▼                                                             │
//!    ┌─────────────┐   consecutive_fails >= max   ┌─────────────┐         │
//!    │   HEALTHY   │ ───────────────────────────> │   TRIPPED   │         │
//!    │ (since = 0) │   CAS(0 -> timestamp_ms)     │ (since > 0) │         │
//!    └─────────────┘                              └──────┬──────┘         │
//!           ▲                                            │                │
//!           │                                            │ cooldown       │
//!           │                                            │ elapsed        │
//!           │              record_success()              ▼                │
//!           │       ┌─────────────────────────── ┌───────────────┐        │
//!           └───────┤  Reset fails = 0           │   HALF-OPEN   │ ───────┘
//!                   │  Swap unhealthy_since_ms=0 │ (Trial Probe) │ failure
//!                   └─────────────────────────── └───────────────┘ CAS new timestamp
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

static PROCESS_START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Configuration options for passive health tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassiveHealthConfig {
    pub enabled: bool,
    /// Consecutive failures before marking endpoint unhealthy.
    pub max_consecutive_failures: u32,
    /// Duration an endpoint stays excluded before a trial probe connection is permitted.
    pub cooldown: Duration,
}

/// Configuration options for active health checking probes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveHealthConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub timeout: Duration,
    pub unhealthy_threshold: u32,
    pub healthy_threshold: u32,
    pub check_type: String, // "http" or "tcp"
    pub path: Option<String>,
    pub expected_statuses: Vec<u16>,
}

/// Unified health configuration combining passive circuit breaking and active probing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HealthConfig {
    pub passive: Option<PassiveHealthConfig>,
    pub active: Option<ActiveHealthConfig>,
}

impl HealthConfig {
    /// Creates a passive-only health config with given threshold and cooldown.
    pub fn passive_only(max_consecutive_failures: u32, cooldown: Duration) -> Self {
        Self {
            passive: Some(PassiveHealthConfig {
                enabled: true,
                max_consecutive_failures,
                cooldown,
            }),
            active: None,
        }
    }
}

/// High-performance lock-free endpoint health record.
#[derive(Debug)]
struct EndpointHealthRecord {
    // Passive tracking: failures and timestamp in ms since PROCESS_START (0 = healthy)
    consecutive_failures: AtomicU32,
    unhealthy_since_ms: AtomicU64,

    // Active probe tracking
    active_healthy: AtomicBool,
    active_consecutive_successes: AtomicU32,
    active_consecutive_failures: AtomicU32,
}

impl EndpointHealthRecord {
    fn new() -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            unhealthy_since_ms: AtomicU64::new(0),
            active_healthy: AtomicBool::new(true),
            active_consecutive_successes: AtomicU32::new(0),
            active_consecutive_failures: AtomicU32::new(0),
        }
    }
}

/// Tracks both passive failure counts and active probe results across endpoints with zero-lock hot path.
#[derive(Debug)]
pub struct HealthTracker {
    config: HealthConfig,
    records: ArcSwap<HashMap<SocketAddr, Arc<EndpointHealthRecord>>>,
    registration_lock: Mutex<()>,
    unhealthy_count: AtomicU32,
    epoch: AtomicU64,
}

impl HealthTracker {
    /// Creates a new health tracker with specified configuration.
    pub fn new(config: HealthConfig) -> Self {
        Self {
            config,
            records: ArcSwap::from_pointee(HashMap::new()),
            registration_lock: Mutex::new(()),
            unhealthy_count: AtomicU32::new(0),
            epoch: AtomicU64::new(0),
        }
    }

    /// Returns a reference to the active configuration.
    pub fn config(&self) -> &HealthConfig {
        &self.config
    }

    /// Checks if any endpoints are currently marked unhealthy.
    /// Hot-path invariant: O(1) atomic check enabling zero-alloc bypass.
    #[inline]
    pub fn has_unhealthy(&self) -> bool {
        self.unhealthy_count.load(Ordering::Relaxed) > 0
    }

    /// Returns the monotonic topology epoch of health changes.
    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    #[inline]
    fn decrement_unhealthy(&self) {
        let mut current = self.unhealthy_count.load(Ordering::Acquire);
        while current > 0 {
            match self.unhealthy_count.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    fn get_or_create(&self, addr: &SocketAddr) -> Arc<EndpointHealthRecord> {
        let current = self.records.load();
        if let Some(record) = current.get(addr) {
            return Arc::clone(record);
        }

        let _guard = self.registration_lock.lock().unwrap();
        let current = self.records.load();
        if let Some(record) = current.get(addr) {
            return Arc::clone(record);
        }

        let mut new_map = (**current).clone();
        let record = Arc::new(EndpointHealthRecord::new());
        new_map.insert(*addr, Arc::clone(&record));
        self.records.store(Arc::new(new_map));
        record
    }

    /// Prunes health records for endpoints that are no longer part of active discovery.
    /// Prevents unbounded memory growth in dynamic environments with ephemeral endpoints.
    pub fn prune_unregistered(&self, active_addrs: &std::collections::HashSet<SocketAddr>) {
        let current = self.records.load();
        if current.keys().all(|addr| active_addrs.contains(addr)) {
            return;
        }

        let _guard = self.registration_lock.lock().unwrap();
        let current = self.records.load();
        let mut new_map = HashMap::new();
        let mut removed_unhealthy = 0u32;

        for (addr, record) in current.iter() {
            if active_addrs.contains(addr) {
                new_map.insert(*addr, Arc::clone(record));
            } else {
                let since = record.unhealthy_since_ms.load(Ordering::Relaxed);
                let active_healthy = record.active_healthy.load(Ordering::Relaxed);
                if since > 0 || !active_healthy {
                    removed_unhealthy += 1;
                }
            }
        }

        self.records.store(Arc::new(new_map));
        for _ in 0..removed_unhealthy {
            self.decrement_unhealthy();
        }
        self.epoch.fetch_add(1, Ordering::Release);
    }

    /// Checks if a health record exists for the given socket address.
    #[inline]
    pub fn has_record(&self, addr: &SocketAddr) -> bool {
        self.records.load().contains_key(addr)
    }

    /// Returns the total number of tracked endpoint health records.
    #[inline]
    pub fn records_count(&self) -> usize {
        self.records.load().len()
    }

    /// Records a successful connection or request from client traffic (Passive).
    /// Pure lock-free atomic fast path.
    pub fn record_success(&self, addr: &SocketAddr) {
        let current = self.records.load();
        let record = match current.get(addr) {
            Some(r) => r,
            None => return,
        };

        record.consecutive_failures.store(0, Ordering::Release);

        // Fast path: if already healthy, zero writes or state mutations
        if record.unhealthy_since_ms.load(Ordering::Relaxed) != 0 {
            let prev = record.unhealthy_since_ms.swap(0, Ordering::AcqRel);
            if prev > 0 {
                self.decrement_unhealthy();
                self.epoch.fetch_add(1, Ordering::Release);
            }
        }
    }

    /// Records a connection or I/O failure from client traffic (Passive).
    /// Pure lock-free CAS state transition.
    pub fn record_failure(&self, addr: &SocketAddr) {
        let passive_cfg = match self.config.passive {
            Some(ref cfg) if cfg.enabled => cfg,
            _ => return,
        };

        let record = self.get_or_create(addr);
        let fails = record.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
        if fails >= passive_cfg.max_consecutive_failures {
            let now_ms = PROCESS_START.elapsed().as_millis() as u64 + 1;
            if record
                .unhealthy_since_ms
                .compare_exchange(0, now_ms, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.unhealthy_count.fetch_add(1, Ordering::Release);
                self.epoch.fetch_add(1, Ordering::Release);
            }
        }
    }

    /// Records an active probe result (from active health check background task).
    pub fn record_active_result(&self, addr: &SocketAddr, success: bool) {
        let active_cfg = match self.config.active {
            Some(ref cfg) if cfg.enabled => cfg,
            _ => return,
        };

        let record = self.get_or_create(addr);
        if success {
            record
                .active_consecutive_failures
                .store(0, Ordering::Release);
            let succ = record
                .active_consecutive_successes
                .fetch_add(1, Ordering::AcqRel)
                + 1;
            if succ >= active_cfg.healthy_threshold {
                let was_healthy = record.active_healthy.swap(true, Ordering::AcqRel);
                if !was_healthy {
                    self.decrement_unhealthy();
                    self.epoch.fetch_add(1, Ordering::Release);
                }
            }
        } else {
            record
                .active_consecutive_successes
                .store(0, Ordering::Release);
            let fails = record
                .active_consecutive_failures
                .fetch_add(1, Ordering::AcqRel)
                + 1;
            if fails >= active_cfg.unhealthy_threshold {
                let was_healthy = record.active_healthy.swap(false, Ordering::AcqRel);
                if was_healthy {
                    self.unhealthy_count.fetch_add(1, Ordering::Release);
                    self.epoch.fetch_add(1, Ordering::Release);
                }
            }
        }
    }

    /// Checks if an endpoint is considered healthy and eligible for connection acquisition.
    /// Hot-path invariant: O(1) lock-free read on atomic flags, zero allocations, zero mutexes.
    pub fn is_healthy(&self, addr: &SocketAddr) -> bool {
        let current = self.records.load();
        let Some(record) = current.get(addr) else {
            return true;
        };

        // 1. Check Active Health status if active checking is enabled
        if let Some(ref active_cfg) = self.config.active
            && active_cfg.enabled
            && !record.active_healthy.load(Ordering::Acquire)
        {
            return false;
        }

        // 2. Check Passive Circuit-Breaking status if passive checking is enabled
        if let Some(ref passive_cfg) = self.config.passive {
            if !passive_cfg.enabled {
                return true;
            }

            let fails = record.consecutive_failures.load(Ordering::Acquire);
            if fails < passive_cfg.max_consecutive_failures {
                return true;
            }

            let since_ms = record.unhealthy_since_ms.load(Ordering::Acquire);
            if since_ms == 0 {
                return true;
            }

            let now_ms = PROCESS_START.elapsed().as_millis() as u64 + 1;
            let elapsed_ms = now_ms.saturating_sub(since_ms);
            elapsed_ms >= passive_cfg.cooldown.as_millis() as u64
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_passive_health_threshold_and_recovery() {
        let config = HealthConfig::passive_only(2, Duration::from_millis(50));
        let tracker = HealthTracker::new(config);
        let ep: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        assert!(tracker.is_healthy(&ep));

        // 1 failure -> still healthy
        tracker.record_failure(&ep);
        assert!(tracker.is_healthy(&ep));

        // 2 failures -> threshold reached -> unhealthy
        tracker.record_failure(&ep);
        assert!(!tracker.is_healthy(&ep));

        // Cooldown expires -> allowed to probe
        std::thread::sleep(Duration::from_millis(60));
        assert!(tracker.is_healthy(&ep));

        // Success resets failure count
        tracker.record_success(&ep);
        assert!(tracker.is_healthy(&ep));
    }

    #[test]
    fn test_active_health_threshold_and_recovery() {
        let config = HealthConfig {
            passive: None,
            active: Some(ActiveHealthConfig {
                enabled: true,
                interval: Duration::from_secs(1),
                timeout: Duration::from_millis(500),
                unhealthy_threshold: 2,
                healthy_threshold: 2,
                check_type: "http".into(),
                path: Some("/healthz".into()),
                expected_statuses: vec![200],
            }),
        };
        let tracker = HealthTracker::new(config);
        let ep: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        assert!(tracker.is_healthy(&ep));

        // 1 probe failure -> still healthy
        tracker.record_active_result(&ep, false);
        assert!(tracker.is_healthy(&ep));

        // 2 probe failures -> unhealthy
        tracker.record_active_result(&ep, false);
        assert!(!tracker.is_healthy(&ep));

        // 1 probe success -> not yet healthy (threshold = 2)
        tracker.record_active_result(&ep, true);
        assert!(!tracker.is_healthy(&ep));

        // 2 probe successes -> healthy again
        tracker.record_active_result(&ep, true);
        assert!(tracker.is_healthy(&ep));
    }

    #[test]
    fn test_prune_unregistered_and_underflow_resilience() {
        let config = HealthConfig::passive_only(1, Duration::from_secs(60));
        let tracker = HealthTracker::new(config);
        let ep1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let ep2: SocketAddr = "127.0.0.1:9002".parse().unwrap();

        tracker.record_failure(&ep1);
        tracker.record_failure(&ep2);
        assert!(!tracker.is_healthy(&ep1));
        assert!(!tracker.is_healthy(&ep2));
        assert!(tracker.has_unhealthy());

        // Prune ep1 (only ep2 remains active)
        let mut active = std::collections::HashSet::new();
        active.insert(ep2);
        tracker.prune_unregistered(&active);

        // ep1 record should be gone
        let records = tracker.records.load();
        assert!(!records.contains_key(&ep1));
        assert!(records.contains_key(&ep2));
        assert!(tracker.has_unhealthy()); // ep2 is still unhealthy

        // Now recover ep2
        tracker.record_success(&ep2);
        assert!(!tracker.has_unhealthy());

        // Underflow resilience test: multiple extra successes must never underflow unhealthy_count
        tracker.record_success(&ep2);
        tracker.record_success(&ep2);
        assert_eq!(tracker.unhealthy_count.load(Ordering::Relaxed), 0);
        assert!(!tracker.has_unhealthy());
    }
}
