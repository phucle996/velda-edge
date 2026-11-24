//! Dual-Tier Clock Subsystem for Velda Data Plane (`velda_core::clock`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                                    velda_core::clock                                    │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │ 1. HARDWARE CLOCK (True Nanoseconds, Monotonic Anchor)                                  │
//! │    now_unix_nanos() -> u64                                                              │
//! │    instant_to_unix_nanos(Instant) -> u64                                                │
//! │    Scope: Request Lifecycle (Arrival time, duration_ns, RequestId, Logs, Tracing)       │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │ 2. COARSE CLOCK (Unix Epoch Seconds, Relaxed Atomic Load)                               │
//! │    coarse_sec() -> u64                                                                  │
//! │    Scope: Logistics & Background (Cache TTL, Session Expiry, Pool Timeout, Rate Limit)  │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Architectural Invariant
//! - Strict domain separation: Hardware Clock owns request lifecycle; Coarse Clock owns infrastructure logistics.
//! - Coarse seconds are never scaled or multiplied to represent nanoseconds.

use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static COARSE_SECS: AtomicU64 = AtomicU64::new(0);
static CLOCK_INIT: Once = Once::new();

/// Hardware Time Stamp Counter (TSC) Anchor.
///
/// Anchors a monotonic hardware `Instant` to an absolute Unix Epoch nanosecond timestamp.
struct EpochAnchor {
    base_unix_nanos: u64,
    base_instant: Instant,
}

static ANCHOR: std::sync::OnceLock<EpochAnchor> = std::sync::OnceLock::new();

/// Initializes the background clock worker (updating coarse seconds periodically on SCHED_IDLE).
pub fn init_clock() {
    CLOCK_INIT.call_once(|| {
        let sys_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let inst_now = Instant::now();
        let _ = ANCHOR.set(EpochAnchor {
            base_unix_nanos: sys_now,
            base_instant: inst_now,
        });

        COARSE_SECS.store(sys_now / 1_000_000_000, Ordering::Relaxed);

        let _ = std::thread::Builder::new()
            .name("velda-clock".to_string())
            .spawn(|| {
                crate::hardware::set_current_thread_idle_priority();
                loop {
                    let sec = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    COARSE_SECS.store(sec, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(100));
                }
            });
    });
}

/// Returns current Unix epoch time in integer seconds via relaxed atomic load.
///
/// Lazily initializes the background clock worker on first cold call if [`init_clock()`]
/// was not yet invoked (e.g. inside standalone unit/integration tests).
#[inline(always)]
pub fn coarse_sec() -> u64 {
    let sec = COARSE_SECS.load(Ordering::Relaxed);
    if sec != 0 {
        sec
    } else {
        init_and_load_coarse_sec()
    }
}

#[cold]
#[inline(never)]
fn init_and_load_coarse_sec() -> u64 {
    init_clock();
    COARSE_SECS.load(Ordering::Relaxed)
}

/// Converts a hardware monotonic [`Instant`] to an absolute Unix Epoch nanosecond timestamp.
#[inline(always)]
pub fn instant_to_unix_nanos(instant: Instant) -> u64 {
    let anchor = ANCHOR.get_or_init(|| {
        let sys_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        EpochAnchor {
            base_unix_nanos: sys_now,
            base_instant: instant,
        }
    });

    if instant >= anchor.base_instant {
        let delta = (instant - anchor.base_instant).as_nanos() as u64;
        anchor.base_unix_nanos.saturating_add(delta)
    } else {
        let delta = (anchor.base_instant - instant).as_nanos() as u64;
        anchor.base_unix_nanos.saturating_sub(delta)
    }
}

/// Returns current Unix Epoch time in nanoseconds derived from hardware monotonic clock.
#[inline(always)]
pub fn now_unix_nanos() -> u64 {
    instant_to_unix_nanos(Instant::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coarse_sec() {
        init_clock();
        let sec = coarse_sec();
        assert!(sec > 1_700_000_000);
    }

    #[test]
    fn test_precision_nanos() {
        let t1 = Instant::now();
        let nano1 = instant_to_unix_nanos(t1);
        assert!(nano1 > 1_700_000_000_000_000_000);

        std::thread::sleep(Duration::from_millis(2));

        let t2 = Instant::now();
        let nano2 = instant_to_unix_nanos(t2);
        assert!(nano2 > nano1);

        let delta = nano2 - nano1;
        assert!(delta >= 1_500_000);
    }

    #[test]
    fn test_now_unix_nanos() {
        let n1 = now_unix_nanos();
        let n2 = now_unix_nanos();
        assert!(n2 >= n1);
    }
}
