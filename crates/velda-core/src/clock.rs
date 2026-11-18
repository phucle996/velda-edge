//! Unified High-Performance Clock Subsystem for Velda Data Plane (`velda_core::clock`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                                    velda_core::clock                                    │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │ 1. COARSE CLOCK (100ms Jitter, ~0.5ns L1D Read)                                         │
//! │                                                                                         │
//! │   Linux Kernel vDSO                                                                     │
//! │          │                                                                              │
//! │          ▼ (Every 100ms on SCHED_IDLE)                                                  │
//! │   [ velda-clock Thread ] ──► Atomic Store ──► [ COARSE_SECS: AtomicU64 ]                │
//! │                                                      │                                  │
//! │                                                      ▼ Single ALU MOV (~0.5ns)          │
//! │                                              coarse_sec() -> u64                        │
//! │                                          (Cache TTL, Rate Limit, Timeout)               │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │ 2. PRECISION HARDWARE ANCHOR (Zero-Syscall, True Nanoseconds)                           │
//! │                                                                                         │
//! │   [ System Boot / OnceLock ] ──► SystemTime::now() + Instant::now() ──► EpochAnchor     │
//! │                                                                             │           │
//! │   CPU Silicon (Hardware TSC)                                                │           │
//! │          │                                                                  │           │
//! │          ▼ rdtsc (~10 CPU cycles)                                           │           │
//! │   req_start: Instant ────────► [ base_nanos + (req_start - base_instant) ] ◄┘           │
//! │                                                     │                                   │
//! │                                                     ▼ Math: ADD/SUB (~1-2ns)            │
//! │                                          instant_to_unix_nanos()                        │
//! │                                        (SpanRecord, OTel Traces, Logs)                  │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

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

/// Initializes the background clock worker (updating coarse seconds every 100ms on SCHED_IDLE).
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

/// Returns current Unix epoch time in integer seconds (~0.5ns, relaxed atomic load).
///
/// If background clock thread is active, this executes in a single L1D atomic load.
/// Otherwise falls back transparently to `SystemTime::now()`.
#[inline(always)]
pub fn coarse_sec() -> u64 {
    let sec = COARSE_SECS.load(Ordering::Relaxed);
    if sec != 0 {
        sec
    } else {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

/// Converts a hardware monotonic [`Instant`] to a true nanosecond Unix Epoch timestamp.
///
/// Executes in ~1-2ns with zero syscalls and zero VDSO calls, computing:
/// `base_unix_nanos + (instant - base_instant)`.
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

/// Returns current Unix Epoch time in true nanoseconds (~10ns, zero syscalls).
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
        assert!(delta >= 1_500_000); // At least 1.5ms
    }

    #[test]
    fn test_now_unix_nanos() {
        let n1 = now_unix_nanos();
        let n2 = now_unix_nanos();
        assert!(n2 >= n1);
    }
}
