//! High-performance lock-free coarse clock for request hot paths.
//!
//! ### Why this exists
//! Calling `SystemTime::now()` or `Instant::now()` on Linux goes through the kernel's
//! vDSO (`clock_gettime`). While faster than a traditional syscall, it still reads hardware
//! TSC registers and runs math to compute epoch timestamps (~15–25ns per call).
//!
//! When serving 30k–100k requests/sec per core, multiple plugins (rate limit, quota, JWT,
//! HMAC, connection idle timeouts, HTTP headers) can easily query the clock 4–8 times on a
//! single request. That burns ~100–200ns of CPU just asking what time it is.
//!
//! ### How it works
//! A lightweight companion thread (`velda-coarse-clock`) runs under `SCHED_IDLE` priority
//! and updates an aligned `AtomicU64` every 100ms.
//!
//! Frontline traffic workers only perform a relaxed atomic read (`coarse_now_sec()`). This
//! compiles down to a single `mov` instruction reading directly from the core's L1 cache
//! (~0.5ns), cutting clock overhead by ~95% with zero lock contention.
//!
//! ```text
//!  Linux vDSO clock_gettime (once every 100ms)
//!             │
//!             ▼
//!     velda-coarse-clock (idle companion thread)
//!             │
//!             ▼ atomic store (relaxed)
//!     [ COARSE_SECS: AtomicU64 ] ◄── atomic load (~0.5ns, single MOV) ── frontline workers
//! ```
//!
//! ### When to use
//! - Rate limiting, quotas, and sliding windows (1s, 60s, 1h). A 100ms jitter is completely
//!   harmless here and conforms to standard RFC rate limiting.
//! - Auth expiration: JWT `exp`/`nbf`, OAuth2 token cache TTL, HMAC timestamp freshness checks.
//! - Connection pool idle eviction (30s, 60s timeouts).
//! - HTTP `Date` response header caching.
//!
//! ### When NOT to use
//! - Sub-millisecond latency measurements or fine-grained network RTT profiling.
//! - Token bucket / leaky bucket bursts that genuinely need microsecond resolution.
//! - Security nonce or cryptographic entropy generation.
//!
//! If called before initialization (e.g. in standalone unit tests), it falls back
//! transparently to standard `SystemTime::now()`.
//!

use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static COARSE_SECS: AtomicU64 = AtomicU64::new(0);
static CLOCK_INIT: Once = Once::new();

/// Initializes the background coarse clock worker (updating every 100ms on SCHED_IDLE).
pub fn init_coarse_clock() {
    CLOCK_INIT.call_once(|| {
        // Initialize with real time immediately before spawning background worker
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        COARSE_SECS.store(now, Ordering::Relaxed);

        let _ = std::thread::Builder::new()
            .name("velda-coarse-clock".to_string())
            .spawn(|| {
                crate::set_current_thread_idle_priority();
                loop {
                    let sec = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    COARSE_SECS.store(sec, Ordering::Relaxed);
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            });
    });
}

/// Returns current Unix epoch time in seconds.
///
/// If background clock thread is active, this executes in ~0.5ns (single relaxed atomic load).
/// Otherwise falls back transparently to `SystemTime::now()`.
#[inline(always)]
pub fn coarse_now_sec() -> u64 {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coarse_now_sec() {
        init_coarse_clock();
        let sec = coarse_now_sec();
        assert!(sec > 1_700_000_000);
    }
}
