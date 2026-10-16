//! Cacheline-Aligned In-Memory Pipeline Metrics Slot.
//!
//! Provides thread-safe, lock-free atomic counters and histogram buckets
//! dedicated to a single `(DownstreamProto, UpstreamProto, PipeStrategy)` tuple.
//!
//! # Hardware Invariants
//! - `#[repr(align(64))]`: Aligned strictly to a 64-byte boundary (L1 CPU cacheline length
//!   on x86_64 AMD Zen 3/Zen 4 and Intel Skylake/Sapphire Rapids).
//! - Eliminates False Sharing: Multiple worker threads updating distinct pipeline slots
//!   will never invalidate each other's L1/L2 cache lines (MESI cache protocol isolation).
//! - Zero Locks: All fields are updated using `Ordering::Relaxed` atomics, avoiding
//!   expensive memory fences (`mfence`), bus locks, or CPU pipeline flushes.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use crate::types::{StatusClass, StreamCompletion};

/// Cacheline-aligned atomic metrics slot representing a unique pipeline combination.
#[repr(align(64))]
pub struct PipelineSlot {
    /// Total count of successful requests (2xx status codes).
    pub requests_2xx: AtomicU64,
    /// Total count of redirection requests (3xx status codes).
    pub requests_3xx: AtomicU64,
    /// Total count of client error requests (4xx status codes).
    pub requests_4xx: AtomicU64,
    /// Total count of server / upstream error requests (5xx status codes).
    pub requests_5xx: AtomicU64,
    /// Total count of non-standard / informational requests.
    pub requests_other: AtomicU64,

    /// Instantaneous number of in-flight active streams currently being served.
    pub active_streams: AtomicI64,

    /// Cumulative count of payload bytes received from downstream clients.
    pub bytes_rx: AtomicU64,
    /// Cumulative count of payload bytes transmitted to downstream clients.
    pub bytes_tx: AtomicU64,

    // Latency Distribution Buckets (Cumulative, standard Prometheus histogram convention)
    /// Requests completing in <= 1 ms.
    pub duration_le_1ms: AtomicU64,
    /// Requests completing in <= 5 ms.
    pub duration_le_5ms: AtomicU64,
    /// Requests completing in <= 10 ms.
    pub duration_le_10ms: AtomicU64,
    /// Requests completing in <= 50 ms.
    pub duration_le_50ms: AtomicU64,
    /// Requests completing in <= 250 ms.
    pub duration_le_250ms: AtomicU64,
    /// Requests completing in <= 1,000 ms.
    pub duration_le_1000ms: AtomicU64,
    /// Requests taking > 1,000 ms.
    pub duration_gt_1000ms: AtomicU64,

    // Time to First Byte (TTFB) Buckets
    /// TTFB <= 1 ms.
    pub ttfb_le_1ms: AtomicU64,
    /// TTFB <= 5 ms.
    pub ttfb_le_5ms: AtomicU64,
    /// TTFB <= 10 ms.
    pub ttfb_le_10ms: AtomicU64,
    /// TTFB <= 50 ms.
    pub ttfb_le_50ms: AtomicU64,
    /// TTFB <= 250 ms.
    pub ttfb_le_250ms: AtomicU64,
    /// TTFB > 250 ms.
    pub ttfb_gt_250ms: AtomicU64,
}

impl Default for PipelineSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineSlot {
    /// Creates a new zeroed pipeline metrics slot.
    pub const fn new() -> Self {
        Self {
            requests_2xx: AtomicU64::new(0),
            requests_3xx: AtomicU64::new(0),
            requests_4xx: AtomicU64::new(0),
            requests_5xx: AtomicU64::new(0),
            requests_other: AtomicU64::new(0),
            active_streams: AtomicI64::new(0),
            bytes_rx: AtomicU64::new(0),
            bytes_tx: AtomicU64::new(0),
            duration_le_1ms: AtomicU64::new(0),
            duration_le_5ms: AtomicU64::new(0),
            duration_le_10ms: AtomicU64::new(0),
            duration_le_50ms: AtomicU64::new(0),
            duration_le_250ms: AtomicU64::new(0),
            duration_le_1000ms: AtomicU64::new(0),
            duration_gt_1000ms: AtomicU64::new(0),
            ttfb_le_1ms: AtomicU64::new(0),
            ttfb_le_5ms: AtomicU64::new(0),
            ttfb_le_10ms: AtomicU64::new(0),
            ttfb_le_50ms: AtomicU64::new(0),
            ttfb_le_250ms: AtomicU64::new(0),
            ttfb_gt_250ms: AtomicU64::new(0),
        }
    }

    /// Increments the in-flight stream gauge when a request begins.
    #[inline(always)]
    pub fn stream_started(&self) {
        self.active_streams.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the in-flight stream gauge when a request ends.
    #[inline(always)]
    pub fn stream_ended(&self) {
        self.active_streams.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records stream completion metrics in a single lock-free atomic sequence.
    ///
    /// # Performance
    /// Executes purely with register operations and cacheline-local atomic adds.
    /// Zero heap allocations, zero system calls, zero mutex contention.
    #[inline(always)]
    pub fn record_completion(&self, completion: &StreamCompletion) {
        // 1. Status Counter
        match StatusClass::from_status_code(completion.status_code) {
            StatusClass::Success2xx => {
                self.requests_2xx.fetch_add(1, Ordering::Relaxed);
            }
            StatusClass::Redirect3xx => {
                self.requests_3xx.fetch_add(1, Ordering::Relaxed);
            }
            StatusClass::ClientError4xx => {
                self.requests_4xx.fetch_add(1, Ordering::Relaxed);
            }
            StatusClass::ServerError5xx => {
                self.requests_5xx.fetch_add(1, Ordering::Relaxed);
            }
            StatusClass::Other => {
                self.requests_other.fetch_add(1, Ordering::Relaxed);
            }
        }

        // 2. Transferred Bytes
        if completion.bytes_rx > 0 {
            self.bytes_rx
                .fetch_add(completion.bytes_rx, Ordering::Relaxed);
        }
        if completion.bytes_tx > 0 {
            self.bytes_tx
                .fetch_add(completion.bytes_tx, Ordering::Relaxed);
        }

        // 3. E2E Duration Histogram Buckets
        let dur_ms = completion.duration.as_millis();
        if dur_ms <= 1 {
            self.duration_le_1ms.fetch_add(1, Ordering::Relaxed);
        }
        if dur_ms <= 5 {
            self.duration_le_5ms.fetch_add(1, Ordering::Relaxed);
        }
        if dur_ms <= 10 {
            self.duration_le_10ms.fetch_add(1, Ordering::Relaxed);
        }
        if dur_ms <= 50 {
            self.duration_le_50ms.fetch_add(1, Ordering::Relaxed);
        }
        if dur_ms <= 250 {
            self.duration_le_250ms.fetch_add(1, Ordering::Relaxed);
        }
        if dur_ms <= 1000 {
            self.duration_le_1000ms.fetch_add(1, Ordering::Relaxed);
        } else {
            self.duration_gt_1000ms.fetch_add(1, Ordering::Relaxed);
        }

        // 4. TTFB Histogram Buckets (if captured)
        if let Some(t) = completion.ttfb {
            let ttfb_ms = t.as_millis();
            if ttfb_ms <= 1 {
                self.ttfb_le_1ms.fetch_add(1, Ordering::Relaxed);
            }
            if ttfb_ms <= 5 {
                self.ttfb_le_5ms.fetch_add(1, Ordering::Relaxed);
            }
            if ttfb_ms <= 10 {
                self.ttfb_le_10ms.fetch_add(1, Ordering::Relaxed);
            }
            if ttfb_ms <= 50 {
                self.ttfb_le_50ms.fetch_add(1, Ordering::Relaxed);
            }
            if ttfb_ms <= 250 {
                self.ttfb_le_250ms.fetch_add(1, Ordering::Relaxed);
            } else {
                self.ttfb_gt_250ms.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
