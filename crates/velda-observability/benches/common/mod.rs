//! Shared Test Fixtures, Memory Tracking Allocator, and PRNG for Observability Benchmarks.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use velda_observability::types::{DownstreamProto, PipeStrategy, StreamCompletion, UpstreamProto};

// ============================================================================
// Memory Tracking Allocator
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocSnapshot {
    pub alloc_count: u64,
    pub dealloc_count: u64,
    pub bytes_allocated: u64,
    pub bytes_deallocated: u64,
}

impl AllocSnapshot {
    #[inline(always)]
    pub fn net_bytes(&self) -> i64 {
        self.bytes_allocated as i64 - self.bytes_deallocated as i64
    }

    #[inline(always)]
    pub fn net_allocs(&self) -> i64 {
        self.alloc_count as i64 - self.dealloc_count as i64
    }
}

pub struct CountingAllocator {
    alloc_count: AtomicU64,
    dealloc_count: AtomicU64,
    bytes_allocated: AtomicU64,
    bytes_deallocated: AtomicU64,
}

impl CountingAllocator {
    pub const fn new() -> Self {
        Self {
            alloc_count: AtomicU64::new(0),
            dealloc_count: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
            bytes_deallocated: AtomicU64::new(0),
        }
    }

    pub fn reset(&self) {
        self.alloc_count.store(0, Ordering::SeqCst);
        self.dealloc_count.store(0, Ordering::SeqCst);
        self.bytes_allocated.store(0, Ordering::SeqCst);
        self.bytes_deallocated.store(0, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.alloc_count.load(Ordering::SeqCst),
            self.bytes_allocated.load(Ordering::SeqCst),
        )
    }

    pub fn detailed_snapshot(&self) -> AllocSnapshot {
        AllocSnapshot {
            alloc_count: self.alloc_count.load(Ordering::SeqCst),
            dealloc_count: self.dealloc_count.load(Ordering::SeqCst),
            bytes_allocated: self.bytes_allocated.load(Ordering::SeqCst),
            bytes_deallocated: self.bytes_deallocated.load(Ordering::SeqCst),
        }
    }
}

#[allow(clippy::all)]
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.alloc_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_allocated
            .fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.dealloc_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_deallocated
            .fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

// ============================================================================
// Formatting Helpers
// ============================================================================

pub fn format_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

pub fn format_duration(d: Duration) -> String {
    let micros = d.as_micros();
    if micros < 1_000 {
        format!("{micros} µs")
    } else if micros < 1_000_000 {
        format!("{:.2} ms", micros as f64 / 1_000.0)
    } else {
        format!("{:.2} s", d.as_secs_f64())
    }
}

pub fn format_throughput(ops: u64, elapsed: Duration) -> String {
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
    if ops_per_sec >= 1_000_000.0 {
        format!("{:.2} M ops/s", ops_per_sec / 1_000_000.0)
    } else if ops_per_sec >= 1_000.0 {
        format!("{:.2} K ops/s", ops_per_sec / 1_000.0)
    } else {
        format!("{:.0} ops/s", ops_per_sec)
    }
}

// ============================================================================
// Fast Deterministic PRNG for Benchmarks (Zero External Crate Dependencies)
// ============================================================================

pub struct FastRng {
    state: u64,
}

impl FastRng {
    pub const fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0xdeadbeefcafebabe } else { seed },
        }
    }

    #[inline(always)]
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    #[inline(always)]
    pub fn next_usize(&mut self, max: usize) -> usize {
        if max == 0 {
            return 0;
        }
        (self.next_u64() % max as u64) as usize
    }

    #[inline(always)]
    pub fn next_status_code(&mut self) -> u16 {
        match self.next_usize(100) {
            0..=79 => 200,  // 80% 200 OK
            80..=89 => 304, // 10% 304 Not Modified
            90..=96 => 404, // 7% 404 Not Found
            97..=98 => 502, // 2% 502 Bad Gateway
            _ => 500,       // 1% 500 Internal Error
        }
    }

    #[inline(always)]
    pub fn next_pipeline_coords(&mut self) -> (DownstreamProto, UpstreamProto, PipeStrategy) {
        let d = match self.next_usize(3) {
            0 => DownstreamProto::H1,
            1 => DownstreamProto::H2,
            _ => DownstreamProto::H3,
        };
        let u = match self.next_usize(3) {
            0 => UpstreamProto::H1,
            1 => UpstreamProto::H2,
            _ => UpstreamProto::H3,
        };
        let p = match self.next_usize(4) {
            0 => PipeStrategy::Buffered,
            1 => PipeStrategy::ServerStream,
            2 => PipeStrategy::ClientStream,
            _ => PipeStrategy::Duplex,
        };
        (d, u, p)
    }

    #[inline(always)]
    pub fn next_completion(&mut self) -> StreamCompletion {
        let status = self.next_status_code();
        let bytes_rx = self.next_usize(4096) as u64;
        let bytes_tx = self.next_usize(65536) as u64;
        let duration_ms = (self.next_usize(50) + 1) as u64;
        let ttfb_ms = (self.next_usize(10) + 1) as u64;

        StreamCompletion::new(
            status,
            bytes_rx,
            bytes_tx,
            Duration::from_millis(duration_ms),
            Some(Duration::from_millis(ttfb_ms)),
        )
    }
}
