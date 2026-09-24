//! Common benchmarking utilities and counting allocator for velda-transport.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub struct CountingAllocator {
    alloc_count: AtomicU64,
    bytes_allocated: AtomicU64,
}

impl CountingAllocator {
    pub const fn new() -> Self {
        Self {
            alloc_count: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
        }
    }

    pub fn reset(&self) {
        self.alloc_count.store(0, Ordering::SeqCst);
        self.bytes_allocated.store(0, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.alloc_count.load(Ordering::SeqCst),
            self.bytes_allocated.load(Ordering::SeqCst),
        )
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
        unsafe { System.dealloc(ptr, layout) }
    }
}

pub fn format_duration(dur: Duration) -> String {
    let nanos = dur.as_nanos();
    if nanos < 1_000 {
        format!("{nanos} ns")
    } else if nanos < 1_000_000 {
        format!("{:.2} µs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2} ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", dur.as_secs_f64())
    }
}

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

pub fn format_throughput(bytes: usize, dur: Duration) -> String {
    let secs = dur.as_secs_f64();
    if secs <= 0.0 {
        return "N/A".to_string();
    }
    let bps = bytes as f64 / secs;
    if bps < 1024.0 {
        format!("{bps:.1} B/s")
    } else if bps < 1024.0 * 1024.0 {
        format!("{:.1} KB/s", bps / 1024.0)
    } else if bps < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2} MB/s", bps / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB/s", bps / (1024.0 * 1024.0 * 1024.0))
    }
}

pub fn calculate_big_o(results: &[(usize, Duration)]) -> (String, f64) {
    if results.len() < 2 {
        return ("O(1)".to_string(), 0.0);
    }

    let (n1, t1) = (results[0].0 as f64, results[0].1.as_nanos() as f64);
    let (n2, t2) = (
        results[results.len() - 1].0 as f64,
        results[results.len() - 1].1.as_nanos() as f64,
    );

    if n1 <= 0.0 || n2 <= n1 || t1 <= 0.0 || t2 <= 0.0 {
        return ("O(1)".to_string(), 0.0);
    }

    let alpha = (t2 / t1).ln() / (n2 / n1).ln();

    let notation = if alpha < 0.25 {
        "O(1)"
    } else if alpha < 0.75 {
        "O(log N)"
    } else if alpha < 1.25 {
        "O(N)"
    } else if alpha < 1.75 {
        "O(N log N)"
    } else {
        "O(N²)"
    };

    (notation.to_string(), alpha)
}
