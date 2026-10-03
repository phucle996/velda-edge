//! Common benchmarking utilities, allocator auditing, and metrics for velda-grpc.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Custom thread-safe `GlobalAlloc` that tracks allocations and deallocations.
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

    pub fn net_bytes(&self) -> i64 {
        let allocated = self.bytes_allocated.load(Ordering::SeqCst) as i64;
        let deallocated = self.bytes_deallocated.load(Ordering::SeqCst) as i64;
        allocated - deallocated
    }

    pub fn net_allocs(&self) -> i64 {
        let allocs = self.alloc_count.load(Ordering::SeqCst) as i64;
        let deallocs = self.dealloc_count.load(Ordering::SeqCst) as i64;
        allocs - deallocs
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            self.alloc_count.fetch_add(1, Ordering::Relaxed);
            self.bytes_allocated
                .fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        self.dealloc_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_deallocated
            .fetch_add(layout.size() as u64, Ordering::Relaxed);
    }
}

pub fn format_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{nanos} ns")
    } else if nanos < 1_000_000 {
        format!("{:.2} µs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2} ms", nanos as f64 / 1_000.0)
    } else {
        format!("{:.2} s", nanos as f64 / 1_000_000_000.0)
    }
}

pub fn format_throughput(ops: u64, d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs <= 0.0 {
        return "N/A".into();
    }
    let rate = ops as f64 / secs;
    if rate >= 1_000_000.0 {
        format!("{:.2}M ops/s", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.2}K ops/s", rate / 1_000.0)
    } else {
        format!("{:.0} ops/s", rate)
    }
}

pub fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}
