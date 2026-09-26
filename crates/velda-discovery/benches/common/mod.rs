//! Common benchmarking utilities for velda-discovery.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use velda_discovery::{DiscoveryError, DnsTransport, Result};

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

pub fn calculate_big_o(times: &[(usize, Duration)]) -> &'static str {
    if times.len() < 2 {
        return "Unknown";
    }

    let first = times[0];
    let last = times[times.len() - 1];

    let n_ratio = last.0 as f64 / first.0 as f64;
    let t_ratio = last.1.as_nanos() as f64 / first.1.as_nanos().max(1) as f64;

    if t_ratio <= 1.5 {
        "O(1)"
    } else if t_ratio <= n_ratio * 1.3 {
        "O(log N) / O(N)"
    } else {
        "O(N^2)"
    }
}

#[derive(Default)]
pub struct BenchDnsTransport {
    table: RwLock<HashMap<(SocketAddr, String), Vec<IpAddr>>>,
    query_count: AtomicUsize,
}

impl BenchDnsTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_response(&self, server: SocketAddr, host: &str, ips: Vec<IpAddr>) {
        self.table
            .write()
            .unwrap()
            .insert((server, host.to_lowercase()), ips);
    }

    pub fn query_count(&self) -> usize {
        self.query_count.load(Ordering::Relaxed)
    }

    pub fn reset_query_count(&self) {
        self.query_count.store(0, Ordering::Relaxed);
    }
}

impl DnsTransport for BenchDnsTransport {
    async fn query(&self, server: SocketAddr, host: &str) -> Result<Vec<IpAddr>> {
        self.query_count.fetch_add(1, Ordering::Relaxed);
        let key = (server, host.to_lowercase());
        if let Some(ips) = self.table.read().unwrap().get(&key) {
            Ok(ips.clone())
        } else {
            Err(DiscoveryError::DnsResolutionFailed {
                host: host.to_string(),
                reason: "NXDOMAIN".into(),
            })
        }
    }
}
