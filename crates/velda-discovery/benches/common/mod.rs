//! Common benchmarking utilities for velda-discovery.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use velda_discovery::{DiscoveryError, DnsTransport, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocSnapshot {
    pub alloc_count: u64,
    pub dealloc_count: u64,
    pub bytes_allocated: u64,
    pub bytes_deallocated: u64,
}

impl AllocSnapshot {
    pub fn net_bytes(&self) -> i64 {
        self.bytes_allocated as i64 - self.bytes_deallocated as i64
    }

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

pub fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
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

/// Helper to construct a valid RFC 1035 response packet for benchmarking wire parsing.
pub fn build_mock_dns_response(
    query_id: u16,
    host: &str,
    ips: &[IpAddr],
    use_compression: bool,
) -> Vec<u8> {
    let mut resp = Vec::with_capacity(512);

    // 12-byte header: ID, Flags=0x8180 (Response, Recursion Available), QDCOUNT=1, ANCOUNT=ips.len()
    resp.extend_from_slice(&query_id.to_be_bytes());
    resp.extend_from_slice(&0x8180u16.to_be_bytes());
    resp.extend_from_slice(&1u16.to_be_bytes());
    resp.extend_from_slice(&(ips.len() as u16).to_be_bytes());
    resp.extend_from_slice(&0u16.to_be_bytes());
    resp.extend_from_slice(&0u16.to_be_bytes());

    // Question section (uncompressed)
    let q_name_offset = resp.len();
    for label in host.trim_end_matches('.').split('.') {
        resp.push(label.len() as u8);
        resp.extend_from_slice(label.as_bytes());
    }
    resp.push(0);
    resp.extend_from_slice(&1u16.to_be_bytes()); // QTYPE A
    resp.extend_from_slice(&1u16.to_be_bytes()); // QCLASS IN

    // Answer section
    for ip in ips {
        if use_compression {
            // Pointer to question name offset
            let ptr = 0xC000u16 | (q_name_offset as u16);
            resp.extend_from_slice(&ptr.to_be_bytes());
        } else {
            for label in host.trim_end_matches('.').split('.') {
                resp.push(label.len() as u8);
                resp.extend_from_slice(label.as_bytes());
            }
            resp.push(0);
        }

        match ip {
            IpAddr::V4(v4) => {
                resp.extend_from_slice(&1u16.to_be_bytes()); // TYPE A
                resp.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
                resp.extend_from_slice(&300u32.to_be_bytes()); // TTL
                resp.extend_from_slice(&4u16.to_be_bytes()); // RDLENGTH = 4
                resp.extend_from_slice(&v4.octets());
            }
            IpAddr::V6(v6) => {
                resp.extend_from_slice(&28u16.to_be_bytes()); // TYPE AAAA
                resp.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
                resp.extend_from_slice(&300u32.to_be_bytes()); // TTL
                resp.extend_from_slice(&16u16.to_be_bytes()); // RDLENGTH = 16
                resp.extend_from_slice(&v6.octets());
            }
        }
    }

    resp
}
