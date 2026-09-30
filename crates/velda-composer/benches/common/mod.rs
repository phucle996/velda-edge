//! Shared utilities, memory tracking allocators, and test fixtures for velda-composer benchmarks.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use velda_composer::{ApplicationProtocol, CompiledListenerComposition, Composer};
use velda_transport::{Datagram, UdpL7Handoff, UdpSocket};

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

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    #[inline]
    pub fn next_usize(&mut self, max: usize) -> usize {
        if max == 0 {
            return 0;
        }
        (self.next_u64() as usize) % max
    }
}

// ============================================================================
// Dataset Generators
// ============================================================================

/// Builds a realistic Composer pre-populated with N listener compositions.
///
/// Distribution:
/// - 40% HTTP/1.1 (cleartext and TLS)
/// - 30% HTTP/2 (cleartext h2c and TLS h2)
/// - 20% HTTP/3 (UDP QUIC + TLS)
/// - 10% gRPC (cleartext h2c and TLS h2)
pub fn build_test_composer(listener_count: usize) -> Composer {
    let mut composer = Composer::new();

    for i in 0..listener_count {
        let (proto, tls) = match i % 10 {
            0..=3 => (ApplicationProtocol::Http1, i % 2 == 1),
            4..=6 => (ApplicationProtocol::Http2, i % 2 == 1),
            7..=8 => (ApplicationProtocol::Http3, true),
            _ => (ApplicationProtocol::Grpc, i % 2 == 1),
        };

        let listener_id = format!("listener_{proto}_{i:04}");
        composer.register_listener(CompiledListenerComposition::new(
            listener_id,
            proto,
            tls,
            velda_core::IngressLimits::default(),
        ));
    }

    composer
}

/// Creates a test UDP L7 handoff envelope using an existing shared socket.
pub fn create_test_udp_handoff(
    listener_id: &str,
    socket: Arc<UdpSocket>,
    peer_port: u16,
) -> UdpL7Handoff {
    let peer = SocketAddr::from(([127, 0, 0, 1], peer_port));
    let local = SocketAddr::from(([127, 0, 0, 1], 8443));
    let datagram = Datagram::new(peer, local, vec![0x00; 64]);
    UdpL7Handoff::new(datagram, socket, listener_id)
}
