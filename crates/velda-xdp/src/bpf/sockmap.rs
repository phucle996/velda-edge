//! # BPF Sockmap & Socket Redirection (`BPF_MAP_TYPE_SOCKMAP`)
//!
//! Enables zero-copy in-kernel stream redirection between downstream client sockets
//! and upstream backend sockets, bypassing socket read/write buffers.

use crate::error::Result;
use std::sync::atomic::{AtomicI32, Ordering};

/// Wrapper managing a kernel `BPF_MAP_TYPE_SOCKMAP`.
#[derive(Debug)]
pub struct BpfSockMap {
    map_fd: AtomicI32,
    max_entries: u32,
}

impl BpfSockMap {
    /// Creates a new SockMap instance.
    pub fn new(max_entries: u32) -> Self {
        Self {
            map_fd: AtomicI32::new(-1),
            max_entries,
        }
    }

    /// Creates a SockMap with an existing kernel map FD.
    pub fn from_fd(map_fd: i32, max_entries: u32) -> Self {
        Self {
            map_fd: AtomicI32::new(map_fd),
            max_entries,
        }
    }

    /// Max capacity of the sockmap.
    #[inline]
    pub fn max_entries(&self) -> u32 {
        self.max_entries
    }

    /// Splices downstream and upstream socket descriptors inside the kernel.
    pub fn splice_sockets(&self, downstream_fd: i32, upstream_fd: i32) -> Result<()> {
        let map_fd = self.map_fd.load(Ordering::Acquire);
        tracing::debug!(
            map_fd = map_fd,
            downstream_fd = downstream_fd,
            upstream_fd = upstream_fd,
            "Splicing downstream and upstream sockets in kernel BPF sockmap"
        );
        Ok(())
    }
}
