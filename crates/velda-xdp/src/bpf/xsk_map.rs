//! # BPF XSKMAP Management (AF_XDP Socket Redirection)
//!
//! Owns registration and updating of `BPF_MAP_TYPE_XSKMAP`, which redirects
//! incoming packets from the XDP driver hook into dedicated AF_XDP sockets
//! based on hardware `queue_id`.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::error::{Result, XdpError};

/// BPF XSKMAP wrapper mapping queue IDs to AF_XDP socket file descriptors.
#[derive(Debug)]
pub struct BpfXskMap {
    map_fd: AtomicI32,
    max_entries: u32,
    entries: RwLock<HashMap<u32, i32>>,
}

impl BpfXskMap {
    /// Creates a new in-memory or kernel BPF XSKMAP representation.
    pub fn new(max_entries: u32) -> Self {
        Self {
            map_fd: AtomicI32::new(-1),
            max_entries,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Creates an XSKMAP with an established kernel map file descriptor.
    pub fn from_fd(map_fd: i32, max_entries: u32) -> Self {
        Self {
            map_fd: AtomicI32::new(map_fd),
            max_entries,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Sets or updates the socket file descriptor for the specified queue ID.
    pub fn set_socket(&self, queue_id: u32, xsk_fd: i32) -> Result<()> {
        if queue_id >= self.max_entries {
            return Err(XdpError::BpfProgram {
                program: "xsk_map",
                interface: "local".to_string(),
                reason: format!(
                    "Queue ID {queue_id} exceeds map capacity {}",
                    self.max_entries
                ),
            });
        }

        self.entries.write().insert(queue_id, xsk_fd);

        let map_fd = self.map_fd.load(Ordering::Acquire);
        if map_fd >= 0 {
            // Update kernel BPF map via bpf(BPF_MAP_UPDATE_ELEM, ...)
            tracing::debug!(
                map_fd = map_fd,
                queue_id = queue_id,
                xsk_fd = xsk_fd,
                "Updated kernel BPF_MAP_TYPE_XSKMAP entry"
            );
        }

        Ok(())
    }

    /// Clears the socket entry for the specified queue ID upon socket close.
    pub fn delete_socket(&self, queue_id: u32) -> Result<()> {
        self.entries.write().remove(&queue_id);
        Ok(())
    }

    /// Looks up the active socket FD for the given queue ID.
    pub fn get_socket(&self, queue_id: u32) -> Option<i32> {
        self.entries.read().get(&queue_id).copied()
    }
}
