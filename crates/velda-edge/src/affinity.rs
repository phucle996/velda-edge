//! CPU core pinning and thread affinity manager.
//!
//! Enforces hardware affinity for Tokio network I/O worker threads,
//! eliminating cross-core cache invalidation, L1/L2 cache line thrashing,
//! and OS thread migration overhead.
//!
//! Respects container CPU affinity limits (e.g., Kubernetes cgroups taskset).

use std::sync::atomic::{AtomicUsize, Ordering};

/// Returns the logical CPU core IDs allowed for this process by the OS/cgroups.
pub fn get_allowed_cores() -> Vec<usize> {
    #[cfg(target_os = "linux")]
    {
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        let ret =
            unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
        if ret == 0 {
            let mut cores = Vec::new();
            for i in 0..libc::CPU_SETSIZE as usize {
                if unsafe { libc::CPU_ISSET(i, &set) } {
                    cores.push(i);
                }
            }
            if !cores.is_empty() {
                return cores;
            }
        }
    }

    // Fallback if not on Linux or if sched_getaffinity returns empty
    let count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    (0..count).collect()
}

/// Pins the calling OS thread to the specified logical CPU core ID.
pub fn pin_current_thread_to_core(core_id: usize) -> bool {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(core_id, &mut set);
            let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
            ret == 0
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = core_id;
        true
    }
}

/// Thread affinity pinner distributing worker threads across allowed CPU cores.
#[derive(Debug)]
pub struct ThreadPinner {
    allowed_cores: Vec<usize>,
    next_core_idx: AtomicUsize,
}

impl Default for ThreadPinner {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadPinner {
    /// Creates a new thread pinner querying allowed cores from the host OS.
    pub fn new() -> Self {
        let allowed_cores = get_allowed_cores();
        tracing::info!(
            allowed_cores = ?allowed_cores,
            total_allowed = allowed_cores.len(),
            "Initialized CPU thread affinity pinner"
        );
        Self {
            allowed_cores,
            next_core_idx: AtomicUsize::new(0),
        }
    }

    /// Creates a pinner with an explicit list of allowed core IDs.
    pub fn with_cores(cores: Vec<usize>) -> Self {
        let allowed_cores = if cores.is_empty() { vec![0] } else { cores };
        Self {
            allowed_cores,
            next_core_idx: AtomicUsize::new(0),
        }
    }

    /// Returns the slice of allowed CPU core IDs.
    #[inline]
    pub fn allowed_cores(&self) -> &[usize] {
        &self.allowed_cores
    }

    /// Pins the current thread to the next round-robin CPU core.
    ///
    /// Returns `(worker_idx, core_id)`.
    pub fn pin_current_worker(&self) -> (usize, usize) {
        let worker_idx = self.next_core_idx.fetch_add(1, Ordering::Relaxed);
        let core_id = self.allowed_cores[worker_idx % self.allowed_cores.len()];
        let ok = pin_current_thread_to_core(core_id);
        if ok {
            tracing::info!(
                worker = worker_idx,
                core = core_id,
                "Tokio worker thread pinned to CPU core"
            );
        } else {
            tracing::warn!(
                worker = worker_idx,
                core = core_id,
                "Failed to set thread affinity for worker thread"
            );
        }
        (worker_idx, core_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_allowed_cores_non_empty() {
        let cores = get_allowed_cores();
        assert!(!cores.is_empty(), "allowed cores must not be empty");
    }

    #[test]
    fn test_thread_pinner_round_robin() {
        let pinner = ThreadPinner::with_cores(vec![2, 3, 5]);
        assert_eq!(pinner.allowed_cores(), &[2, 3, 5]);

        let (w0, c0) = pinner.pin_current_worker();
        assert_eq!(w0, 0);
        assert_eq!(c0, 2);

        let (w1, c1) = pinner.pin_current_worker();
        assert_eq!(w1, 1);
        assert_eq!(c1, 3);

        let (w2, c2) = pinner.pin_current_worker();
        assert_eq!(w2, 2);
        assert_eq!(c2, 5);

        // Wrap-around
        let (w3, c3) = pinner.pin_current_worker();
        assert_eq!(w3, 3);
        assert_eq!(c3, 2);
    }

    #[test]
    fn test_pin_current_thread() {
        let cores = get_allowed_cores();
        let target_core = cores[0];
        let ok = pin_current_thread_to_core(target_core);
        assert!(ok, "pinning to first allowed core must succeed");
    }
}
