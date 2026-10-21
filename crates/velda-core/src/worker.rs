//! Host Worker Thread & Concurrency Management (`worker`).
//!
//! Provides the process-wide Single Source of Truth for all active Data Plane
//! worker threads, core assignments, operational states, and roles.

use std::sync::{OnceLock, RwLock};

/// Strongly-typed identifier for an OS worker thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkerId(pub u32);

impl std::fmt::Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "worker-{}", self.0)
    }
}

/// Operational role of a worker thread running in the Data Plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerRole {
    /// Primary Traffic Worker: Pinned to CPU core, normal priority (`SCHED_OTHER`), serves user traffic hot path.
    Traffic,
    /// Shadow Worker: Pinned to the same CPU core, lowest priority (`SCHED_IDLE`), executes all auxiliary & plugin tasks.
    Shadow,
}

impl std::fmt::Display for WorkerRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Traffic => write!(f, "Traffic"),
            Self::Shadow => write!(f, "Shadow"),
        }
    }
}

/// Lifecycle execution state of a worker thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerState {
    /// Worker thread has been spawned and is initializing its runtime event loop.
    Starting,
    /// Worker thread is actively polling I/O events and processing requests.
    Running,
    /// Worker thread received shutdown signal and is draining active streams/tasks.
    Draining,
    /// Worker thread terminated execution cleanly.
    Stopped,
    /// Worker thread terminated abnormally due to an unhandled panic.
    Panicked,
}

impl std::fmt::Display for WorkerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Starting => write!(f, "Starting"),
            Self::Running => write!(f, "Running"),
            Self::Draining => write!(f, "Draining"),
            Self::Stopped => write!(f, "Stopped"),
            Self::Panicked => write!(f, "Panicked"),
        }
    }
}

/// Point-in-time descriptor of an active worker thread on the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerDescriptor {
    /// Unique identifier of this worker.
    pub id: WorkerId,
    /// Logical CPU core ID this worker is pinned to.
    pub core_id: usize,
    /// Operational role of this worker.
    pub role: WorkerRole,
    /// Current lifecycle execution state.
    pub state: WorkerState,
    /// Human-readable OS thread name (e.g. `velda-worker-0`, `velda-shadow-0`).
    pub thread_name: String,
}

/// Process-wide registry tracking active worker threads and core mappings.
#[derive(Debug, Default)]
pub struct WorkerRegistry {
    workers: RwLock<Vec<WorkerDescriptor>>,
}

static GLOBAL_WORKER_REGISTRY: OnceLock<WorkerRegistry> = OnceLock::new();

/// Returns the process-wide central WorkerRegistry instance.
pub fn global_worker_registry() -> &'static WorkerRegistry {
    GLOBAL_WORKER_REGISTRY.get_or_init(WorkerRegistry::new)
}

impl WorkerRegistry {
    /// Creates an empty worker registry.
    pub fn new() -> Self {
        Self {
            workers: RwLock::new(Vec::new()),
        }
    }

    /// Registers a newly spawned worker thread.
    pub fn register(
        &self,
        id: WorkerId,
        core_id: usize,
        role: WorkerRole,
        thread_name: impl Into<String>,
    ) {
        let descriptor = WorkerDescriptor {
            id,
            core_id,
            role,
            state: WorkerState::Starting,
            thread_name: thread_name.into(),
        };

        let mut workers = self.workers.write().unwrap();
        if let Some(pos) = workers.iter().position(|w| w.id == id && w.role == role) {
            workers[pos] = descriptor;
        } else {
            workers.push(descriptor);
        }
    }

    /// Updates the lifecycle state of a registered worker.
    pub fn set_state(&self, id: WorkerId, role: WorkerRole, state: WorkerState) {
        let Ok(mut workers) = self.workers.write() else {
            return;
        };
        if let Some(worker) = workers.iter_mut().find(|w| w.id == id && w.role == role) {
            worker.state = state;
        }
    }

    /// Returns a point-in-time snapshot of all registered workers.
    pub fn list(&self) -> Vec<WorkerDescriptor> {
        self.workers.read().unwrap().clone()
    }

    /// Returns the number of currently active (Running or Draining) workers.
    pub fn active_count(&self) -> usize {
        self.workers
            .read()
            .unwrap()
            .iter()
            .filter(|w| w.state == WorkerState::Running || w.state == WorkerState::Draining)
            .count()
    }

    /// Clears all worker descriptors (typically during shutdown or tests).
    pub fn clear(&self) {
        if let Ok(mut workers) = self.workers.write() {
            workers.clear();
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_registry_lifecycle() {
        let reg = WorkerRegistry::new();
        let w_id = WorkerId(0);

        reg.register(w_id, 2, WorkerRole::Traffic, "velda-worker-0");
        reg.register(w_id, 2, WorkerRole::Shadow, "velda-shadow-0");

        assert_eq!(reg.list().len(), 2);

        reg.set_state(w_id, WorkerRole::Traffic, WorkerState::Running);
        assert_eq!(reg.active_count(), 1);

        reg.set_state(w_id, WorkerRole::Shadow, WorkerState::Running);
        assert_eq!(reg.active_count(), 2);

        reg.set_state(w_id, WorkerRole::Traffic, WorkerState::Stopped);
        assert_eq!(reg.active_count(), 1);
    }

    #[test]
    fn test_get_allowed_cores_and_pin() {
        let cores = get_allowed_cores();
        assert!(!cores.is_empty(), "allowed cores must not be empty");

        let target_core = cores[0];
        let ok = pin_current_thread_to_core(target_core);
        assert!(ok, "pinning to first allowed core must succeed");
    }
}
