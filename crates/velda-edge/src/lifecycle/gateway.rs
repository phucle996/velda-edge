//! End-to-end gateway execution and Thread-Per-Core traffic serving coordination.

use std::sync::Arc;
use tokio::sync::watch;
use velda_core::hardware::{CorePartition, HardwareTopology};
use velda_core::{get_allowed_cores, set_current_thread_idle_priority};
use velda_transport::TrafficEngine;

use crate::config::EdgeConfig;
use crate::error::EdgeError;
use crate::lifecycle::RunningWorkerPool;
use crate::lifecycle::ipc::run_ipc_server;
use crate::profile::RuntimeProfile;
use crate::runtime::SharedRuntime;

/// Coordinates the end-to-end Thread-Per-Core gateway execution:
/// 1. Allocates 100% of allowed CPU cores to Data Plane traffic workers (zero core waste).
/// 2. Spawns background UDS IPC listener for live config updates (operating on idle CPU).
/// 3. Spawns Symmetrical Worker Pool (1 Primary Traffic Worker + 1 Shadow Worker per Core).
///    - Primary Shadow Worker (Worker 0) hosts process-wide background services (Prometheus exporter,
///      overload monitor, buffer pool sweeper) inside a single cooperative event loop.
///    - All Shadow Workers expose a core-local hook for non-blocking asynchronous plugin tasks.
/// 4. Each worker thread runs its own single-threaded Tokio current_thread runtime,
///    owning its dedicated shard of all TCP and UDP listener sockets with SO_REUSEPORT + SO_INCOMING_CPU.
/// 5. Awaits graceful termination when shutdown is signaled.
pub async fn run_gateway(
    engine: TrafficEngine,
    shared_runtime: SharedRuntime,
    config: EdgeConfig,
    _hardware: HardwareTopology,
    runtime_profile: RuntimeProfile,
    _metrics: Arc<velda_observability::MetricsStore>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), EdgeError> {
    let socket_path = config.socket_path.clone();
    let runtime_dir = config.runtime_dir();
    let worker_count = runtime_profile.transport.io_workers.max(1);
    let cpu_pinning = runtime_profile.transport.cpu_pinning;
    let allowed_cores = get_allowed_cores();

    // 1. Establish hardware partition: 100% of allowed cores allocated to Traffic Workers
    let partition = CorePartition::plan(&allowed_cores);
    tracing::info!(
        traffic_cores = ?partition.traffic_cores,
        total_cores = partition.traffic_cores.len(),
        "Established zero-waste hardware core partition (all-worker balanced sharding)"
    );

    // Register UDS IPC socket claim in host SocketManager
    if let Err(e) = velda_core::global_socket_manager()
        .register_management_uds(socket_path.to_string_lossy().to_string(), "gateway_ipc")
    {
        tracing::warn!(error = %e, "Gateway IPC UDS socket claim collision");
    }

    // 2. Partition TrafficEngine into N dedicated WorkerEngines
    let (workers, initial_bindings, engine_handle) = engine.into_workers(worker_count);

    // 3. Spawn background UDS IPC server task (runs on idle cycles via SCHED_IDLE)
    let ipc_shutdown = shutdown.clone();
    let ipc_shared = shared_runtime.clone();
    let ipc_handle = engine_handle.clone();
    let ipc_task = tokio::spawn(async move {
        set_current_thread_idle_priority();
        if let Err(e) = run_ipc_server(
            socket_path,
            runtime_dir,
            ipc_shared,
            Some(ipc_handle),
            ipc_shutdown,
        )
        .await
        {
            tracing::error!(error = %e, "IPC server task exited with error");
        }
    });

    // 4. Spawn Symmetrical Worker Pool (1 Primary Traffic Worker + 1 Shadow Worker per Core)
    // Primary Shadow Worker (Worker 0) automatically hosts all process-wide shadow background services
    let worker_pool = RunningWorkerPool::spawn(
        workers,
        &partition,
        initial_bindings,
        shared_runtime.clone(),
        cpu_pinning,
        shutdown.clone(),
    )?;

    // 5. Await worker pool shutdown and join
    worker_pool.shutdown_and_join().await?;

    // 6. Await Gateway Management IPC server task termination
    let _ = ipc_task.await;

    tracing::info!("Velda Edge Data Plane stopped cleanly across all workers.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{Runtime, new_shared_runtime};

    #[tokio::test]
    async fn test_shadow_worker_0_lifecycle_and_reload() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let initial_rt = Runtime::empty();
        let shared = new_shared_runtime(initial_rt);
        let (_task_tx, task_rx) = tokio::sync::mpsc::channel(16);
        let (traffic_tx, traffic_rx) = tokio::sync::oneshot::channel();
        let _ = traffic_tx.send(tokio::runtime::Handle::current());

        let handle = crate::lifecycle::worker::spawn_shadow_worker(
            0,
            0,
            false,
            shared.clone(),
            shutdown_rx,
            task_rx,
            traffic_rx,
        );

        // Update runtime revision to trigger reload branch on Shadow Worker 0
        let mut new_rt = Runtime::empty();
        new_rt.revision = 2;
        shared.store(Arc::new(new_rt));
        crate::reload::notify_reload(2);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Signal gateway shutdown
        let _ = shutdown_tx.send(true);

        // Shadow worker thread must terminate cleanly
        let res = tokio::task::spawn_blocking(move || handle.join()).await;
        assert!(res.is_ok());
    }
}
