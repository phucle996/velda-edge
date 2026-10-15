//! End-to-end gateway execution and Thread-Per-Core traffic serving coordination.

use std::sync::Arc;
use tokio::sync::watch;
use velda_core::hardware::HardwareTopology;
use velda_transport::TrafficEngine;

use crate::affinity::{get_allowed_cores, pin_current_thread_to_core};
use crate::config::EdgeConfig;
use crate::error::EdgeError;
use crate::lifecycle::ipc::run_ipc_server;
use crate::lifecycle::overload::{DEFAULT_OVERLOAD_SAMPLE_INTERVAL, spawn_overload_monitor};
use crate::pipeline::{build_tcp_pipeline_runner, build_udp_pipeline_runner};
use crate::profile::RuntimeProfile;
use crate::runtime::SharedRuntime;

/// Coordinates the end-to-end Thread-Per-Core gateway execution:
/// 1. Spawns background UDS IPC listener for live config updates.
/// 2. Spawns background Resource Overload Monitor for OOM protection.
/// 3. Spawns N dedicated worker OS threads, each pinned 1:1 to a CPU core.
/// 4. Each worker thread runs its own single-threaded Tokio current_thread runtime,
///    owning its dedicated shard of all TCP and UDP listener sockets with SO_REUSEPORT + SO_INCOMING_CPU.
/// 5. Awaits graceful termination when shutdown is signaled.
pub async fn run_gateway(
    engine: TrafficEngine,
    shared_runtime: SharedRuntime,
    config: EdgeConfig,
    _hardware: HardwareTopology,
    runtime_profile: RuntimeProfile,
    shutdown: watch::Receiver<bool>,
) -> Result<(), EdgeError> {
    let socket_path = config.socket_path.clone();
    let runtime_dir = config.runtime_dir();
    let worker_count = runtime_profile.transport.io_workers.max(1);
    let cpu_pinning = runtime_profile.transport.cpu_pinning;
    let allowed_cores = get_allowed_cores();

    // 1. Partition TrafficEngine into N dedicated WorkerEngines
    let (workers, initial_bindings, engine_handle) = engine.into_workers(worker_count);

    // 2. Spawn background UDS IPC server task
    let ipc_shutdown = shutdown.clone();
    let ipc_shared = shared_runtime.clone();
    let ipc_handle = engine_handle.clone();
    let ipc_task = tokio::spawn(async move {
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

    // 3. Spawn background Resource Overload Monitor task (OOM protection)
    let overload_tracker = shared_runtime.load().overload.clone();
    let overload_task = spawn_overload_monitor(
        overload_tracker,
        DEFAULT_OVERLOAD_SAMPLE_INTERVAL,
        shutdown.clone(),
    );

    // 4. Coordinate ephemeral port resolution across worker threads (for port 0 listeners)
    let (resolved_tx, resolved_rx) = watch::channel(None);
    let resolved_publisher = Arc::new(resolved_tx);

    // 5. Spawn N dedicated Worker OS threads, each running a core-pinned single-threaded Tokio runtime
    let mut worker_threads = Vec::with_capacity(worker_count);
    for worker in workers {
        let worker_id = worker.worker_id();
        let core_id = allowed_cores[worker_id % allowed_cores.len()];
        let worker_shutdown = shutdown.clone();
        let bindings = initial_bindings.clone();
        let rt_shared = shared_runtime.clone();
        let resolved_pub = Arc::clone(&resolved_publisher);
        let resolved_sub = resolved_rx.clone();

        let handle = std::thread::Builder::new()
            .name(format!("velda-worker-{worker_id}"))
            .spawn(move || {
                if cpu_pinning {
                    let ok = pin_current_thread_to_core(core_id);
                    if ok {
                        tracing::info!(
                            worker = worker_id,
                            core = core_id,
                            "Worker thread affined to dedicated CPU core"
                        );
                    } else {
                        tracing::warn!(
                            worker = worker_id,
                            core = core_id,
                            "Failed to set thread affinity for worker thread"
                        );
                    }
                }

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .event_interval(31)
                    .thread_stack_size(1024 * 1024)
                    .build()
                    .expect("Failed to build worker current_thread Tokio runtime");

                rt.block_on(async move {
                    let rt_tcp = rt_shared.clone();
                    let tcp_dispatcher = move |listener_id: &str| {
                        let runner = build_tcp_pipeline_runner(listener_id, rt_tcp.clone());
                        move |conn| runner(conn)
                    };

                    let rt_udp = rt_shared.clone();
                    let udp_dispatcher = move |listener_id: &str| {
                        let runner = build_udp_pipeline_runner(listener_id, rt_udp.clone());
                        move |id, sock, dgram| runner(id, sock, dgram)
                    };

                    if let Err(e) = worker
                        .run(
                            bindings,
                            resolved_pub,
                            resolved_sub,
                            worker_shutdown,
                            tcp_dispatcher,
                            udp_dispatcher,
                        )
                        .await
                    {
                        tracing::error!(
                            worker = worker_id,
                            error = %e,
                            "Worker engine encountered error"
                        );
                    }
                })
            })
            .map_err(|e| {
                EdgeError::Internal(format!("Failed to spawn worker thread {worker_id}: {e}"))
            })?;

        worker_threads.push(handle);
    }

    // 6. Await shutdown or join of worker threads
    let join_res = tokio::task::spawn_blocking(move || {
        let mut first_err = None;
        for handle in worker_threads {
            if let Err(e) = handle.join() {
                tracing::error!("Worker thread panicked: {:?}", e);
                if first_err.is_none() {
                    first_err = Some(EdgeError::Internal("Worker thread panicked".into()));
                }
            }
        }
        first_err
    })
    .await;

    // 7. Await background supervisor tasks termination
    let _ = ipc_task.await;
    let _ = overload_task.await;

    if let Ok(Some(err)) = join_res {
        return Err(err);
    }

    tracing::info!("Velda Edge Data Plane stopped cleanly across all workers.");
    Ok(())
}
