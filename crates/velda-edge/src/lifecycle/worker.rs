//! Dedicated Symmetrical Worker Pool & Lifecycle Manager (`lifecycle::worker`).
//!
//! Owns the instantiation, CPU core affinity pinning, and structured graceful shutdown
//! of paired Data Plane worker threads (Traffic Worker + Shadow Worker per Core).
//!
//! # Core Invariants:
//! - **Per-Core Symmetrical Pairing**: Exactly 1 Primary Traffic Worker (`SCHED_OTHER`)
//!   and 1 Auxiliary Shadow Worker (`SCHED_IDLE`) pinned 1:1 to each CPU core.
//! - **0% CPU Contention on Hot Path**: CFS preempts Shadow Workers instantly when
//!   traffic arrives on the Primary Traffic Worker.
//! - **Hook-Driven Architecture**: Shadow Workers provide a single core-local Hook
//!   channel. Plugins and auxiliary services register tasks directly into this Hook
//!   without arbitrary detached background task spawning.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use velda_core::{
    CorePartition, WorkerId, WorkerRole, WorkerState, global_worker_registry,
    pin_current_thread_to_core, set_current_thread_idle_priority,
};
use velda_plugin::ShadowTask;
use velda_transport::{IngressBinding, WorkerEngine};

use crate::error::EdgeError;
use crate::lifecycle::overload::DEFAULT_OVERLOAD_SAMPLE_INTERVAL;
use crate::pipeline::{build_tcp_pipeline_runner, build_udp_pipeline_runner};
use crate::runtime::SharedRuntime;

/// Running pool of active symmetrical worker threads across all hardware cores.
pub struct RunningWorkerPool {
    worker_threads: Vec<std::thread::JoinHandle<()>>,
}

impl RunningWorkerPool {
    /// Spawns the complete symmetrical worker pool (1 Primary Worker + 1 Shadow Worker per Core).
    pub fn spawn(
        workers: Vec<WorkerEngine>,
        partition: &CorePartition,
        initial_bindings: Vec<IngressBinding>,
        shared_runtime: SharedRuntime,
        cpu_pinning: bool,
        shutdown: watch::Receiver<bool>,
    ) -> Result<Self, EdgeError> {
        let (resolved_tx, resolved_rx) =
            watch::channel::<Option<HashMap<String, SocketAddr>>>(None);
        let resolved_publisher = Arc::new(resolved_tx);

        let worker_count = workers.len();
        let mut worker_threads = Vec::with_capacity(worker_count * 2);
        let registry = global_worker_registry();

        for worker in workers {
            let worker_id = worker.worker_id();
            let core_id = partition.traffic_cores[worker_id % partition.traffic_cores.len()];
            let worker_shutdown = shutdown.clone();
            let bindings = initial_bindings.clone();
            let rt_shared = shared_runtime.clone();
            let resolved_pub = Arc::clone(&resolved_publisher);
            let resolved_sub = resolved_rx.clone();
            let w_id = WorkerId(worker_id as u32);

            let traffic_thread_name = format!("velda-worker-{worker_id}");
            let shadow_thread_name = format!("velda-shadow-{worker_id}");

            // Register both symmetrical workers in global registry
            registry.register(w_id, core_id, WorkerRole::Traffic, &traffic_thread_name);
            registry.register(w_id, core_id, WorkerRole::Shadow, &shadow_thread_name);

            // 1. Core-local task hook & Shadow Worker (SCHED_IDLE)
            let (shadow_tx, shadow_rx) = tokio::sync::mpsc::channel(1024);
            let shadow_handle = spawn_shadow_worker(
                worker_id,
                core_id,
                cpu_pinning,
                rt_shared.clone(),
                worker_shutdown.clone(),
                shadow_rx,
            );
            registry.set_state(w_id, WorkerRole::Shadow, WorkerState::Running);
            worker_threads.push(shadow_handle);

            // 2. Primary Traffic Worker (SCHED_OTHER, pinned to same core)
            let handle = std::thread::Builder::new()
                .name(traffic_thread_name)
                .spawn(move || {
                    velda_plugin::set_core_shadow_sender(shadow_tx);
                    let reg = global_worker_registry();

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

                    reg.set_state(w_id, WorkerRole::Traffic, WorkerState::Running);

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
                    });

                    reg.set_state(w_id, WorkerRole::Traffic, WorkerState::Stopped);
                })
                .map_err(|e| {
                    EdgeError::Internal(format!("Failed to spawn worker thread {worker_id}: {e}"))
                })?;

            worker_threads.push(handle);
        }

        Ok(Self { worker_threads })
    }

    /// Awaits termination of all worker threads and ensures clean state transitions.
    pub async fn shutdown_and_join(self) -> Result<(), EdgeError> {
        let join_res = tokio::task::spawn_blocking(move || {
            let mut first_err = None;
            for handle in self.worker_threads {
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

        if let Ok(Some(err)) = join_res {
            return Err(err);
        }

        Ok(())
    }
}

/// Spawns a dedicated Shadow Auxiliary Worker thread pinned to `core_id`.
///
/// Drives the core-local Shadow Hook receiver and, for Worker 0, hosts process-wide
/// background plugins and system maintenance routines inside a single unified event loop.
pub fn spawn_shadow_worker(
    worker_id: usize,
    core_id: usize,
    cpu_pinning: bool,
    shared_runtime: SharedRuntime,
    mut shutdown: watch::Receiver<bool>,
    mut shadow_rx: mpsc::Receiver<ShadowTask>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("velda-shadow-{worker_id}"))
        .spawn(move || {
            if cpu_pinning {
                let _ = pin_current_thread_to_core(core_id);
            }
            set_current_thread_idle_priority();

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .event_interval(31)
                .thread_stack_size(1024 * 1024)
                .build()
                .expect("Failed to build shadow worker Tokio runtime");

            rt.block_on(async move {
                // Unified JoinSet for process-wide background services on Worker 0
                let mut bg_tasks = JoinSet::new();
                let mut shadow_tasks = JoinSet::new();
                let mut reload_rx = crate::reload::get_reload_receiver();
                let mut last_revision = shared_runtime.load().revision;

                if worker_id == 0 {
                    // 1. Register declared background plugins
                    register_background_plugins(
                        &mut bg_tasks,
                        &shared_runtime.load().plugins.background_plugins,
                        shutdown.clone(),
                    );

                    // 2. Register Resource Overload Monitor
                    let overload = shared_runtime.load().overload.clone();
                    let mut overload_shutdown = shutdown.clone();
                    bg_tasks.spawn(async move {
                        let mut interval = tokio::time::interval(DEFAULT_OVERLOAD_SAMPLE_INTERVAL);
                        loop {
                            tokio::select! {
                                _ = interval.tick() => {
                                    let _ = overload.sample_and_update();
                                }
                                _ = overload_shutdown.changed() => break,
                            }
                        }
                    });

                    // 3. Register Buffer Pool Sweeper
                    let sweeper_pool = shared_runtime.load().buffer_pool.clone();
                    let mut sweeper_shutdown = shutdown.clone();
                    bg_tasks.spawn(async move {
                        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                        loop {
                            tokio::select! {
                                _ = interval.tick() => {
                                    let _ = sweeper_pool.sweep_all_cores(std::time::Instant::now());
                                }
                                _ = sweeper_shutdown.changed() => break,
                            }
                        }
                    });
                }

                // Single unified cooperative event loop
                loop {
                    tokio::select! {
                        // Core-local Hook: receive generic task from Traffic Worker or registered plugins
                        item = shadow_rx.recv() => {
                            let Some(task) = item else { break };
                            if shadow_tasks.len() >= 1024 {
                                let _ = shadow_tasks.join_next().await;
                            }
                            shadow_tasks.spawn(task);
                        }

                        // Drive active shadow tasks and reap finished tasks
                        Some(_) = shadow_tasks.join_next(), if !shadow_tasks.is_empty() => {}
                        Some(_) = bg_tasks.join_next(), if !bg_tasks.is_empty() => {}

                        // Runtime revision update: reload background plugins on Worker 0
                        res = reload_rx.changed(), if worker_id == 0 => {
                            if res.is_ok() {
                                let current_rt = shared_runtime.load();
                                if current_rt.revision != last_revision {
                                    tracing::info!(
                                        old_rev = last_revision,
                                        new_rev = current_rt.revision,
                                        "Runtime revision updated, reloading background plugins on Shadow Worker 0"
                                    );
                                    bg_tasks.shutdown().await;
                                    register_background_plugins(
                                        &mut bg_tasks,
                                        &current_rt.plugins.background_plugins,
                                        shutdown.clone(),
                                    );
                                    last_revision = current_rt.revision;
                                }
                            }
                        }

                        // Graceful shutdown
                        _ = shutdown.changed() => break,
                    }
                }

                // Gracefully cancel and flush all active tasks in both sets
                shadow_tasks.shutdown().await;
                bg_tasks.shutdown().await;
            });
        })
        .expect("Failed to spawn shadow worker thread")
}

/// Helper registering declared background auxiliary plugins into a task [`JoinSet`].
fn register_background_plugins(
    tasks: &mut JoinSet<()>,
    plugins: &[Arc<dyn velda_plugin::BackgroundPlugin>],
    shutdown: watch::Receiver<bool>,
) {
    for plugin in plugins {
        let p = Arc::clone(plugin);
        let s = shutdown.clone();
        tasks.spawn(async move {
            let id = p.id().to_string();
            tracing::info!(plugin_id = %id, "Background auxiliary plugin starting on Shadow Worker");
            if let Err(e) = p.run(s).await {
                tracing::error!(plugin_id = %id, error = %e, "Background auxiliary plugin exited with error");
            } else {
                tracing::info!(plugin_id = %id, "Background auxiliary plugin stopped cleanly");
            }
        });
    }
}
