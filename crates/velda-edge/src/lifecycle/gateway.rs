//! End-to-end gateway execution and traffic serving coordination.

use std::sync::Arc;
use tokio::sync::watch;
use velda_transport::{Connection, Datagram, TrafficEngine, UdpSocket};

use crate::config::EdgeConfig;
use crate::error::EdgeError;
use crate::lifecycle::ipc::run_ipc_server;
use crate::pipeline::{build_tcp_pipeline_runner, build_udp_pipeline_runner};
use crate::runtime::SharedRuntime;

/// Coordinates the end-to-end gateway execution:
/// 1. Spawns background UDS IPC listener for live config updates.
/// 2. Directly executes TrafficEngine accept and protocol pipeline dispatch loops.
/// 3. Awaits graceful termination when shutdown is signaled.
pub async fn run_gateway(
    engine: TrafficEngine,
    shared_runtime: SharedRuntime,
    config: EdgeConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<(), EdgeError> {
    let socket_path = config.socket_path.clone();
    let runtime_dir = config.runtime_dir();
    let engine_handle = engine.handle();

    // 1. Spawn background UDS IPC server task
    let ipc_shutdown = shutdown.clone();
    let ipc_shared = shared_runtime.clone();
    let ipc_task = tokio::spawn(async move {
        if let Err(e) = run_ipc_server(
            socket_path,
            runtime_dir,
            ipc_shared,
            Some(engine_handle),
            ipc_shutdown,
        )
        .await
        {
            tracing::error!(error = %e, "IPC server task exited with error");
        }
    });

    // 2. Drive TrafficEngine accept and pipeline dispatch loops directly
    // Cold-path per-listener dispatchers: pre-resolves specialized pipeline runners ONCE per listener!
    let rt_tcp = shared_runtime.clone();
    let tcp_dispatcher = move |listener_id: &str| {
        let runner = build_tcp_pipeline_runner(listener_id, rt_tcp.clone());
        move |conn: Connection| {
            let runner = Arc::clone(&runner);
            runner(conn)
        }
    };

    let rt_udp = shared_runtime.clone();
    let udp_dispatcher = move |listener_id: &str| {
        let runner = build_udp_pipeline_runner(listener_id, rt_udp.clone());
        move |id: Arc<str>, socket: Arc<UdpSocket>, dgram: Datagram| {
            let runner = Arc::clone(&runner);
            runner(id, socket, dgram)
        }
    };

    let engine_result = engine
        .run_with_dispatchers(shutdown, tcp_dispatcher, udp_dispatcher)
        .await;

    // 3. Await background IPC task termination
    let _ = ipc_task.await;

    engine_result.map_err(EdgeError::Transport)
}
