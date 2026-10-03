//! End-to-end gateway execution and traffic serving coordination.

use tokio::sync::watch;
use velda_transport::{Connection, Datagram, TcpL7Handoff, TrafficEngine, UdpL7Handoff};

use crate::config::EdgeConfig;
use crate::error::EdgeError;
use crate::lifecycle::ipc::run_ipc_server;
use crate::pipeline::{handle_l4_tcp, handle_l4_udp, handle_tcp_l7, handle_udp_l7};
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
    let rt_l4 = shared_runtime.clone();
    let l4_handler = move |conn: Connection| {
        let rt = rt_l4.clone();
        async move {
            handle_l4_tcp(conn, &rt).await;
        }
    };

    let rt_l7 = shared_runtime.clone();
    let l7_handler = move |handoff: TcpL7Handoff| {
        let rt = rt_l7.clone();
        async move {
            handle_tcp_l7(handoff, &rt).await;
        }
    };

    let rt_udp_l4 = shared_runtime.clone();
    let udp_l4_handler = move |id, socket, dgram: Datagram| {
        let rt = rt_udp_l4.clone();
        async move {
            handle_l4_udp(id, socket, dgram, &rt).await;
        }
    };

    let rt_udp_l7 = shared_runtime.clone();
    let udp_l7_handler = move |handoff: UdpL7Handoff| {
        let rt = rt_udp_l7.clone();
        async move {
            handle_udp_l7(handoff, &rt).await;
        }
    };

    let engine_result = engine
        .run_all(
            shutdown,
            l4_handler,
            l7_handler,
            udp_l4_handler,
            udp_l7_handler,
        )
        .await;

    // 3. Await background IPC task termination
    let _ = ipc_task.await;

    engine_result.map_err(EdgeError::Transport)
}
