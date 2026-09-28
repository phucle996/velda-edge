//! Process bootstrap and supervisor lifecycle owner.
//!
//! Executes cold-start initialization:
//! 1. Reads Last Known Good (LKG) binary artifacts from disk.
//! 2. Compiles initial in-memory [`Runtime`] snapshot into [`SharedRuntime`].
//! 3. Binds declarative ingress ports onto [`TrafficEngine`].
//! 4. Launches the UDS IPC server for live reloads.
//! 5. Supervises execution until process termination signal.

use tokio::sync::watch;
use velda_core::hardware::{HardwareTopology, init_hardware_topology};
use velda_transport::{Connection, TcpL7Handoff, TrafficEngine, UdpL7Handoff};

use crate::config::{
    EdgeConfig, load_listeners, load_plugins, load_routes, load_tls, load_upstreams,
};
use crate::error::EdgeError;
use crate::pipeline::{dispatch_l4, dispatch_tcp_l7, dispatch_udp_l4, dispatch_udp_l7};
use crate::runtime::composer::build_composer;
use crate::runtime::h3::compile_h3_engine;
use crate::runtime::tls::compile_tls_server;
use crate::runtime::{Runtime, RuntimeConfig, SharedRuntime, new_shared_runtime};
use crate::uds::run_ipc_server;

/// Composition root supervisor coordinating the lifecycle of `velda-edge`.
pub struct EdgeSupervisor {
    config: EdgeConfig,
    hardware: HardwareTopology,
    shared_runtime: SharedRuntime,
    engine: TrafficEngine,
}

impl EdgeSupervisor {
    /// Cold-starts the supervisor from disk artifacts according to specified configuration.
    pub fn bootstrap(config: EdgeConfig) -> Result<Self, EdgeError> {
        // Probe host hardware topology once during cold-start bootstrap and cache in RAM
        let hardware = HardwareTopology::probe();
        let _ = init_hardware_topology(hardware);

        let runtime_dir = config.runtime_dir();

        // Load initial LKG state if available on disk
        let initial_runtime = if runtime_dir.exists() {
            let listeners = load_listeners(&runtime_dir)?;
            let tls = load_tls(&runtime_dir)?;
            let tls_server = compile_tls_server(&tls)?;
            let h3_engine = compile_h3_engine(tls_server.as_ref());
            let composer = build_composer(&listeners)?;

            let runtime_config = RuntimeConfig {
                listeners,
                routes: load_routes(&runtime_dir)?,
                upstreams: load_upstreams(&runtime_dir)?,
                plugins: load_plugins(&runtime_dir)?,
                tls,
            };

            Runtime {
                revision: 1,
                config: runtime_config,
                composer,
                tls_server,
                h3_engine,
            }
        } else {
            tracing::info!(
                path = %runtime_dir.display(),
                "Runtime directory does not exist yet; initializing with empty state"
            );
            Runtime::empty()
        };

        let initial_listeners = initial_runtime.listener_count();
        let initial_routes = initial_runtime.route_count();

        let shared_runtime = new_shared_runtime(initial_runtime);

        // Populate TrafficEngine with declared ingress bindings
        let mut engine = TrafficEngine::new();
        let bindings = shared_runtime.load().active_bindings()?;
        for binding in bindings {
            engine.add_binding(binding)?;
        }

        tracing::info!(
            listeners = initial_listeners,
            routes = initial_routes,
            storage = %config.storage_dir.display(),
            workers = hardware.worker_threads,
            "Velda Edge supervisor bootstrapped successfully"
        );

        Ok(Self {
            config,
            hardware,
            shared_runtime,
            engine,
        })
    }

    /// Returns the hardware topology probed during cold-start bootstrap.
    #[inline]
    pub fn hardware(&self) -> HardwareTopology {
        self.hardware
    }

    /// Returns a reference to the active shared runtime container.
    pub fn shared_runtime(&self) -> &SharedRuntime {
        &self.shared_runtime
    }

    /// Runs the edge gateway until `shutdown` is signaled.
    pub async fn run(self, shutdown: watch::Receiver<bool>) -> Result<(), EdgeError> {
        let socket_path = self.config.socket_path.clone();
        let runtime_dir = self.config.runtime_dir();
        let shared_runtime = self.shared_runtime.clone();

        let engine_handle = self.engine.handle();

        // 1. Spawn UDS IPC listener task in background
        let ipc_shutdown = shutdown.clone();
        let ipc_task = tokio::spawn(async move {
            if let Err(e) = run_ipc_server(
                socket_path,
                runtime_dir,
                shared_runtime,
                Some(engine_handle),
                ipc_shutdown,
            )
            .await
            {
                tracing::error!(error = %e, "IPC server task exited with error");
            }
        });

        // 2. Run TrafficEngine accept and dispatch loops
        let runtime_l4 = self.shared_runtime.clone();
        let l4_handler = move |conn: Connection| {
            let rt = runtime_l4.clone();
            async move {
                dispatch_l4(conn, &rt).await;
            }
        };

        let runtime_l7 = self.shared_runtime.clone();
        let l7_handler = move |handoff: TcpL7Handoff| {
            let rt = runtime_l7.clone();
            async move {
                dispatch_tcp_l7(handoff, &rt).await;
            }
        };

        let runtime_udp_l4 = self.shared_runtime.clone();
        let udp_l4_handler = move |id, socket, dgram| {
            let rt = runtime_udp_l4.clone();
            async move {
                dispatch_udp_l4(id, socket, dgram, &rt).await;
            }
        };

        let runtime_udp_l7 = self.shared_runtime.clone();
        let udp_l7_handler = move |handoff: UdpL7Handoff| {
            let rt = runtime_udp_l7.clone();
            async move {
                dispatch_udp_l7(handoff, &rt).await;
            }
        };

        let engine_result = self
            .engine
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
}

/// Convenience function to bootstrap and run the edge process with OS signal termination.
pub async fn start(config: EdgeConfig) -> Result<(), EdgeError> {
    let supervisor = EdgeSupervisor::bootstrap(config)?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Trap Ctrl+C (SIGINT) and SIGTERM signals
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Received interrupt signal; initiating graceful edge shutdown");
        let _ = shutdown_tx.send(true);
    });

    supervisor.run(shutdown_rx).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_supervisor_bootstrap_and_graceful_shutdown() {
        let tmp = tempdir().unwrap();
        let config = EdgeConfig::new(tmp.path().join("storage"), tmp.path().join("test.sock"));

        let supervisor = EdgeSupervisor::bootstrap(config).unwrap();
        assert_eq!(supervisor.shared_runtime().load().revision, 0);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown_tx.send(true).unwrap();

        let res = handle.await.unwrap();
        assert!(res.is_ok());
    }
}
