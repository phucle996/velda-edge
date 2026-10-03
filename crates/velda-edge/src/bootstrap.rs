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
use crate::pipeline::{handle_l4_tcp, handle_l4_udp, handle_tcp_l7, handle_udp_l7};
use crate::runtime::router::build_router;
use crate::runtime::tls::{compile_tls_client, compile_tls_server};
use crate::runtime::{Runtime, RuntimeConfig, SharedRuntime, new_shared_runtime};
use crate::runtime_profile::{RuntimeProfile, resolve_runtime_profile};
use crate::uds::run_ipc_server;

/// Composition root supervisor coordinating the lifecycle of `velda-edge`.
pub struct EdgeSupervisor {
    config: EdgeConfig,
    hardware: HardwareTopology,
    runtime_profile: RuntimeProfile,
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

        // Resolve runtime profile: file > probe fallback > write-back
        let runtime_profile = resolve_runtime_profile(&runtime_dir, &hardware);

        // Load initial LKG state if available on disk (requires binary artifacts)
        let has_lkg = runtime_dir.join("listeners.bin").exists()
            || runtime_dir.join("routes.bin").exists()
            || runtime_dir.join("upstreams.bin").exists();

        let initial_runtime = if has_lkg {
            let listeners = load_listeners(&runtime_dir)?;
            let routes = load_routes(&runtime_dir)?;
            let upstreams = load_upstreams(&runtime_dir)?;
            velda_sync::post_sync::validate_streaming_policy(&listeners, &routes, &upstreams)
                .map_err(|e| EdgeError::InvalidConfig {
                    detail: e.to_string(),
                })?;
            let tls = load_tls(&runtime_dir)?;
            let tls_server = compile_tls_server(&tls, &runtime_profile.to_tls_server_params())?;
            let tls_client = compile_tls_client(&upstreams)?;
            let router = build_router(&routes, &upstreams, &listeners)?;
            let upstreams_table = crate::runtime::build_upstreams(&upstreams, tls_client.as_ref());

            // Pre-initialize HTTP/3 persistent pipeline engines for declared H3 listeners
            if let Some(tls) = tls_server.as_ref() {
                for listener in &listeners {
                    if listener.transport.protocol.eq_ignore_ascii_case("udp")
                        && (listener.application.protocol.eq_ignore_ascii_case("http3")
                            || listener.application.protocol.eq_ignore_ascii_case("grpc"))
                    {
                        let _ = crate::pipeline::l7::http3::init_h3_engine(&listener.id, tls);
                    }
                }
            }

            let runtime_config = RuntimeConfig {
                listeners,
                routes,
                upstreams,
                plugins: load_plugins(&runtime_dir)?,
                tls,
            };

            let pipelines =
                crate::runtime::pipeline::PipelineTable::build(&runtime_config.listeners)?;

            Runtime {
                revision: 1,
                config: runtime_config,
                router,
                pipelines,
                upstreams: upstreams_table,
                tls_server,
                tls_client,
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

        // Populate TrafficEngine with declared ingress bindings tuned to runtime profile
        let mut engine = TrafficEngine::new();
        let tcp_cfg = runtime_profile.to_tcp_listener_config();
        let udp_cfg = runtime_profile.to_udp_socket_config();
        let bindings = shared_runtime
            .load()
            .active_bindings_with_configs(Some(&tcp_cfg), Some(&udp_cfg))?;
        for binding in bindings {
            engine.add_binding(binding)?;
        }

        tracing::info!(
            listeners = initial_listeners,
            routes = initial_routes,
            storage = %config.storage_dir.display(),
            cores = hardware.available_cores,
            workers = runtime_profile.transport.io_workers,
            cpu_tier = hardware.cpu_tier().as_str(),
            memory_tier = hardware.memory_tier().as_str(),
            "Velda Edge supervisor bootstrapped successfully"
        );

        Ok(Self {
            config,
            hardware,
            runtime_profile,
            shared_runtime,
            engine,
        })
    }

    /// Returns the hardware topology probed during cold-start bootstrap.
    #[inline]
    pub fn hardware(&self) -> HardwareTopology {
        self.hardware
    }

    /// Returns the active runtime tuning profile.
    #[inline]
    pub fn runtime_profile(&self) -> &RuntimeProfile {
        &self.runtime_profile
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

        // 2. Run TrafficEngine accept and pipeline execution loops
        let runtime_l4 = self.shared_runtime.clone();
        let l4_handler = move |conn: Connection| {
            let rt = runtime_l4.clone();
            async move {
                handle_l4_tcp(conn, &rt).await;
            }
        };

        let runtime_l7 = self.shared_runtime.clone();
        let l7_handler = move |handoff: TcpL7Handoff| {
            let rt = runtime_l7.clone();
            async move {
                handle_tcp_l7(handoff, &rt).await;
            }
        };

        let runtime_udp_l4 = self.shared_runtime.clone();
        let udp_l4_handler = move |id, socket, dgram| {
            let rt = runtime_udp_l4.clone();
            async move {
                handle_l4_udp(id, socket, dgram, &rt).await;
            }
        };

        let runtime_udp_l7 = self.shared_runtime.clone();
        let udp_l7_handler = move |handoff: UdpL7Handoff| {
            let rt = runtime_udp_l7.clone();
            async move {
                handle_udp_l7(handoff, &rt).await;
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
