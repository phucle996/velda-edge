//! Process bootstrap and supervisor lifecycle owner.
//!
//! Executes cold-start initialization:
//! 1. Reads Last Known Good (LKG) binary artifacts from disk.
//! 2. Compiles initial in-memory [`Runtime`] snapshot into [`SharedRuntime`].
//! 3. Binds declarative ingress ports onto [`TrafficEngine`].
//! 4. Prepares [`EdgeSupervisor`] for execution.

use tokio::sync::watch;
use velda_core::hardware::{HardwareTopology, init_hardware_topology};
use velda_transport::TrafficEngine;

use crate::config::EdgeConfig;
use crate::error::EdgeError;
use crate::reload::load_initial_runtime;
use crate::runtime::{SharedRuntime, new_shared_runtime};
use crate::runtime_profile::{RuntimeProfile, resolve_runtime_profile};

/// Composition root supervisor coordinating the lifecycle of `velda-edge`.
pub struct EdgeSupervisor {
    pub config: EdgeConfig,
    pub hardware: HardwareTopology,
    pub runtime_profile: RuntimeProfile,
    pub shared_runtime: SharedRuntime,
    pub engine: TrafficEngine,
}

impl EdgeSupervisor {
    /// Cold-starts the supervisor from disk artifacts according to specified configuration.
    #[inline]
    pub fn bootstrap(config: EdgeConfig) -> Result<Self, EdgeError> {
        bootstrap(config)
    }

    /// Runs the edge gateway until `shutdown` is signaled.
    #[inline]
    pub async fn run(self, shutdown: watch::Receiver<bool>) -> Result<(), EdgeError> {
        crate::lifecycle::run_gateway(self.engine, self.shared_runtime, self.config, shutdown).await
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
    #[inline]
    pub fn shared_runtime(&self) -> &SharedRuntime {
        &self.shared_runtime
    }
}

/// Cold-starts the supervisor from disk artifacts according to specified configuration.
pub fn bootstrap(config: EdgeConfig) -> Result<EdgeSupervisor, EdgeError> {
    // Probe host hardware topology once during cold-start bootstrap and cache in RAM
    let hardware = HardwareTopology::probe();
    let _ = init_hardware_topology(hardware);

    let runtime_dir = config.runtime_dir();

    // Resolve runtime profile: file > probe fallback > write-back
    let runtime_profile = resolve_runtime_profile(&runtime_dir, &hardware);

    // Cold-start runtime snapshot assembly delegated entirely to reload subsystem
    let initial_runtime = load_initial_runtime(&runtime_dir, &runtime_profile)?;

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
        cpu_tier = hardware.cpu.tier.as_str(),
        memory_tier = hardware.memory.tier.as_str(),
        kernel_version = %hardware.kernel.version,
        acceleration = hardware.kernel.acceleration.as_str(),
        acceleration_reason = hardware.kernel.reason,
        "Velda Edge supervisor bootstrapped successfully"
    );

    Ok(EdgeSupervisor {
        config,
        hardware,
        runtime_profile,
        shared_runtime,
        engine,
    })
}
