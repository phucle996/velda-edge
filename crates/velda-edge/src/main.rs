//! Velda Edge binary entrypoint.

use velda_core::hardware::{HardwareTopology, init_hardware_topology};
use velda_edge::{EdgeConfig, start};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // 1. Probe host hardware topology once during cold-start (respects container cgroups)
    let hardware = HardwareTopology::probe();
    let _ = init_hardware_topology(hardware);

    tracing::info!(
        available_cores = hardware.available_cores,
        cpu_tier = hardware.cpu_tier().as_str(),
        memory_tier = hardware.memory_tier().as_str(),
        "Configured Velda Edge runtime from HardwareTopology"
    );

    // 2. Build multi-threaded Tokio runtime explicitly matched to HardwareTopology available cores
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(hardware.available_cores)
        .enable_all()
        .build()?;

    runtime.block_on(async {
        tracing::info!("Initializing Velda Edge Data Plane supervisor...");
        let config = EdgeConfig::default();
        start(config).await?;
        tracing::info!("Velda Edge Data Plane stopped cleanly.");
        Ok(())
    })
}
