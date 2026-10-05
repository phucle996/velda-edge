//! Velda Edge binary entrypoint.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::watch;
use velda_edge::{
    EdgeConfig, EdgeSupervisor, HardwareTopology, ThreadPinner, resolve_runtime_profile,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Check CLI arguments first before runtime bootstrap
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        let first = args[1].as_str();
        if matches!(first, "-v" | "--version" | "version") {
            let prov = velda_core::Provenance::current();
            println!("{}", prov.banner("velda-edge"));
            return Ok(());
        }
        if matches!(first, "-h" | "--help" | "help") {
            println!(
                "Velda Edge — High-Performance Edge Traffic Engine\n\n\
                 USAGE:\n    velda-edge [OPTIONS]\n\n\
                 OPTIONS:\n    -v, --version, version    Print version and build provenance information\n    -h, --help, help          Print help information\n\n\
                 ENVIRONMENT:\n    VELDA_RUNTIME_DIR         Directory containing compiled runtime profiles and configurations\n"
            );
            return Ok(());
        }
    }

    // Force link retention for watermark in .rodata
    let _ = velda_core::provenance::watermark();
    let prov = velda_core::Provenance::current();

    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    tracing::info!(
        version = prov.version,
        git_hash = prov.git_hash,
        author = prov.author,
        target = prov.target_triple,
        "Initializing Velda Edge Data Plane supervisor..."
    );

    // Discover hardware topology and runtime profile to size Tokio runtime
    let config = EdgeConfig::default();
    let hardware = HardwareTopology::probe();
    let runtime_profile = resolve_runtime_profile(&config.runtime_dir(), &hardware);
    let worker_threads = runtime_profile.transport.io_workers.max(1);
    let cpu_pinning = runtime_profile.transport.cpu_pinning;

    tracing::info!(
        cores = hardware.available_cores,
        workers = worker_threads,
        cpu_pinning,
        "Configuring multi-threaded Tokio runtime"
    );

    // Build Tokio runtime with CPU core pinning and designated thread names
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(worker_threads);
    builder.thread_name_fn(|| {
        static WORKER_ID: AtomicUsize = AtomicUsize::new(0);
        let id = WORKER_ID.fetch_add(1, Ordering::Relaxed);
        format!("velda-worker-{id}")
    });

    if cpu_pinning {
        let pinner = Arc::new(ThreadPinner::new());
        builder.on_thread_start(move || {
            pinner.pin_current_worker();
        });
    }

    builder.enable_all();
    let runtime = builder.build()?;

    // Execute edge gateway within custom runtime
    runtime.block_on(async_main(config))
}

async fn async_main(config: EdgeConfig) -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = EdgeSupervisor::bootstrap(config)?;

    // Trap OS signals (Ctrl+C / SIGINT and SIGTERM) for graceful shutdown
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let ctrl_c = async {
            let _ = tokio::signal::ctrl_c().await;
        };

        #[cfg(unix)]
        let terminate = async {
            if let Ok(mut sig) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                sig.recv().await;
            }
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => tracing::info!("Received SIGINT (Ctrl+C); initiating graceful edge shutdown..."),
            _ = terminate => tracing::info!("Received SIGTERM; initiating graceful edge shutdown..."),
        }

        // Notify supervisor and traffic engine to stop accepting new connections and drain
        let _ = shutdown_tx.send(true);

        // If a second interrupt is received while draining, force immediate exit
        let _ = tokio::signal::ctrl_c().await;
        tracing::warn!("Received second interrupt signal; forcing immediate exit");
        std::process::exit(1);
    });

    supervisor.run(shutdown_rx).await?;
    tracing::info!("Velda Edge Data Plane stopped cleanly.");
    Ok(())
}
