//! Velda Edge binary entrypoint.

use tokio::sync::watch;
use velda_edge::{EdgeConfig, EdgeSupervisor, HardwareTopology, resolve_runtime_profile};

#[cfg(all(
    feature = "jemalloc",
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// High-performance Datacenter Jemalloc profile (Pingora-aligned):
// - background_thread:true: Purges dirty pages in dedicated background thread, eliminating latency spikes on worker threads.
// - dirty_decay_ms / muzzy_decay_ms: 30000ms: Retains active page arenas across request bursts without issuing frequent kernel madvise syscalls.
#[cfg(all(
    feature = "jemalloc",
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[unsafe(export_name = "malloc_conf")]
pub static MALLOC_CONF: &[u8] =
    b"background_thread:true,dirty_decay_ms:30000,muzzy_decay_ms:30000\0";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Check CLI arguments first before runtime bootstrap
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        let first = args[1].as_str();
        if matches!(first, "-v" | "--version" | "version") {
            let prov = velda_core::Provenance::current();
            let config = EdgeConfig::default();
            let hardware = HardwareTopology::probe();
            let runtime_profile = resolve_runtime_profile(&config.runtime_dir(), &hardware);
            let worker_threads = runtime_profile.transport.io_workers.max(1);
            let cpu_pinning = runtime_profile.transport.cpu_pinning;
            let listeners =
                match velda_edge::load_initial_runtime(&config.runtime_dir(), &runtime_profile) {
                    Ok(rt) => rt
                        .config
                        .listeners
                        .iter()
                        .map(|l| {
                            let tls_suffix = if l.tls.enabled { "+TLS" } else { "" };
                            format!(
                                "{} ({}{})",
                                l.address,
                                l.application.protocol.to_uppercase(),
                                tls_suffix
                            )
                        })
                        .collect::<Vec<_>>(),
                    Err(_) => Vec::new(),
                };
            velda_edge::print_startup_banner(
                &prov,
                &hardware,
                worker_threads,
                cpu_pinning,
                &config.runtime_dir(),
                &listeners,
            );
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

    // Discover hardware topology and runtime profile to size Tokio runtime
    let config = EdgeConfig::default();
    let hardware = HardwareTopology::probe();
    let runtime_profile = resolve_runtime_profile(&config.runtime_dir(), &hardware);
    let worker_threads = runtime_profile.transport.io_workers.max(1);
    let cpu_pinning = runtime_profile.transport.cpu_pinning;

    // Resolve initial listeners for banner display
    let listeners = match velda_edge::load_initial_runtime(&config.runtime_dir(), &runtime_profile)
    {
        Ok(rt) => rt
            .config
            .listeners
            .iter()
            .map(|l| {
                let tls_suffix = if l.tls.enabled { "+TLS" } else { "" };
                format!(
                    "{} ({}{})",
                    l.address,
                    l.application.protocol.to_uppercase(),
                    tls_suffix
                )
            })
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };

    // Print the Hourglass ASCII startup banner to console
    velda_edge::print_startup_banner(
        &prov,
        &hardware,
        worker_threads,
        cpu_pinning,
        &config.runtime_dir(),
        &listeners,
    );

    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // System Doctor: warn if kernel UDP buffer is undersized (< 2MB)
    if let Some(rmem) = hardware.kernel.rmem_max {
        const RECOMMENDED_BUFFER: u64 = 2 * 1024 * 1024;
        if rmem < RECOMMENDED_BUFFER {
            let rmem_kb = rmem / 1024;
            tracing::warn!(
                rmem_max = rmem,
                rmem_kb,
                recommended = 16_777_216,
                "Kernel UDP buffer (rmem_max) is low ({}KB). For high-throughput HTTP/3, recommended: net.core.rmem_max=16777216",
                rmem_kb
            );
        }
    }

    #[cfg(unix)]
    auto_elevate_nofile_limit();

    tracing::info!(
        cores = hardware.available_cores,
        workers = worker_threads,
        cpu_pinning,
        "Bootstrapping Thread-Per-Core (Share-Nothing / Core-Affined) Edge Architecture"
    );

    // Build dedicated Supervisor runtime (lightweight single-threaded runtime for IPC and signals)
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .thread_name("velda-supervisor")
        .build()?;

    // Execute edge supervisor; worker threads are spawned 1:1 on dedicated CPU cores
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

/// Automatically elevates the process file descriptor limit (`RLIMIT_NOFILE`).
///
/// Ensures high-throughput HTTP/3 and reverse proxy workloads are never choked
/// by default OS soft limits (e.g., 1024 on Linux).
#[cfg(unix)]
fn auto_elevate_nofile_limit() {
    unsafe {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            const DESIRED_LIMIT: libc::rlim_t = 1_048_576;
            let initial_cur = rlim.rlim_cur;
            let initial_max = rlim.rlim_max;

            // 1. Try to elevate both max and cur to DESIRED_LIMIT (succeeds if running as root or with CAP_SYS_RESOURCE)
            rlim.rlim_cur = DESIRED_LIMIT;
            rlim.rlim_max = DESIRED_LIMIT.max(initial_max);
            if libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) == 0 {
                tracing::info!(
                    from_cur = initial_cur,
                    to_cur = rlim.rlim_cur,
                    from_max = initial_max,
                    to_max = rlim.rlim_max,
                    "Elevated file descriptor limit (RLIMIT_NOFILE) to datacenter scale"
                );
            } else {
                // 2. Unprivileged fallback: raise soft limit to match existing hard limit
                rlim.rlim_cur = initial_max;
                rlim.rlim_max = initial_max;
                if rlim.rlim_cur > initial_cur && libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) == 0 {
                    tracing::info!(
                        from_cur = initial_cur,
                        to_cur = rlim.rlim_cur,
                        max = initial_max,
                        "Elevated soft file descriptor limit to hard limit (RLIMIT_NOFILE)"
                    );
                }
            }
        }
    }
}
