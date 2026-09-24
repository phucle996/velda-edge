//! Binary entrypoint for velda-sync daemon.
//!
//! Orchestrates the configuration reconciler loop with graceful shutdown
//! on SIGINT (Ctrl+C) and SIGTERM.

use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use velda_sync::pre_sync::PreSyncStage;
use velda_sync::provider::{
    ControlPlaneProvider, LocalFileProvider, Provider, SyncMetrics, init_logger,
};
use velda_sync::{SyncComposition, SyncOutcome};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize pure non-blocking logging provider (reads VELDA_SYNC_LOG_LEVEL and VELDA_SYNC_LOG_FORMAT)
    // WorkerGuard keeps the background worker thread alive and flushes on drop.
    let _log_guard = init_logger();

    // Initialize pure metrics provider
    let metrics = Arc::new(SyncMetrics::new());

    let storage_dir = env::var("VELDA_STORAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/lib/velda"));

    let socket_path = env::var("VELDA_SOCKET_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/run/velda/edge.sock"));

    let poll_interval_ms: u64 = env::var("VELDA_POLL_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let poll_interval = Duration::from_millis(poll_interval_ms);

    let staging_dir = env::var("VELDA_STAGING_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| storage_dir.join("staging"));

    // Initialize Provider based on acquisition mode
    let mode_str = env::var("VELDA_SYNC_MODE").unwrap_or_else(|_| "standalone".into());
    let (provider, mode_desc) = match mode_str.to_lowercase().as_str() {
        "control_plane" => {
            let endpoint = env::var("VELDA_CONTROL_PLANE_ENDPOINT")
                .unwrap_or_else(|_| "https://localhost:8443".into());
            let control_plane_timeout_ms: u64 = env::var("VELDA_CONTROL_PLANE_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10_000);

            let mut control_plane = ControlPlaneProvider::new(endpoint.clone())
                .with_timeout(Duration::from_millis(control_plane_timeout_ms));

            if let (Ok(cert), Ok(key)) =
                (env::var("VELDA_CLIENT_CERT"), env::var("VELDA_CLIENT_KEY"))
            {
                let ca = env::var("VELDA_CA_CERT").unwrap_or_default();
                control_plane = control_plane.with_mtls(ca, cert, key);
            } else if let Ok(ca) = env::var("VELDA_CA_CERT") {
                control_plane = control_plane.with_tls(ca);
            }

            let desc = format!(
                "ControlPlane (endpoint: {endpoint}, timeout: {control_plane_timeout_ms}ms)"
            );
            (Provider::ControlPlane(control_plane), desc)
        }
        _ => {
            let config_dir = env::var("VELDA_CONFIG_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("/etc/velda/config"));
            let desc = format!("LocalFile (dir: {})", config_dir.display());
            (
                Provider::LocalFile(LocalFileProvider::new(config_dir)),
                desc,
            )
        }
    };

    info!(
        mode = %mode_desc,
        storage = %storage_dir.display(),
        socket = %socket_path.display(),
        interval_ms = poll_interval_ms,
        "Starting velda-sync daemon"
    );

    let stage = PreSyncStage::new(provider, staging_dir);
    let mut composition = SyncComposition::with_stage(stage, storage_dir, socket_path);

    // Initial reconciliation pass
    let initial_start = Instant::now();
    match composition.reconcile().await {
        Ok(outcome) => {
            let elapsed = initial_start.elapsed();
            metrics.record_cycle_success(&outcome, elapsed);
            match outcome {
                SyncOutcome::Updated { changed_domains } => {
                    info!(
                        domains = ?changed_domains,
                        duration_ms = elapsed.as_millis(),
                        "Initial publication successful"
                    );
                }
                SyncOutcome::Unchanged => {
                    info!("Initial state up to date (no changes)");
                }
            }
        }
        Err(err) => {
            metrics.record_cycle_error(initial_start.elapsed());
            warn!(error = %err, "Initial sync warning (keeping existing LKG)");
        }
    }

    // Set up shutdown signal listeners
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    info!("Running reconciliation loop. Waiting for signals...");

    loop {
        #[cfg(unix)]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received SIGINT (Ctrl+C). Initiating graceful shutdown...");
                break;
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM. Initiating graceful shutdown...");
                break;
            }
            _ = sleep(poll_interval) => {
                let cycle_start = Instant::now();
                match composition.reconcile().await {
                    Ok(outcome) => {
                        let elapsed = cycle_start.elapsed();
                        metrics.record_cycle_success(&outcome, elapsed);
                        match outcome {
                            SyncOutcome::Updated { changed_domains } => {
                                info!(
                                    domains = ?changed_domains,
                                    duration_ms = elapsed.as_millis(),
                                    "Published configuration delta update"
                                );
                            }
                            SyncOutcome::Unchanged => {
                                debug!(duration_ms = elapsed.as_millis(), "Reconcile completed (unchanged)");
                            }
                        }
                    }
                    Err(err) => {
                        let elapsed = cycle_start.elapsed();
                        metrics.record_cycle_error(elapsed);
                        error!(
                            error = %err,
                            duration_ms = elapsed.as_millis(),
                            "Reconcile error (safe fallback, LKG preserved)"
                        );
                    }
                }
            }
        }

        #[cfg(not(unix))]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C. Initiating graceful shutdown...");
                break;
            }
            _ = sleep(poll_interval) => {
                let cycle_start = Instant::now();
                match composition.reconcile().await {
                    Ok(outcome) => {
                        let elapsed = cycle_start.elapsed();
                        metrics.record_cycle_success(&outcome, elapsed);
                        match outcome {
                            SyncOutcome::Updated { changed_domains } => {
                                info!(
                                    domains = ?changed_domains,
                                    duration_ms = elapsed.as_millis(),
                                    "Published configuration delta update"
                                );
                            }
                            SyncOutcome::Unchanged => {
                                debug!(duration_ms = elapsed.as_millis(), "Reconcile completed (unchanged)");
                            }
                        }
                    }
                    Err(err) => {
                        let elapsed = cycle_start.elapsed();
                        metrics.record_cycle_error(elapsed);
                        error!(
                            error = %err,
                            duration_ms = elapsed.as_millis(),
                            "Reconcile error (safe fallback, LKG preserved)"
                        );
                    }
                }
            }
        }
    }

    info!("Daemon stopped cleanly.");
    Ok(())
}
