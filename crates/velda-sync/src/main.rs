//! Binary entrypoint for velda-sync daemon.
//!
//! Orchestrates the configuration reconciler loop with graceful shutdown
//! on SIGINT (Ctrl+C) and SIGTERM.

use std::env;
use std::path::PathBuf;
use std::time::Duration;

use tokio::time::sleep;
use velda_sync::provider::{ControlPlaneProvider, LocalFileProvider, Provider};
use velda_sync::{SyncComposition, SyncOutcome};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    // Initialize Provider based on acquisition mode
    let mode_str = env::var("VELDA_SYNC_MODE").unwrap_or_else(|_| "standalone".into());
    let (provider, mode_desc) = match mode_str.to_lowercase().as_str() {
        "control_plane" => {
            let endpoint = env::var("VELDA_CONTROL_PLANE_ENDPOINT")
                .unwrap_or_else(|_| "https://localhost:8443".into());
            let staging_dir = env::var("VELDA_STAGING_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| storage_dir.join("staging"));
            let control_plane_timeout_ms: u64 = env::var("VELDA_CONTROL_PLANE_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10_000);

            let mut control_plane =
                ControlPlaneProvider::new(endpoint.clone(), staging_dir)
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

    println!(
        "[velda-sync] Starting daemon (provider: {}, storage: {}, socket: {}, interval: {}ms)",
        mode_desc,
        storage_dir.display(),
        socket_path.display(),
        poll_interval_ms
    );

    let mut composition = SyncComposition::new(provider, storage_dir, socket_path);

    // Initial reconciliation pass
    match composition.reconcile().await {
        Ok(SyncOutcome::Updated { changed_domains }) => {
            println!(
                "[velda-sync] Initial publication successful (changed: {:?})",
                changed_domains
            );
        }
        Ok(SyncOutcome::Unchanged) => {
            println!("[velda-sync] Initial state up to date (no changes)");
        }
        Err(err) => {
            eprintln!("[velda-sync] Initial sync warning (keeping existing LKG): {err}");
        }
    }

    // Set up shutdown signal listeners
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    println!("[velda-sync] Running reconciliation loop. Waiting for signals...");

    loop {
        #[cfg(unix)]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("[velda-sync] Received SIGINT (Ctrl+C). Initiating graceful shutdown...");
                break;
            }
            _ = sigterm.recv() => {
                println!("[velda-sync] Received SIGTERM. Initiating graceful shutdown...");
                break;
            }
            _ = sleep(poll_interval) => {
                match composition.reconcile().await {
                    Ok(SyncOutcome::Updated { changed_domains }) => {
                        println!("[velda-sync] Published delta update: {:?}", changed_domains);
                    }
                    Ok(SyncOutcome::Unchanged) => {}
                    Err(err) => {
                        eprintln!("[velda-sync] Reconcile error (safe fallback, LKG preserved): {err}");
                    }
                }
            }
        }

        #[cfg(not(unix))]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("[velda-sync] Received Ctrl+C. Initiating graceful shutdown...");
                break;
            }
            _ = sleep(poll_interval) => {
                match composition.reconcile().await {
                    Ok(SyncOutcome::Updated { changed_domains }) => {
                        println!("[velda-sync] Published delta update: {:?}", changed_domains);
                    }
                    Ok(SyncOutcome::Unchanged) => {}
                    Err(err) => {
                        eprintln!("[velda-sync] Reconcile error (safe fallback, LKG preserved): {err}");
                    }
                }
            }
        }
    }

    println!("[velda-sync] Daemon stopped cleanly.");
    Ok(())
}
