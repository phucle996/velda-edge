//! Velda Edge binary entrypoint.

use tokio::sync::watch;
use velda_edge::{EdgeConfig, EdgeSupervisor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    tracing::info!("Initializing Velda Edge Data Plane supervisor...");
    let config = EdgeConfig::default();
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
