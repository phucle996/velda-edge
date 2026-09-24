//! Velda Edge binary entrypoint.

use velda_edge::{EdgeConfig, start};

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
    start(config).await?;

    tracing::info!("Velda Edge Data Plane stopped cleanly.");
    Ok(())
}
