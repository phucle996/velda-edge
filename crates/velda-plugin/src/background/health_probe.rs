//! Auxiliary Health & Readiness Probe Background Plugin Skeleton (`phase: "background"`).
//!
//! Exposes cold-path Kubernetes / Load Balancer liveness (`/healthz`) and readiness (`/readyz`)
//! endpoints on a dedicated auxiliary port, completely isolated from Data Plane traffic loops.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

use super::BackgroundPlugin;
use crate::error::PluginError;

/// Production skeleton for background health and readiness probe service.
pub struct HealthProbePlugin {
    pub id: String,
    pub listen_addr: SocketAddr,
}

impl HealthProbePlugin {
    /// Constructs a new health probe background service.
    pub fn new(id: impl Into<String>, listen_addr: SocketAddr) -> Self {
        Self {
            id: id.into(),
            listen_addr,
        }
    }
}

impl BackgroundPlugin for HealthProbePlugin {
    #[inline]
    fn id(&self) -> &str {
        &self.id
    }

    async fn run(self: Box<Self>, mut shutdown: watch::Receiver<bool>) -> Result<(), PluginError> {
        let listener = TcpListener::bind(self.listen_addr).await.map_err(|e| {
            PluginError::Config(format!(
                "Failed to bind health probe to {}: {e}",
                self.listen_addr
            ))
        })?;

        tracing::info!(addr = %self.listen_addr, "Auxiliary health probe service listening");

        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (mut stream, _) = match res {
                        Ok(conn) => conn,
                        Err(e) => {
                            tracing::warn!(error = %e, "Health probe accept error");
                            continue;
                        }
                    };

                    tokio::spawn(async move {
                        let mut buf = [0u8; 512];
                        let n = stream.read(&mut buf).await.unwrap_or(0);
                        let req_str = std::str::from_utf8(&buf[..n]).unwrap_or("");

                        let (status, body) = if req_str.starts_with("GET /healthz") || req_str.starts_with("GET /readyz") {
                            ("200 OK", "OK\n")
                        } else {
                            ("404 Not Found", "Not Found\n")
                        };

                        let response = format!(
                            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                    });
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("Health probe received shutdown signal; stopping accept loop");
                        break;
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }
}
