//! Layer 4 (L4) Raw TCP Echo & Benchmark Server.
//!
//! Listens on a dedicated raw TCP port (default 9001) and provides bidirectional byte streaming
//! echo for throughput, latency, and connection concurrency benchmarking.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;

static TOTAL_TCP_BYTES: AtomicU64 = AtomicU64::new(0);
static TOTAL_TCP_CONNS: AtomicU64 = AtomicU64::new(0);

pub struct L4TcpServer;

impl L4TcpServer {
    /// Runs the raw TCP echo server loop on the specified address.
    pub async fn run(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!(addr = %addr, "🚀 L4 Raw TCP Echo Server listening");

        loop {
            let (mut socket, peer) = match listener.accept().await {
                Ok(res) => res,
                Err(e) => {
                    tracing::warn!(error = %e, "TCP accept error");
                    continue;
                }
            };

            TOTAL_TCP_CONNS.fetch_add(1, Ordering::Relaxed);
            let _ = socket.set_nodelay(true);

            tokio::spawn(async move {
                let (mut reader, mut writer) = socket.split();
                match tokio::io::copy(&mut reader, &mut writer).await {
                    Ok(bytes) => {
                        TOTAL_TCP_BYTES.fetch_add(bytes, Ordering::Relaxed);
                        tracing::debug!(peer = %peer, bytes, "L4 TCP connection closed");
                    }
                    Err(e) => {
                        tracing::debug!(peer = %peer, error = %e, "L4 TCP connection error");
                    }
                }
            });
        }
    }
}
