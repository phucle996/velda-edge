//! Layer 4 (L4) Raw UDP Echo & Game Telemetry Server.
//!
//! Listens on a dedicated raw UDP port (default 9002) and reflects datagrams back to clients.
//! Preserves packet boundaries and client timestamps for RTT, jitter, and packet loss benchmarking.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::UdpSocket;

static TOTAL_UDP_DATAGRAMS: AtomicU64 = AtomicU64::new(0);
static TOTAL_UDP_BYTES: AtomicU64 = AtomicU64::new(0);

pub struct L4UdpServer;

impl L4UdpServer {
    /// Runs the raw UDP echo server loop on the specified address.
    pub async fn run(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let socket = UdpSocket::bind(addr).await?;
        tracing::info!(addr = %addr, "🚀 L4 Raw UDP Echo / Game Server listening");

        let mut buf = vec![0u8; 65536];

        loop {
            let (len, peer) = match socket.recv_from(&mut buf).await {
                Ok(res) => res,
                Err(e) => {
                    tracing::warn!(error = %e, "UDP recv_from error");
                    continue;
                }
            };

            TOTAL_UDP_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
            TOTAL_UDP_BYTES.fetch_add(len as u64, Ordering::Relaxed);

            // Echo the exact datagram back to the client peer preserving framing
            if let Err(e) = socket.send_to(&buf[..len], peer).await {
                tracing::warn!(peer = %peer, error = %e, "Failed echoing UDP datagram to client");
            }
        }
    }
}
