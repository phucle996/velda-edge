//! UDP datagram forwarding and proxying.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;

use super::config::UdpSocketConfig;
use super::socket::UdpSocket;
use crate::error::{Result, TransportError};
use crate::tcp::forward::TransferStats;

/// Forwards a single datagram to the specified destination address.
pub async fn forward_datagram(
    socket: &UdpSocket,
    data: &[u8],
    destination: SocketAddr,
) -> Result<usize> {
    socket.send_to(data, destination).await
}

/// Runs a bidirectional UDP proxy session for a specific client flow.
///
/// Binds an ephemeral upstream UDP socket connected to `upstream_addr`,
/// routes responses from upstream back to `client_addr` through `downstream`,
/// and tracks transfer statistics until `shutdown` is signaled.
pub async fn forward_udp_flow(
    downstream: Arc<UdpSocket>,
    client_addr: SocketAddr,
    upstream_addr: SocketAddr,
    initial_payload: &[u8],
    mut shutdown: watch::Receiver<bool>,
) -> Result<TransferStats> {
    let bind_addr: SocketAddr = if upstream_addr.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };

    let upstream_socket = UdpSocket::bind(bind_addr, UdpSocketConfig::default())?;
    upstream_socket
        .inner()
        .connect(upstream_addr)
        .await
        .map_err(|e| TransportError::Connect {
            addr: upstream_addr,
            source: e,
        })?;

    // Send initial client payload to upstream
    let sent = upstream_socket
        .inner()
        .send(initial_payload)
        .await
        .map_err(TransportError::Io)?;

    let mut stats = TransferStats::new(sent as u64, 0);

    let mut buf = [0u8; 65535]; // Maximum UDP datagram size

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            res = upstream_socket.inner().recv(&mut buf) => {
                match res {
                    Ok(n) => {
                        let sent_back = downstream.send_to(&buf[..n], client_addr).await?;
                        stats.server_to_client_bytes += sent_back as u64;
                    }
                    Err(e) => {
                        return Err(TransportError::Io(e));
                    }
                }
            }
        }
    }

    Ok(stats)
}
