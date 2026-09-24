//! L4 fast-path forwarding without protocol interpretation.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;

use crate::connection::Connection;
use crate::error::Result;
use crate::tcp::forward::{TransferStats, connect_and_forward, forward_connection};
use crate::udp::forward::forward_udp_flow;
use crate::udp::socket::UdpSocket;

/// Streams raw TCP bytes between a downstream client and an upstream target socket.
pub async fn forward_tcp_direct(client: Connection, target: SocketAddr) -> Result<TransferStats> {
    connect_and_forward(client, target).await
}

/// Streams raw TCP bytes between an accepted downstream [`Connection`] and an established upstream connection.
pub async fn forward_tcp_stream(
    client: Connection,
    upstream: tokio::net::TcpStream,
) -> Result<TransferStats> {
    forward_connection(client, upstream).await
}

/// Proxies raw UDP datagrams between a downstream client and an upstream target socket.
pub async fn forward_udp_direct(
    downstream: Arc<UdpSocket>,
    client_addr: SocketAddr,
    upstream_addr: SocketAddr,
    initial_payload: &[u8],
    shutdown: watch::Receiver<bool>,
) -> Result<TransferStats> {
    forward_udp_flow(
        downstream,
        client_addr,
        upstream_addr,
        initial_payload,
        shutdown,
    )
    .await
}
