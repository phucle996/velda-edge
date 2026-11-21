//! Layer 4 (L4) Raw TCP Connector delegating to `velda-sock` (`OnceLock` hardware tuning).

use std::time::Duration;
use tokio::net::TcpStream;

/// Connects a raw TCP stream to `(target_ip, target_port_be)` via `velda-sock`.
#[inline]
pub async fn connect_tcp_stream(
    target_ip: u128,
    target_port_be: u16,
    timeout: Duration,
) -> Result<TcpStream, std::io::Error> {
    velda_sock::connect::connect_tcp_u128(target_ip, target_port_be, timeout).await
}
