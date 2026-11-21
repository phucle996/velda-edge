//! Layer 4 (L4) Raw UDP Connector delegating to `velda-sock` (`OnceLock` hardware tuning).

/// Connects an ephemeral UDP socket to `(target_ip, target_port_be)` via `velda-sock`.
#[inline]
pub fn connect_udp_socket(
    target_ip: u128,
    target_port_be: u16,
) -> Result<tokio::net::UdpSocket, std::io::Error> {
    velda_sock::connect::connect_udp_u128(target_ip, target_port_be)
}
