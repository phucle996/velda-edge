//! Layer 4 (L4) raw TCP connection forwarding.
//!
//! Direct byte-level proxying between downstream client and upstream backend
//! via `velda_transport::forward_connection`. Pure raw TCP with zero TLS termination.

use velda_core::TransportProtocol;
use velda_transport::Connection;

use crate::runtime::SharedRuntime;

/// Handles a raw L4 TCP connection: evaluates route, acquires backend stream, and pumps bytes.
pub async fn handle_l4_tcp(conn: Connection, runtime: &SharedRuntime) {
    let peer = conn.peer();
    let Some(listener_id) = conn.listener_id().map(|s| s.to_string()) else {
        tracing::warn!(peer = %peer, "Received L4 connection without listener_id; dropping");
        return;
    };

    let rt = runtime.load();
    let Some(route) = rt.router.route_l4(&listener_id, TransportProtocol::Tcp) else {
        tracing::warn!(
            listener = %listener_id,
            peer = %peer,
            "No L4 TCP route configured for listener; dropping connection"
        );
        return;
    };

    // 1. Acquire backend connection from Upstream L4 TCP (Zero-TLS, pure raw TCP)
    let (target_addr, backend_stream) =
        if let Some(upstream) = rt.upstreams.tcp.get(&route.upstream_name) {
            match upstream.acquire().await {
                Ok(lease) => {
                    let target = lease.endpoint();
                    if let Some(stream) = lease.into_tcp_stream() {
                        (target, stream)
                    } else {
                        tracing::error!(
                            upstream = %route.upstream_name,
                            "L4 TCP upstream lease did not contain raw TcpStream"
                        );
                        return;
                    }
                }
                Err(e) => {
                    tracing::error!(
                        listener = %listener_id,
                        route = %route.id,
                        upstream = %route.upstream_name,
                        peer = %peer,
                        error = %e,
                        "Failed to acquire L4 TCP upstream connection; dropping connection"
                    );
                    return;
                }
            }
        } else {
            tracing::error!(
                listener = %listener_id,
                route = %route.id,
                upstream = %route.upstream_name,
                peer = %peer,
                "No backend upstream available in TCP upstream table; dropping connection"
            );
            return;
        };

    tracing::debug!(
        listener = %listener_id,
        route = %route.id,
        upstream = %route.upstream_name,
        target = %target_addr,
        peer = %peer,
        "Proxying L4 TCP stream to upstream backend via forward_connection (Zero TLS)"
    );

    match velda_transport::forward_connection(conn, backend_stream).await {
        Ok(stats) => {
            tracing::debug!(
                target = %target_addr,
                peer = %peer,
                client_to_server = stats.client_to_server_bytes,
                server_to_client = stats.server_to_client_bytes,
                "L4 TCP stream forwarding completed"
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                target = %target_addr,
                peer = %peer,
                "L4 TCP stream forwarding terminated with error"
            );
        }
    }
}
